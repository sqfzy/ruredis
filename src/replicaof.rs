use crate::{
    conf::{CONFIG, OFFSET},
    db::Db,
    frame::Frame,
    stream::FrameHandler,
    util,
};
use anyhow::{bail, Result};
use bytes::Bytes;
use std::sync::atomic::Ordering;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    select,
    sync::broadcast::Sender,
};

// 连接master server，开始主从复制
pub async fn enable_replicaof(
    master_addr: String,
    db: Db,
    psync_to_others_sender: Sender<Frame>,
    others_to_psync_sender: Sender<Frame>,
) {
    let mut to_master = TcpStream::connect(&master_addr)
        .await
        .expect("Fail to connect to master.");

    // 三路握手，与master server建立连接。如果连接失败，则尝试重新连接。如果超过三次握手失败，则退出程序
    let mut retry = 3;
    while let Err(e) = replicaof_hanshake(&mut to_master, CONFIG.server.port).await {
        tracing::error!("Fail to handshake with master: {}", e);
        let _ = to_master.shutdown().await;
        to_master = TcpStream::connect(&master_addr)
            .await
            .expect("Fail to connect to master.");
        retry -= 1;
        if retry == 0 {
            panic!("Fail to handshake with master.");
        }
    }

    loop {
        // 处理master server发送过来的命令
        match handle_master_connection(
            &mut to_master,
            &db,
            &psync_to_others_sender,
            &others_to_psync_sender,
        )
        .await
        {
            Err(e) => {
                let _ = to_master.write_frame(Frame::Error(e.to_string())).await;
            }
            Ok(Some(())) => {}
            Ok(None) => break,
        }
    }
}

async fn full_replication(to_master: &mut TcpStream, db: &Db) -> Result<()> {
    // send {PSYNC ? -1}
    to_master
        .write_frame(vec!["PSYNC".into(), "?".into(), "-1".into()].into())
        .await?;
    // recv {FULLRESYNC <REPL_ID> 0}
    if let Some(Frame::Simple(s)) = to_master.read_frame().await? {
        if !s.starts_with("FULLRESYNC") {
            bail!("Fail to replicate.");
        }
        tracing::info!("Successfully replicate. {}", s);
    } else {
        bail!("Fail to replicate.");
    }

    // 从master server接收RDB文件，并写入本地
    let rdb = get_rdb(to_master).await.expect("Fail to get rdb");
    tokio::fs::write("dump.rdb", rdb)
        .await
        .expect("Fail to write rdb.");

    // 从本地加载RDB文件
    let mut retry = 3;
    while let Err(e) = util::rdb_load(&mut db.inner.write().await) {
        tracing::error!("{} Trying again.", e);
        retry -= 1;
        if retry == 0 {
            panic!("Fail to load rdb.");
        }
    }

    // 从master server接收RDB文件，并写入本地
    let rdb = get_rdb(to_master).await?;
    tokio::fs::write("dump.rdb", rdb).await?;

    Ok(())
}

async fn partial_replication(to_master: &mut TcpStream, db: &Db, replid: String) -> Result<()> {
    // // send {PSYNC <REPL_ID> <OFFSET>}
    // to_master
    //     .write_frame(
    //         vec![
    //             "PSYNC".into(),
    //             replid.into(),
    //             OFFSET.load(Ordering::SeqCst).into(),
    //         ]
    //         .into(),
    //     )
    //     .await?;
    // // recv {CONTINUE}
    // if to_master.read_frame().await? != Some(Frame::Simple("CONTINUE".to_string())) {
    //     bail!("Fail to replicate.");
    // }
    //
    // // 从master server接收增量数据
    // loop {
    //     let frame = to_master.read_frame().await?;
    //     if let Some(frame) = frame {
    //         tracing::info!("received from master: {}", frame);
    //         let cmd = frame.clone().parse_cmd()?;
    //         if let Some(res) = cmd.replicate_execute(db).await? {
    //             tracing::info!("sending to master: {}", res);
    //             to_master.write_frame(res).await?;
    //         }
    //         cmd.hook(to_master, db, frame.clone()).await?;
    //         OFFSET.fetch_add(frame.num_of_bytes(), Ordering::SeqCst);
    //     } else {
    //         break;
    //     }
    // }

    Ok(())
}

async fn replicaof_hanshake(to_master: &mut TcpStream, port: u16) -> Result<()> {
    /* First Stage: 发送PING命令 */

    // 向master server 发送PING
    to_master
        .write_frame(Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"PING"))]))
        .await?;

    // 希望收到PONG回复，如果不是，则返回错误，然后重新连接
    if let Ok(future) =
        tokio::time::timeout(std::time::Duration::from_secs(5), to_master.read_frame()).await
    {
        match future? {
            Some(Frame::Simple(s)) => {
                // master server返回的不是PONG
                if s != "PONG" {
                    bail!("Master server does not respond correctly");
                }
            }
            Some(Frame::Error(e)) => {
                // master server返回一个错误
                bail!("Master server responds an error——{}", e);
            }
            _ => {
                bail!("Master server does not respond correctly");
            }
        }
    } else {
        // 在指定时间内没有收到master server的响应
        bail!("Receive no response from master server");
    }

    /* Second Stage: 身份验证(可选) */

    if let Some(auth) = &CONFIG.replication.masterauth {
        // send {AUTH <PASSWORD>}
        to_master
            .write_frame(vec!["AUTH".into(), auth.clone().into()].into())
            .await?;
        // recv {OK}
        if to_master.read_frame().await? != Some(Frame::Simple("OK".to_string())) {
            bail!("Master server responds invaildly");
        }
    }

    /* Third stage:  */

    // send {REPLCONF listening-port <PORT>}
    to_master
        .write_frame(
            vec![
                "REPLCONF".into(),
                "listening-port".into(),
                port.to_string().into(),
            ]
            .into(),
        )
        .await?;
    // recv {OK}
    if to_master.read_frame().await? != Some(Frame::Simple("OK".to_string())) {
        bail!("Master server responds invaildly");
    }

    // send {REPLCONF capa psync2}
    to_master
        .write_frame(vec!["REPLCONF".into(), "capa".into(), "psync2".into()].into())
        .await?;
    // recv {OK}
    if to_master.read_frame().await? != Some(Frame::Simple("OK".to_string())) {
        bail!("Master server responds invaildly");
    }

    Ok(())
}

async fn get_rdb(to_master: &mut TcpStream) -> Result<Vec<u8>> {
    let _ = to_master.read_u8().await;
    let rdb_len = to_master.read_decimal().await?;
    let mut buf = vec![0u8; rdb_len as usize];
    to_master.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn handle_master_connection(
    stream: &mut TcpStream,
    db: &Db,
    psync_to_others_sender: &Sender<Frame>,
    others_to_psync_sender: &Sender<Frame>,
) -> Result<Option<()>> {
    if let Some(frame) = stream.read_frame().await? {
        tracing::info!("received from master: {}", frame);

        let cmd = frame.clone().parse_cmd()?;
        if let Some(res) = cmd.replicate_execute(db).await? {
            tracing::info!("sending to master: {}", res);
            stream.write_frame(res).await?;
        }
        cmd.hook(
            stream,
            psync_to_others_sender,
            others_to_psync_sender,
            db,
            frame.clone(),
        )
        .await?;

        // 记录从master中接收到的命令的字节数
        OFFSET.fetch_add(frame.num_of_bytes(), Ordering::SeqCst);

        Ok(Some(()))
    } else {
        Ok(None)
    }
}
