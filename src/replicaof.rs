use crate::{
    conf::{CONFIG, OFFSET, REPLID},
    connection::Connection,
    db::Db,
    frame::Frame,
    stream::FrameHandler,
    util,
};
use anyhow::{bail, Result};
use bytes::Bytes;
use std::sync::atomic::Ordering;
use tokio::{io::AsyncReadExt, net::TcpStream, sync::broadcast::Sender};

// 连接master server，开始主从复制
pub async fn start_replicaof(
    master_addr: String,
    db: &Db,
    replacate_msg_sender: Sender<Frame>,
    write_cmd_sender: Sender<Frame>,
) -> anyhow::Result<()> {
    let mut to_master = Connection::connect(&master_addr).await?;

    // 三路握手，与master server建立连接。
    replicaof_hanshake(&mut to_master, CONFIG.server.port).await?;

    let replid = REPLID.lock().await.clone();
    if let Some(replid) = replid {
        // 非首次复制

        // 发送之前从master server接收到的REPLID
        to_master
            .write_frame(
                vec![
                    "PSYNC".into(),
                    replid.into(),
                    format!("{}", OFFSET.load(Ordering::SeqCst)).into(),
                ]
                .into(),
            )
            .await?;

        if let Some(Frame::Simple(s)) = to_master.read_frame().await? {
            if s.starts_with("FULLRESYNC") {
                // 如果master server返回FULLRESYNC，则进行全量复制
                let replid = s.split_whitespace().nth(1).unwrap_or_default();
                full_replication(&mut to_master, db, replid).await?;
                tracing::info!("Receive '{}'. Start full replicate sync.", s);
            } else if s.starts_with("CONTINUE") {
                // 如果master server返回CONTINUE，则继续增量复制
                tracing::info!("Receive '{}'. Perform partial replicate sync.", s);
            } else {
                bail!(
                    "Master server should respond 'FULLRESYNC' or 'CONTINUE' but got {:?}",
                    s
                );
            }
        } else {
            bail!("Master server responds invaildly.");
        }
    } else {
        // 首次复制

        // send {PSYNC ? -1}
        to_master
            .write_frame(vec!["PSYNC".into(), "?".into(), "-1".into()].into())
            .await?;

        // recv {FULLRESYNC <REPL_ID> <offset>}
        if let Some(Frame::Simple(s)) = to_master.read_frame().await? {
            if s.starts_with("FULLRESYNC") {
                tracing::info!("Receive '{}'. Perform full replicate sync.", s);
                let replid = s.split_whitespace().nth(1).unwrap_or_default();
                full_replication(&mut to_master, db, replid).await?;
            } else {
                bail!("Master server should respond 'FULLRESYNC' but got {:?}", s);
            }
        } else {
            bail!("Master server responds invaildly.");
        }
    }

    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        tokio::select! {
            // 每隔1秒向master server发送REPLCONF ACK <replication_offset>命令
            _ = interval.tick() => {
                to_master
                    .write_frame(
                        vec![
                            "REPLCONF".into(),
                            "ACK".into(),
                            format!("{}", OFFSET.load(Ordering::SeqCst)).into(),
                        ]
                        .into(),
                    )
                    .await?;
            }
            // 处理master server发送过来的命令
            resp = handle_master_connection(&mut to_master, db, &replacate_msg_sender, &write_cmd_sender) => {
                match resp {
                    Err(e) => {
                        let _ = to_master.write_frame(Frame::Error(e.to_string())).await;
                    }
                    Ok(Some(())) => {}
                    Ok(None) => break,
                }
            }
        }
    }

    Ok(())
}

async fn full_replication(to_master: &mut Connection, db: &Db, replid: &str) -> Result<()> {
    // 记录主服务器的ID
    *REPLID.lock().await = Some(replid.to_string());

    // 从master server接收RDB文件，并写入本地
    let rdb = get_rdb(to_master).await.expect("Fail to get rdb");
    tokio::fs::write("dump.rdb", rdb)
        .await
        .expect("Fail to write rdb.");

    // 从本地加载RDB文件
    util::rdb_load(db)?;

    Ok(())
}

async fn replicaof_hanshake(to_master: &mut Connection, port: u16) -> Result<()> {
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

async fn get_rdb(to_master: &mut Connection) -> Result<Vec<u8>> {
    let _ = to_master.stream.read_u8().await;
    let rdb_len = to_master.read_decimal().await?;
    let mut buf = vec![0u8; rdb_len as usize];
    to_master.stream.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn handle_master_connection(
    stream: &mut Connection,
    db: &Db,
    replacate_msg_sender: &Sender<Frame>,
    write_cmd_sender: &Sender<Frame>,
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
            replacate_msg_sender,
            write_cmd_sender,
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
