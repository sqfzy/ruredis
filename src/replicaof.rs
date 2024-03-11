use crate::{config::CONFIG, db::Db, frame::Frame, stream::FrameHandler};
use anyhow::Result;
use bytes::Bytes;
use std::net::SocketAddr;
use tokio::{io::AsyncReadExt, net::TcpStream, sync::broadcast::Sender};

// 开启一个新的协程，连接master server
pub fn enable_replicaof(master_addr: SocketAddr, db: Db, tx: Sender<Frame>) {
    tokio::spawn(async move {
        let mut to_master = TcpStream::connect(master_addr)
            .await
            .expect("Fail to connect to master");

        // 四路握手，与master server建立连接
        replicaof_hanshake(&mut to_master, CONFIG.port)
            .await
            .expect("Fail to handshake");

        // 获取RDB文件
        let _rdb = get_rdb(&mut to_master).await.expect("Fail to get rdb");

        loop {
            // 处理master server发送过来的命令
            match handle_master_connection(&mut to_master, &db, &tx).await {
                Err(e) => {
                    let _ = to_master.write_frame(Frame::Error(e.to_string())).await;
                }
                Ok(Some(())) => {}
                Ok(None) => break,
            }
        }
    });
}

async fn replicaof_hanshake(to_master: &mut TcpStream, port: u16) -> Result<()> {
    // send {PING}
    to_master
        .write_frame(Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"PING"))]))
        .await?;
    // recv {PONG}
    if to_master.read_frame().await? != Some(Frame::Simple("PONG".to_string())) {
        panic!("Master server responds invaildly");
    }

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
        panic!("Master server responds invaildly");
    }

    // send {REPLCONF capa psync2}
    to_master
        .write_frame(vec!["REPLCONF".into(), "capa".into(), "psync2".into()].into())
        .await?;
    // recv {OK}
    if to_master.read_frame().await? != Some(Frame::Simple("OK".to_string())) {
        panic!("Master server responds invaildly");
    }

    // send {PSYNC ? -1}
    to_master
        .write_frame(vec!["PSYNC".into(), "?".into(), "-1".into()].into())
        .await?;
    // recv {FULLRESYNC <REPL_ID> 0}
    if let Some(Frame::Simple(s)) = to_master.read_frame().await? {
        tracing::info!("Successfully replicate, repl_id is {}", s);
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
    tx: &Sender<Frame>,
) -> Result<Option<()>> {
    if let Some(frame) = stream.read_frame().await? {
        let cmd = frame.clone().parse_cmd()?;
        cmd.master_execute(db).await?;

        cmd.hook(stream, db, tx, frame.clone()).await?;
        tracing::info!("received command from master: {}", frame);

        Ok(Some(()))
    } else {
        Ok(None)
    }
}

// #[tokio::test]
// async fn test_replicaof() {
//     use crate::server::run;
//
//     // 启动一个主服务器
//     let server_thread = std::thread::spawn(|| async {
//         run().await;
//     });
//
//     // 启动一个从服务器，当主服务器收到"write"命令时，从服务器也应当收到相同的命令
//     let replicate_thread = std::thread::spawn(|| async {
//         let addr = format!("127.0.0.1:{}", CONFIG.port);
//         let db = Db::new();
//         let (tx, _rx) = tokio::sync::broadcast::channel(CONFIG.max_replication as usize * 2);
//         enable_replicaof(SocketAddr::from_str(&addr).unwrap(), db, tx);
//     });
//
//     // 启动一个客户端，向主服务器发送命令
//     let client_thread = std::thread::spawn(|| async {
//         let addr = format!("127.0.0.1:{}", CONFIG.port);
//         let mut stream = TcpStream::connect(addr).await.unwrap();
//         stream
//             .write_frame(Frame::Array(vec![
//                 Frame::Bulk(Bytes::from_static(b"SET")),
//                 Frame::Bulk(Bytes::from_static(b"key")),
//                 Frame::Bulk(Bytes::from_static(b"value")),
//             ]))
//             .await
//             .unwrap();
//     });
// }
