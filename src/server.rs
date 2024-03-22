use crate::util;
use crate::{conf::CONFIG, db::Db, frame::Frame, stream::FrameHandler};
use anyhow::Result;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::broadcast::Sender;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::broadcast::channel,
};
use tracing::{debug, error};

// TODO: 将master和replica的逻辑流分开
pub async fn run() {
    // util::client_test("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n").await;
    // return;

    let db = Db::new();

    // 创建一个广播通道，当replacate回复消息给Psync协程时，它可以向其它协程广播该消息
    let (replacate_msg_sender, _replacate_msg_receiver) =
        channel(CONFIG.replication.max_replicate as usize * 2);
    // 创建一个广播通道，当有写命令时，写命令的协程可以向Psync协程以及有关aof的协程发送写命令
    let (write_cmd_sender, _write_cmd_receiver) =
        channel(CONFIG.replication.max_replicate as usize * 2);

    // 如果配置了主从复制，则启动一个异步任务，连接到主服务器
    CONFIG.may_enable_replicaof(
        db.clone(),
        replacate_msg_sender.clone(),
        write_cmd_sender.clone(),
    );

    let finished_notify = std::sync::Arc::new(tokio::sync::Notify::new());
    // 如果配置了AOF持久化，则加载AOF文件
    CONFIG
        .may_enable_aof(write_cmd_sender.clone(), finished_notify.clone())
        .await;

    // 如果配置了RDB持久化，则加载RDB文件。(当RDB和AOF同时开启时，只会加载AOF文件)
    CONFIG.may_enable_rdb(&mut db.inner.write().await);

    // 开启一个异步任务，定时检查过期键
    util::check_expiration_periodical(
        Duration::from_secs(CONFIG.server.expire_check_interval_secs),
        &db,
    )
    .await;

    let listener = TcpListener::bind(format!("127.0.0.1:{}", CONFIG.server.port))
        .await
        .expect("Fail to connect");
    tracing::info!("server is running on port {}", CONFIG.server.port);

    loop {
        match listener.accept().await {
            Ok((mut stream, addr)) => {
                debug!("accepted new connection from {addr}");

                let db = db.clone();
                let replacate_msg_sender = replacate_msg_sender.clone();
                let write_cmd_sender = write_cmd_sender.clone();
                tokio::spawn(async move {
                    loop {
                        match handle(
                            &mut stream,
                            &db,
                            &replacate_msg_sender,
                            &write_cmd_sender,
                            addr,
                        )
                        .await
                        {
                            // 处理命令出错，向客户端返回错误信息，并继续循环处理客户端的下一条命令
                            Err(e) => {
                                tracing::error!("error: {}", e);
                                let _ = stream.write_frame(Frame::Error(e.to_string())).await;
                            }
                            Ok(Some(())) => {} // 当前命令处理完毕，客户端还未关闭连接。继续循环处理客户端的下一条命令
                            Ok(None) => break, // 客户端关闭连接，退出循环
                        }
                    }
                });
                finished_notify.notify_one();
            }
            Err(e) => {
                error!("error: {}", e);
            }
        }
    }
}

async fn handle(
    stream: &mut TcpStream,
    db: &Db,
    psync_to_others_sender: &Sender<Frame>,
    others_to_psync_sender: &Sender<Frame>,
    addr: SocketAddr,
) -> Result<Option<()>> {
    // util::server_test(&mut stream).await;
    // return Ok(());

    // 读取命令为一个Frame
    if let Some(frame) = stream.read_frame().await? {
        tracing::info!("received from client: {}", frame);

        let cmd = frame.clone().parse_cmd()?; // 解析Frame为一个命令

        // 执行命令，如果命令需要返回结果，则将结果写入stream
        if let Some(res) = cmd.execute(db).await? {
            tracing::info!("sending to client: {}", res);
            stream.write_frame(res).await?;
        }

        // 执行命令钩子
        cmd.hook(
            stream,
            psync_to_others_sender,
            others_to_psync_sender,
            frame,
        )
        .await?;

        Ok(Some(()))
    } else {
        tracing::info!("{addr} turn off connection");
        Ok(None)
    }
}
