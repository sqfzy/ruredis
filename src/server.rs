use crate::{
    config::RedisConfig, db::Db, frame::Frame, replicaof::enable_replicaof, stream::FrameHandler,
};
use anyhow::Result;
use std::sync::Arc;
use std::{net::SocketAddr, sync::atomic::AtomicU64};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::broadcast::channel,
};
use tracing::{debug, error};

pub static CONFIG: once_cell::sync::Lazy<Arc<RedisConfig>> =
    once_cell::sync::Lazy::new(|| Arc::new(RedisConfig::new()));

pub static ACKOFFSET: AtomicU64 = AtomicU64::new(0);

// TODO: 将master和replica的逻辑流分开
pub async fn run() {
    // util::client_test("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n").await;
    // return;

    let db = Db::new();

    // 每个连接的异步任务都持有一个sender，任意一个连接都可以通过sender注册一个接收者接收其它连接的消息，以此实现不同连接之间的相互通信
    // 例如，主服务器接收到一个写命令，就会通过sender将这个写命令发送给所有从服务器
    let (tx, _rx) = channel(CONFIG.max_replication as usize * 2);

    // 如果配置了主从复制，则启动一个异步任务，连接到主服务器
    if let Some(master_addr) = CONFIG.replicaof {
        enable_replicaof(master_addr, db.clone(), tx.clone());
    }

    // 开启一个异步任务，定时检查过期键
    crate::util::check_expiration_periodical(CONFIG.expire_check_interval, &db).await;

    let listener = TcpListener::bind(format!("localhost:{}", CONFIG.port))
        .await
        .expect("Fail to connect");
    tracing::info!("server is running on port {}", CONFIG.port);

    // 循环接收客户端的连接
    loop {
        match listener.accept().await {
            Ok((mut stream, addr)) => {
                debug!("accepted new connection from {addr}");

                let db = db.clone();
                let tx = tx.clone();
                tokio::spawn(async move {
                    // 循环处理客户端的命令
                    loop {
                        match handle(&mut stream, &db, &tx, addr).await {
                            // 处理命令出错，向客户端返回错误信息，并继续循环处理客户端的下一条命令
                            Err(e) => {
                                let _ = stream.write_frame(Frame::Error(e.to_string())).await;
                            }
                            Ok(Some(())) => {} // 当前命令处理完毕，客户端还未关闭连接。继续循环处理客户端的下一条命令
                            Ok(None) => break, // 客户端关闭连接，退出循环
                        }
                    }
                });
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
    tx: &tokio::sync::broadcast::Sender<Frame>,
    addr: SocketAddr,
) -> Result<Option<()>> {
    // util::server_test(&mut stream).await;
    // return Ok(());

    // 读取命令为一个Frame
    if let Some(frame) = stream.read_frame().await? {
        let cmd = frame.clone().parse_cmd()?; // 解析Frame为一个命令
                                              // 执行命令，如果命令需要返回结果，则将结果写入stream
        if let Some(res) = cmd.execute(db).await? {
            stream.write_frame(res).await?;
        }
        cmd.hook(stream, db, tx, frame).await?; // 执行命令钩子
        Ok(Some(()))
    } else {
        debug!("{addr} turn off connection");
        Ok(None)
    }
}
