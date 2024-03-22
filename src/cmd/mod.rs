mod command;
mod replicate;
mod string_cmd;

use crate::{db::Db, frame::Frame};
use tokio::sync::broadcast::Sender;

pub use command::*;
pub use replicate::*;
pub use string_cmd::*;

// TODO: 实现SAVE, BGSAVE, BGREWRITEAOF

#[async_trait::async_trait]
pub trait CmdExecutor: Send + Sync {
    async fn execute(&self, db: &Db) -> anyhow::Result<Option<Frame>>;

    // 默认情况下，replicate不需要返回响应给master
    async fn replicate_execute(&self, db: &Db) -> anyhow::Result<Option<Frame>> {
        let _ = self.execute(db).await;
        Ok(None)
    }

    async fn hook(
        &self,
        _stream: &mut tokio::net::TcpStream,
        _replacate_msg_sender: &Sender<Frame>,
        _write_cmd_sender: &Sender<Frame>,
        _frame: Frame,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
