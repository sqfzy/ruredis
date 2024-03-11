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
    async fn master_execute(&self, db: &Db) -> anyhow::Result<Option<Frame>>;

    async fn replicate_execute(&self, db: &Db) -> anyhow::Result<Option<Frame>> {
        Ok(None)
    }

    async fn hook(
        &self,
        _stream: &mut tokio::net::TcpStream,
        _db: &Db,
        _frame_sender: &Sender<Frame>,
        _frame: Frame,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
