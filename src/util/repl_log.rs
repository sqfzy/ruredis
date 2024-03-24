use bytes::Bytes;
use std::{
    collections::VecDeque,
    sync::{atomic::AtomicU64, Arc},
};
use tokio::sync::Mutex;

pub struct RepliBackLog {
    pub buf: Arc<Mutex<VecDeque<u8>>>,
    pub capacity: AtomicU64,
}

impl RepliBackLog {
    /// RepliBackLog至少有1024个字节的容量
    pub fn new(capacity: u64) -> Self {
        let capacity = if capacity < 1024 { 1024 } else { capacity };
        Self {
            buf: Arc::new(Mutex::new(VecDeque::new())),
            capacity: AtomicU64::new(capacity),
        }
    }

    /// 将命令追加到RepliBackLog中，如果RepliBackLog的容量不足，将会删除头部的数据
    pub async fn force_append(&self, cmd: Bytes) {
        let mut buf = self.buf.lock().await;
        let cap = self.capacity.load(std::sync::atomic::Ordering::SeqCst);
        let new_cap = (buf.len() + cmd.len()) as u64;
        if new_cap > cap {
            let mut diff = new_cap - cap;
            while diff > 0 {
                let _ = buf.pop_front();
                diff -= 1;
            }
        }
        for b in cmd {
            buf.push_back(b);
        }
    }

    // 从尾部获取n个字节
    pub async fn get_from_end(&self, n: usize) -> Option<Vec<u8>> {
        let buf = self.buf.lock().await;
        println!("buf.len() = {}", buf.len());
        let mut res = Vec::new();
        for i in 0..n {
            if buf.len() < i + 1 {
                return None;
            }
            if let Some(v) = buf.get(buf.len() - i - 1) {
                res.push(*v);
            }
        }
        Some(res)
    }
}

#[tokio::test]
async fn test_repli_backlog() {
    let repli_backlog = RepliBackLog::new(1024);
    repli_backlog
        .force_append(Bytes::from("hello".to_string()))
        .await;
    repli_backlog
        .force_append(Bytes::from("world".to_string()))
        .await;
    let res = repli_backlog.get_from_end(2).await;
    assert_eq!(res.unwrap(), vec![b'd', b'l']);
}
