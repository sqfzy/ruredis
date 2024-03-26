// #![allow(clippy::needless_return)]

mod cli;
mod cmd;
mod conf;
mod connection;
mod db;
mod frame;
mod init;
mod replicaof;
mod server;
mod stream;
mod util;

// TODO:
// 1. more reasonable error handling
// 2. more commands: ttl, exist

// TODO: 对安全性的支持：
// 1. 限制最大内存
// 2. 限制最大连接数
// 3. 限制最大请求长度
// 4. 限制最大 key 长度
// 5. 限制最大 value 长度
// 6. 限制最大 list 长度
// 7. 限制最大 set 长度
// 8. 限制最大 zset 长度
// 9. 限制最大 hash 长度
// 10. 限制最大 db 数量
// 11. 限制最大 key 数量
// ...

// TODO: 对加密的支持：
// 1. 支持 SSL
// 2. 支持密码认证连接
// 3. 支持密钥认证连接
// 4. 支持命令的权限控制
// 5. 支持RDB和AOF的文件加密

// TODO: 对更多数据类型的支持：

// TODO: 对更多命令的支持：

// TODO: 性能基准测试

// INFO: 已有的feature:
// 1. RDB和AOF持久化

#[tokio::main]
async fn main() {
    init::init();

    server::run().await;
}
