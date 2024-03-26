use std::thread;
use std::time::Duration;

use assert_cmd::prelude::*;
use assert_cmd::Command;

/* 1. 测试全量复制 */
#[test]
fn test_full_replicate_sync() {
    // 向主服务器发送一些写命令
    let handle1 = thread::spawn(|| {
        let mut cmd = Command::new("redis-cli");
        cmd.arg("set").arg("foo").arg("bar").assert().stdout("OK\n");
        Command::new("redis-cli")
            .arg("set")
            .arg("foo1")
            .arg("bar1")
            .assert()
            .stdout("OK\n");
    });

    // 开启一个从服务器，此时从服务器会全量复制主服务器的数据
    let handle2 = thread::spawn(|| {
        std::thread::sleep(Duration::from_millis(500));

        // cargo run -- -p 1233 --replicaof 127.0.0.1:6379 --rdb-path ~/work_space/work_code/rust/                                                                                                                        ─╯
        Command::new("cargo")
            .arg("run")
            .arg("--")
            .arg("-p")
            .arg("6000")
            .arg("--replicaof")
            .arg("127.0.0.1:6379")
            .arg("--rdb-path")
            .arg("~/work_space/work_code/rust/rutinose/tests/")
            .assert()
            .success();
    });

    let handle3 = thread::spawn(|| {
        // 等待从服务器启动后，检查是否复制成功
        std::thread::sleep(Duration::from_millis(500));

        // redis-cli -p 6000 get foo
        Command::new("redis-cli")
            .arg("-p")
            .arg("6000")
            .arg("get")
            .arg("foo")
            .assert()
            .stdout("bar\n");
        // redis-cli -p 6000 get foo1
        Command::new("redis-cli")
            .arg("-p")
            .arg("6000")
            .arg("get")
            .arg("foo1")
            .assert()
            .stdout("bar1\n");
    });

    handle1.join().unwrap();
    handle2.join().unwrap();
    handle3.join().unwrap();
}

/* 2. 测试增量复制 */
#[test]
fn test_partial_replicate_sync() {
    let handle1 = thread::spawn(move || {
        let mut child = std::process::Command::new("cargo")
            .arg("run")
            .arg("--")
            .arg("-p")
            .arg("6001")
            .arg("--replicaof")
            .arg("127.0.0.1:6379")
            .arg("--rdb-path")
            .arg("~/work_space/work_code/rust/rutinose/tests/")
            .stdout(std::process::Stdio::piped()) // Assuming you want to capture or ignore the output
            .spawn()
            .expect("Failed to start process");

        // Do something with the process

        // Wait for some time
        thread::sleep(Duration::from_secs(1));

        // Now kill the process
        child.kill().expect("Failed to kill the process");
        let _ = child.wait().expect("Failed to wait on the child");
    });

    handle1.join().unwrap();
    // std::thread::sleep(Duration::from_millis(500));
    //
    // let handle2 = thread::spawn(|| {
    //     let mut cmd = Command::new("redis-cli");
    //     cmd.arg("set")
    //         .arg("foo3")
    //         .arg("bar3")
    //         .assert()
    //         .stdout("OK\n");
    //     Command::new("redis-cli")
    //         .arg("set")
    //         .arg("foo4")
    //         .arg("bar4")
    //         .assert()
    //         .stdout("OK\n");
    // });
}

// 3. 测试命令传播
// 4. 测试断线重连
