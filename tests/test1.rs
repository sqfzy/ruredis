use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

#[test]
fn test1() {
    let handle1 = thread::spawn(move || {
        let mut child = Command::new("cargo")
            .arg("run")
            .arg("--")
            .arg("-p")
            .arg("6001")
            .arg("--replicaof")
            .arg("127.0.0.1:6379")
            .arg("--rdb-path")
            .arg("~/work_space/work_code/rust/rutinose/tests/")
            .stdout(Stdio::piped()) // Assuming you want to capture or ignore the output
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
}
