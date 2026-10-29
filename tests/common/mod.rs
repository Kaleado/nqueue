//! A throwaway Valkey server per test.

#![allow(dead_code)]

use std::path::PathBuf;
use std::process::{Child, Stdio};
use std::thread;
use std::time::{Duration, Instant};

pub struct Server {
    child: Child,
    pub dir: tempfile::TempDir,
    pub socket: PathBuf,
    pub url: String,
}

impl Server {
    /// Starts `valkey-server` (or `$NQUEUE_VALKEY_SERVER`) on a socket in a
    /// temp dir, without persistence.
    pub fn start() -> Server {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("valkey.sock");
        let child = nqueue::valkey::server_command(dir.path(), &socket, false)
            .stdout(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| {
                panic!(
                    "starting {}: {e} (install valkey or set NQUEUE_VALKEY_SERVER)",
                    nqueue::valkey::server_binary().display()
                )
            });
        let url = nqueue::valkey::socket_url(&socket);
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(mut conn) = redis::Client::open(url.as_str()).unwrap().get_connection() {
                if redis::cmd("PING").query::<String>(&mut conn).is_ok() {
                    break;
                }
            }
            assert!(Instant::now() < deadline, "valkey did not start");
            thread::sleep(Duration::from_millis(20));
        }
        Server { child, dir, socket, url }
    }

    pub fn broker(&self) -> nqueue::broker::Broker {
        nqueue::broker::Broker::connect(&self.url).unwrap()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
