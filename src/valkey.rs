//! Where nqueue's Valkey server lives, and how to start it.

use std::path::{Path, PathBuf};
use std::process::Command;

/// `$NQUEUE_HOME`, else `~/.nqueue`.
pub fn home_dir() -> PathBuf {
    match std::env::var_os("NQUEUE_HOME") {
        Some(dir) => PathBuf::from(dir),
        None => PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".nqueue"),
    }
}

pub fn home_config() -> PathBuf {
    home_dir().join("config.toml")
}

/// The socket `nqueue daemon` listens on.
pub fn default_socket() -> PathBuf {
    home_dir().join("valkey.sock")
}

/// `$NQUEUE_VALKEY`, else the default socket.
pub fn url() -> String {
    std::env::var("NQUEUE_VALKEY").unwrap_or_else(|_| socket_url(&default_socket()))
}

pub fn socket_url(socket: &Path) -> String {
    format!("valkey+unix://{}", socket.display())
}

/// `$NQUEUE_VALKEY_SERVER`, else `valkey-server` from `PATH`.
pub fn server_binary() -> PathBuf {
    std::env::var_os("NQUEUE_VALKEY_SERVER").map_or_else(|| "valkey-server".into(), PathBuf::from)
}

/// A `valkey-server` listening only on `socket`, keeping its data in `dir`.
/// With `persist`, every write is logged to an append-only file there, so
/// queues survive a restart.
pub fn server_command(dir: &Path, socket: &Path, persist: bool) -> Command {
    let mut cmd = Command::new(server_binary());
    cmd.arg("--port")
        .arg("0")
        .arg("--unixsocket")
        .arg(socket)
        .arg("--unixsocketperm")
        .arg("700")
        .arg("--dir")
        .arg(dir)
        .arg("--save")
        .arg("")
        .arg("--appendonly")
        .arg(if persist { "yes" } else { "no" });
    cmd
}
