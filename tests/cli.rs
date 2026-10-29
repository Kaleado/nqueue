//! End-to-end tests of the `nqueue` binary.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

struct Env {
    _tmp: tempfile::TempDir,
    home: PathBuf,
    repo: PathBuf,
    daemon: Child,
}

impl Drop for Env {
    fn drop(&mut self) {
        let _ = self.daemon.kill();
        let _ = self.daemon.wait();
    }
}

/// Runs `nqueue daemon` and waits until it answers.
fn start_daemon(home: &Path) -> Child {
    let daemon = nqueue(home).arg("daemon").stdout(Stdio::null()).spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !nqueue(home).arg("status").output().unwrap().status.success() {
        assert!(Instant::now() < deadline, "daemon did not start");
        thread::sleep(Duration::from_millis(20));
    }
    daemon
}

const HOME_CONFIG: &str = r#"
[policies.chat]
mode = "broadcast"
delivery = "at-most-once"

[policies.work]
mode = "compete"
"#;

const REPO_CONFIG: &str = r#"
[policies.reviews]
mode = "compete"
default-urgency = "urgent"

[queues.reviews]
policy = "reviews"
"#;

fn nqueue(home: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_nqueue"));
    cmd.env("NQUEUE_HOME", home).env_remove("NQUEUE_ID").env_remove("NQUEUE_VALKEY");
    cmd
}

/// A home dir with a config, a git repo with a config, and a running daemon.
fn setup() -> Env {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let repo = tmp.path().join("repo");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(repo.join(".git")).unwrap();
    std::fs::create_dir_all(repo.join("src")).unwrap();
    std::fs::write(home.join("config.toml"), HOME_CONFIG).unwrap();
    std::fs::write(repo.join(".nqueue.toml"), REPO_CONFIG).unwrap();
    let daemon = start_daemon(&home);
    Env { _tmp: tmp, home, repo, daemon }
}

impl Env {
    /// Runs nqueue in `dir` as agent `id`.
    fn run(&self, dir: &Path, id: Option<&str>, args: &[&str]) -> Output {
        let mut cmd = nqueue(&self.home);
        cmd.current_dir(dir).args(args);
        if let Some(id) = id {
            cmd.env("NQUEUE_ID", id);
        }
        cmd.output().unwrap()
    }

    fn ok(&self, dir: &Path, id: Option<&str>, args: &[&str]) -> String {
        let out = self.run(dir, id, args);
        assert!(out.status.success(), "{args:?} failed: {}", String::from_utf8_lossy(&out.stderr));
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    }

    fn err(&self, dir: &Path, id: Option<&str>, args: &[&str]) -> String {
        let out = self.run(dir, id, args);
        assert!(!out.status.success(), "{args:?} should have failed");
        String::from_utf8(out.stderr).unwrap()
    }
}

fn deliveries(stdout: &str) -> Vec<Value> {
    stdout.lines().map(|l| serde_json::from_str(l).unwrap()).collect()
}

#[test]
fn queues_get_policies_from_home_and_repo_configs() {
    let env = setup();
    let outside = env.home.clone();
    let inside = env.repo.join("src");

    // outside the repo only home policies exist
    assert!(env.err(&outside, None, &["create", "reviews"]).contains("no policy for queue"));
    assert!(env.err(&outside, None, &["create", "x", "--policy", "reviews"]).contains("unknown policy"));
    env.ok(&outside, None, &["create", "general", "--policy", "chat"]);

    // inside the repo (at any depth) its [queues] entry supplies the policy
    env.ok(&inside, None, &["create", "reviews"]);
    env.ok(&inside, None, &["create", "jobs", "--policy", "work"]);

    let status: Value = serde_json::from_str(&env.ok(&outside, None, &["status"])).unwrap();
    let policies: Vec<(&str, &str)> = status["queues"]
        .as_array()
        .unwrap()
        .iter()
        .map(|q| (q["name"].as_str().unwrap(), q["policy"].as_str().unwrap()))
        .collect();
    assert_eq!(policies, [("general", "chat"), ("jobs", "work"), ("reviews", "reviews")]);

    let config = env.ok(&inside, None, &["config"]);
    assert!(config.contains("[policies.reviews]") && config.contains("[policies.chat]"), "{config}");
}

#[test]
fn review_request_flow() {
    let env = setup();
    let dir = &env.repo;
    let author = env.ok(dir, None, &["register"]);
    assert_eq!(author.split('-').count(), 3, "generated nchat-style ID: {author}");
    env.ok(dir, None, &["register", "reviewer"]);
    env.ok(dir, None, &["create", "reviews"]);
    env.ok(dir, Some("reviewer"), &["subscribe", "reviews"]);

    // nothing yet: wait times out with exit code 3
    assert_eq!(env.run(dir, Some("reviewer"), &["wait", "--timeout", "0.2"]).status.code(), Some(3));

    let id = env.ok(dir, Some(&author), &["enqueue", "reviews", "please review abc123"]);
    assert!(id.starts_with("reviews/urgent/"), "{id}");
    env.ok(dir, Some("reviewer"), &["wait", "--urgent-only", "--timeout", "2"]);

    let d = deliveries(&env.ok(dir, Some("reviewer"), &["dequeue"]));
    assert_eq!(d.len(), 1);
    assert_eq!(d[0]["message"]["body"], "please review abc123");
    assert_eq!(d[0]["message"]["from"], author.as_str());
    assert_eq!(d[0]["message"]["urgency"], "urgent", "the repo policy's default");
    assert!(d[0]["lease_expires_at"].is_u64());

    env.ok(dir, Some("reviewer"), &["ack", &id]);
    assert!(env.err(dir, Some("reviewer"), &["ack", &id]).contains("not leased"));
}

#[test]
fn enqueue_reads_body_from_stdin_and_honours_urgency_flags() {
    let env = setup();
    let dir = &env.repo;
    env.ok(dir, None, &["register", "a"]);
    env.ok(dir, None, &["create", "general", "--policy", "chat"]);
    env.ok(dir, Some("a"), &["subscribe", "general"]);

    let mut child = nqueue(&env.home)
        .current_dir(dir)
        .env("NQUEUE_ID", "a")
        .args(["enqueue", "general", "--urgent"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    std::io::Write::write_all(child.stdin.as_mut().unwrap(), b"multi\nline").unwrap();
    drop(child.stdin.take());
    assert!(child.wait().unwrap().success());

    let d = deliveries(&env.ok(dir, Some("a"), &["dequeue", "--urgent-only"]));
    assert_eq!(d[0]["message"]["body"], "multi\nline");
    assert_eq!(d[0]["message"]["urgency"], "urgent");
    assert_eq!(d[0]["lease_expires_at"], Value::Null, "at-most-once");
}

#[test]
fn agent_commands_need_an_id() {
    let env = setup();
    assert!(env.err(&env.repo, None, &["dequeue"]).contains("no agent ID"));
}

#[test]
fn queues_survive_a_daemon_restart() {
    let mut env = setup();
    let dir = env.repo.clone();
    env.ok(&dir, None, &["register", "a"]);
    env.ok(&dir, None, &["create", "reviews"]);
    env.ok(&dir, Some("a"), &["subscribe", "reviews"]);
    env.ok(&dir, Some("a"), &["enqueue", "reviews", "kept"]);

    // stop gracefully so the append-only file is flushed
    Command::new("kill").arg("-TERM").arg(env.daemon.id().to_string()).status().unwrap();
    env.daemon.wait().unwrap();
    env.daemon = start_daemon(&env.home);

    let d = deliveries(&env.ok(&dir, Some("a"), &["dequeue"]));
    assert_eq!(d[0]["message"]["body"], "kept");
}
