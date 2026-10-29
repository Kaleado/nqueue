//! Queue policies and configuration files.
//!
//! Configuration is resolved from the caller's working directory, not by the
//! daemon: the home config (`~/.nqueue/config.toml`) applies everywhere, and
//! a repo config (`.nqueue.toml` at the top level of a git repo) applies
//! anywhere inside that repo. A policy or queue defined in both is taken
//! whole from the repo config.
//!
//! ```toml
//! [policies.chat]
//! mode = "broadcast"
//! delivery = "at-most-once"
//!
//! [policies.reviews]
//! mode = "compete"
//! delivery = "at-least-once"
//! lease-secs = 1800
//! default-urgency = "non-urgent"
//!
//! [queues.reviews]
//! policy = "reviews"
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// How a queue's messages are distributed among its subscribers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Mode {
    /// Every subscriber receives every message.
    Broadcast,
    /// Each message is assigned to exactly one subscriber.
    Compete,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Delivery {
    /// A message is handled once it is delivered.
    AtMostOnce,
    /// A delivered message is leased until it is acked; nacked or expired
    /// leases are redelivered.
    AtLeastOnce,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Urgency {
    /// Delivered between tool calls while the agent works.
    Urgent,
    /// Delivered when the agent finishes its turn.
    NonUrgent,
}

/// Required shape of a message body.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Format {
    Text,
    Json,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct Policy {
    // dequeuing policy
    pub mode: Mode,
    pub delivery: Delivery,
    pub lease_secs: u64,
    /// Most messages from this queue a subscriber can have leased at once
    /// (compete, at-least-once queues only).
    pub max_in_flight: usize,
    /// Agents allowed to subscribe; any agent if unset.
    pub subscribers: Option<Vec<String>>,
    /// Whether broadcast subscribers receive messages they enqueued
    /// themselves.
    pub echo: bool,

    // enqueuing policy
    /// Agents allowed to enqueue; any agent if unset.
    pub publishers: Option<Vec<String>>,
    pub format: Format,
    pub max_bytes: usize,
    pub default_urgency: Urgency,
    pub allow_urgency_override: bool,
    /// Most unfinished messages a single publisher can have in the queue at
    /// once (compete queues only); unlimited if unset.
    pub max_outstanding_per_publisher: Option<usize>,
}

impl Default for Policy {
    fn default() -> Self {
        Policy {
            mode: Mode::Compete,
            delivery: Delivery::AtLeastOnce,
            lease_secs: 600,
            max_in_flight: 1,
            subscribers: None,
            echo: true,
            publishers: None,
            format: Format::Text,
            max_bytes: 64 * 1024,
            default_urgency: Urgency::NonUrgent,
            allow_urgency_override: true,
            max_outstanding_per_publisher: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct QueueConfig {
    /// Name of a policy in `[policies]`.
    pub policy: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct Config {
    pub policies: BTreeMap<String, Policy>,
    /// Policies for queues created without naming one.
    pub queues: BTreeMap<String, QueueConfig>,
}

/// Name of the repo config file, at the top level of a git repo.
pub const REPO_CONFIG: &str = ".nqueue.toml";

/// The top level of the git repo containing `dir`: the nearest ancestor with
/// a `.git` entry (a directory, or a file in worktrees and submodules).
pub fn repo_root(dir: &Path) -> Option<&Path> {
    dir.ancestors().find(|d| d.join(".git").exists())
}

impl Config {
    /// Parses one config file. Its queues may refer to policies defined in
    /// other files, so references are checked by [`Config::validate`] once
    /// all files are merged.
    pub fn parse(text: &str) -> anyhow::Result<Config> {
        let config: Config = toml::from_str(text)?;
        for (name, p) in &config.policies {
            if p.max_in_flight == 0 {
                anyhow::bail!("policy {name:?}: max-in-flight must be at least 1");
            }
            if p.lease_secs == 0 {
                anyhow::bail!("policy {name:?}: lease-secs must be at least 1");
            }
        }
        Ok(config)
    }

    pub fn load(path: &Path) -> anyhow::Result<Config> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading {}: {e}", path.display()))?;
        Config::parse(&text).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))
    }

    /// Overlays `other` on `self`: its policies and queues replace any of
    /// the same name.
    pub fn merge(&mut self, other: Config) {
        self.policies.extend(other.policies);
        self.queues.extend(other.queues);
    }

    /// Checks that every queue names a known policy.
    pub fn validate(&self) -> anyhow::Result<()> {
        for (queue, qc) in &self.queues {
            if !self.policies.contains_key(&qc.policy) {
                anyhow::bail!("queue {queue:?} names unknown policy {:?}", qc.policy);
            }
        }
        Ok(())
    }

    /// The config files that apply in `cwd`, in the order they are merged:
    /// `home_config`, then the config of the git repo containing `cwd`. Files
    /// that don't exist are left out.
    pub fn files(home_config: Option<&Path>, cwd: &Path) -> Vec<PathBuf> {
        let repo_config = repo_root(cwd).map(|r| r.join(REPO_CONFIG));
        home_config
            .map(Path::to_path_buf)
            .into_iter()
            .chain(repo_config)
            .filter(|p| p.is_file())
            .collect()
    }

    /// Loads and merges the config files that apply in `cwd`.
    pub fn resolve(home_config: Option<&Path>, cwd: &Path) -> anyhow::Result<Config> {
        let mut config = Config::default();
        for file in Config::files(home_config, cwd) {
            config.merge(Config::load(&file)?);
        }
        config.validate()?;
        Ok(config)
    }

    /// The policy for a queue being created: `policy` if given, else the
    /// queue's entry in `[queues]`. Returns the policy's name and value.
    pub fn policy_for(&self, queue: &str, policy: Option<&str>) -> anyhow::Result<(String, Policy)> {
        let name = match policy {
            Some(p) => p.to_string(),
            None => match self.queues.get(queue) {
                Some(qc) => qc.policy.clone(),
                None => anyhow::bail!(
                    "no policy for queue {queue:?}: pass --policy or add [queues.{queue}] to a config file"
                ),
            },
        };
        match self.policies.get(&name) {
            Some(p) => Ok((name, p.clone())),
            None => anyhow::bail!("unknown policy {name:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_config() {
        let config = Config::parse(
            r#"
            [policies.chat]
            mode = "broadcast"
            delivery = "at-most-once"

            [policies.reviews]
            lease-secs = 1800
            max-in-flight = 3
            default-urgency = "urgent"
            publishers = ["gentle-otter-72"]

            [queues.reviews]
            policy = "reviews"
            "#,
        )
        .unwrap();
        let chat = &config.policies["chat"];
        assert_eq!(chat.mode, Mode::Broadcast);
        assert_eq!(chat.delivery, Delivery::AtMostOnce);
        assert_eq!(chat.lease_secs, 600, "unset fields take defaults");
        let reviews = &config.policies["reviews"];
        assert_eq!(reviews.mode, Mode::Compete);
        assert_eq!(reviews.max_in_flight, 3);
        assert_eq!(reviews.default_urgency, Urgency::Urgent);
        assert_eq!(reviews.publishers.as_deref(), Some(&["gentle-otter-72".to_string()][..]));
        assert_eq!(config.queues["reviews"].policy, "reviews");
        config.validate().unwrap();
    }

    #[test]
    fn rejects_unknown_fields_and_bad_values() {
        assert!(Config::parse("[policies.p]\nmood = \"broadcast\"").is_err());
        assert!(Config::parse("[policies.p]\nmode = \"fanout\"").is_err());
        assert!(Config::parse("[policies.p]\nmax-in-flight = 0").is_err());
        assert!(Config::parse("[queues.q]").is_err(), "a queue must name a policy");
    }

    #[test]
    fn queue_needs_known_policy() {
        let config = Config::parse("[queues.q]\npolicy = \"nope\"").unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn policy_for_queue() {
        let config = Config::parse(
            "[policies.a]\n[policies.b]\nmode = \"broadcast\"\n[queues.q]\npolicy = \"b\"",
        )
        .unwrap();
        assert_eq!(config.policy_for("q", None).unwrap().0, "b");
        assert_eq!(config.policy_for("q", Some("a")).unwrap().0, "a");
        assert!(config.policy_for("other", None).is_err(), "no policy named and no [queues] entry");
        assert!(config.policy_for("q", Some("nope")).is_err());
    }

    #[test]
    fn repo_config_overrides_home_config() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().join("home.toml");
        std::fs::write(
            &home,
            "[policies.shared]\nmode = \"broadcast\"\n[policies.home-only]\n[queues.q]\npolicy = \"shared\"\n[queues.r]\npolicy = \"home-only\"",
        )
        .unwrap();
        let repo = tmp.path().join("repo");
        let sub = repo.join("src/deeper");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::create_dir(repo.join(".git")).unwrap();
        std::fs::write(
            repo.join(REPO_CONFIG),
            "[policies.shared]\nlease-secs = 5\n[policies.repo]\n[queues.q]\npolicy = \"repo\"",
        )
        .unwrap();
        // only the top-level repo config counts
        std::fs::write(sub.join(REPO_CONFIG), "[policies.ignored]").unwrap();

        // outside the repo only the home config applies
        let c = Config::resolve(Some(&home), tmp.path()).unwrap();
        assert_eq!(c.policies["shared"].mode, Mode::Broadcast);
        assert!(!c.policies.contains_key("repo"));

        // anywhere in the repo, its policies and queues replace the home ones
        for dir in [&repo, &sub] {
            let c = Config::resolve(Some(&home), dir).unwrap();
            assert_eq!(c.policies["shared"].mode, Mode::Compete, "{}", dir.display());
            assert_eq!(c.policies["shared"].lease_secs, 5);
            assert!(c.policies.contains_key("home-only"));
            assert!(!c.policies.contains_key("ignored"));
            assert_eq!(c.queues["q"].policy, "repo");
            assert_eq!(c.queues["r"].policy, "home-only");
        }

        // a missing home config is fine
        let c = Config::resolve(Some(&tmp.path().join("missing.toml")), &sub).unwrap();
        assert!(!c.policies.contains_key("home-only"));
    }

    #[test]
    fn finds_repo_root_in_worktrees() {
        let tmp = tempfile::tempdir().unwrap();
        let wt = tmp.path().join("wt");
        std::fs::create_dir_all(wt.join("a/b")).unwrap();
        std::fs::write(wt.join(".git"), "gitdir: /elsewhere").unwrap();
        assert_eq!(repo_root(&wt.join("a/b")), Some(wt.as_path()));
        assert_eq!(repo_root(tmp.path()), None);
    }
}
