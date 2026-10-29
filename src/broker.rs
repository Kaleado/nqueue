//! The broker: queues and agents, stored in Valkey.
//!
//! Each queue has two streams, one per urgency, so an agent can read only
//! its urgent messages. Delivery uses consumer groups, with each agent's ID
//! as its consumer name:
//!
//! - a compete queue has one group, `compete`, shared by its subscribers, so
//!   each message goes to whichever subscriber reads it first
//! - a broadcast queue has a group per subscriber, `sub:<id>`, whose position
//!   is that subscriber's place in the log
//!
//! A message read from an at-least-once queue stays in the group's pending
//! list (its lease) until acked. Once it has been pending longer than the
//! queue's lease, the next read by any of the group's consumers claims it
//! (`XAUTOCLAIM`). Nacked messages, and those held by an agent that leaves,
//! are moved to the [`RELEASED`] consumer with their idle time set to the
//! lease, so the next read claims them straight away. At-most-once queues
//! read with `NOACK`, so nothing is pending.
//!
//! Keys:
//!
//! | key                          | type   | contents                         |
//! |------------------------------|--------|----------------------------------|
//! | `nq:agents`                  | hash   | agent ID -> `online`/`offline`   |
//! | `nq:agent:<id>:subs`         | set    | queues the agent subscribes to   |
//! | `nq:queues`                  | hash   | queue -> policy name and policy  |
//! | `nq:queue:<q>:subs`          | set    | the queue's subscribers          |
//! | `nq:queue:<q>:outstanding`   | hash   | publisher -> unfinished messages |
//! | `nq:stream:<q>:<urgency>`    | stream | messages: `from`, `body`         |

use std::collections::HashMap;
use std::time::{Duration, Instant};

use redis::{Connection, Value};
use serde::{Deserialize, Serialize};

use crate::id;
use crate::policy::{Delivery, Format, Mode, Policy, Urgency};

pub type Millis = u64;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unknown agent {0:?}")]
    UnknownAgent(String),
    #[error("agent ID {0:?} is already in use")]
    IdTaken(String),
    #[error("invalid agent ID {0:?}")]
    InvalidId(String),
    #[error("could not generate an unused agent ID")]
    IdsExhausted,
    #[error("unknown queue {0:?}")]
    UnknownQueue(String),
    #[error("queue {0:?} already exists")]
    QueueExists(String),
    #[error("invalid queue name {0:?}")]
    InvalidQueueName(String),
    #[error("{agent:?} is not subscribed to queue {queue:?}")]
    NotSubscribed { agent: String, queue: String },
    #[error("{0:?} is not leased to this agent")]
    NotLeased(String),
    #[error("policy violation: {0}")]
    Policy(String),
    #[error("valkey: {0}")]
    Valkey(#[from] redis::RedisError),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Message {
    /// `<queue>/<urgency>/<stream entry ID>`.
    pub id: String,
    pub queue: String,
    pub from: String,
    pub urgency: Urgency,
    pub body: String,
    pub enqueued_at: Millis,
}

/// A message delivered to a subscriber.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Delivered {
    pub message: Message,
    /// 1 on first delivery; higher when the message is redelivered.
    pub attempt: u64,
    /// When the lease runs out (Unix ms), for at-least-once queues.
    pub lease_expires_at: Option<Millis>,
}

/// Where a broadcast subscriber starts reading a queue's log.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum StartAt {
    /// Only messages enqueued after subscribing.
    #[default]
    Now,
    /// Every message still in the log.
    Beginning,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueueStatus {
    pub name: String,
    pub policy: String,
    pub mode: Mode,
    pub messages: u64,
    pub subscribers: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentStatus {
    pub id: String,
    pub online: bool,
    pub subscriptions: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub queues: Vec<QueueStatus>,
    pub agents: Vec<AgentStatus>,
}

/// A queue's entry in `nq:queues`.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct QueueInfo {
    policy_name: String,
    policy: Policy,
}

impl QueueInfo {
    fn group(&self, agent: &str) -> String {
        match self.policy.mode {
            Mode::Compete => COMPETE_GROUP.to_string(),
            Mode::Broadcast => format!("sub:{agent}"),
        }
    }

    fn lease_ms(&self) -> u64 {
        self.policy.lease_secs * 1000
    }

    fn at_least_once(&self) -> bool {
        self.policy.delivery == Delivery::AtLeastOnce
    }
}

const AGENTS: &str = "nq:agents";
const QUEUES: &str = "nq:queues";
const COMPETE_GROUP: &str = "compete";
/// Consumer holding nacked and released messages until they are claimed
/// again; not a valid agent ID, so it can't clash with one.
pub const RELEASED: &str = "nq:released";
const URGENCIES: [Urgency; 2] = [Urgency::Urgent, Urgency::NonUrgent];
/// How long `wait` blocks per round before checking for expired leases.
const WAIT_ROUND: Duration = Duration::from_secs(1);

fn agent_subs(id: &str) -> String {
    format!("nq:agent:{id}:subs")
}

fn queue_subs(queue: &str) -> String {
    format!("nq:queue:{queue}:subs")
}

fn outstanding(queue: &str) -> String {
    format!("nq:queue:{queue}:outstanding")
}

fn urgency_name(u: Urgency) -> &'static str {
    match u {
        Urgency::Urgent => "urgent",
        Urgency::NonUrgent => "non-urgent",
    }
}

fn parse_urgency(s: &str) -> Option<Urgency> {
    URGENCIES.into_iter().find(|u| urgency_name(*u) == s)
}

fn stream(queue: &str, u: Urgency) -> String {
    format!("nq:stream:{queue}:{}", urgency_name(u))
}

fn levels(urgent_only: bool) -> &'static [Urgency] {
    if urgent_only {
        &URGENCIES[..1]
    } else {
        &URGENCIES
    }
}

/// Splits `<queue>/<urgency>/<entry ID>`.
fn parse_message_id(message_id: &str) -> Option<(&str, Urgency, &str)> {
    let mut parts = message_id.splitn(3, '/');
    let queue = parts.next()?;
    let urgency = parse_urgency(parts.next()?)?;
    let entry = parts.next()?;
    Some((queue, urgency, entry))
}

/// A stream entry: its ID and flattened field/value pairs.
type Entry = (String, Vec<String>);

/// One row of `XPENDING <key> <group> <start> <end> <count>`.
type PendingRow = (String, String, u64, u64);

const REGISTER: &str = r"
local state = redis.call('HGET', KEYS[1], ARGV[1])
if state == 'online' or (ARGV[2] == 'new' and state) then return 0 end
redis.call('HSET', KEYS[1], ARGV[1], 'online')
return 1
";

const ENQUEUE: &str = r"
if ARGV[4] == '1' then
  local n = tonumber(redis.call('HGET', KEYS[2], ARGV[1]) or '0')
  local max = tonumber(ARGV[5])
  if max >= 0 and n >= max then
    return redis.error_reply('OUTSTANDING ' .. n)
  end
  redis.call('HINCRBY', KEYS[2], ARGV[1], 1)
end
return redis.call('XADD', KEYS[1], '*', 'from', ARGV[1], 'body', ARGV[2])
";

pub struct Broker {
    conn: Connection,
    gen_id: Box<dyn FnMut() -> String + Send>,
}

impl Broker {
    /// Connects to Valkey at `url`, e.g. `valkey+unix:///path/to/socket`.
    pub fn connect(url: &str) -> Result<Broker> {
        Ok(Broker::new(redis::Client::open(url)?.get_connection()?))
    }

    pub fn new(conn: Connection) -> Broker {
        Broker::with_id_generator(conn, id::generate)
    }

    /// A broker that generates agent IDs with `gen_id`.
    pub fn with_id_generator(conn: Connection, gen_id: impl FnMut() -> String + Send + 'static) -> Broker {
        Broker { conn, gen_id: Box::new(gen_id) }
    }

    fn query<T: redis::FromRedisValue>(&mut self, cmd: &redis::Cmd) -> Result<T> {
        Ok(cmd.query(&mut self.conn)?)
    }

    fn check_agent(&mut self, id: &str) -> Result<()> {
        let exists: bool = self.query(redis::cmd("HEXISTS").arg(AGENTS).arg(id))?;
        if exists { Ok(()) } else { Err(Error::UnknownAgent(id.into())) }
    }

    fn queue_info(&mut self, queue: &str) -> Result<QueueInfo> {
        let raw: Option<String> = self.query(redis::cmd("HGET").arg(QUEUES).arg(queue))?;
        let raw = raw.ok_or_else(|| Error::UnknownQueue(queue.into()))?;
        Ok(serde_json::from_str(&raw).expect("queue info is written by the broker"))
    }

    fn subscriptions(&mut self, id: &str) -> Result<Vec<String>> {
        let mut subs: Vec<String> = self.query(redis::cmd("SMEMBERS").arg(agent_subs(id)))?;
        subs.sort();
        Ok(subs)
    }

    /// Valkey's clock, in Unix ms.
    fn now(&mut self) -> Result<Millis> {
        let (secs, micros): (u64, u64) = self.query(&redis::cmd("TIME"))?;
        Ok(secs * 1000 + micros / 1000)
    }

    // agents

    /// Brings an agent online. With `id`, that agent is registered, or brought
    /// back if it was offline (keeping its subscriptions); it fails if the ID
    /// is held by an online agent. Without `id`, a new unused one is
    /// generated. Returns the agent's ID.
    pub fn register(&mut self, id: Option<&str>) -> Result<String> {
        let script = redis::Script::new(REGISTER);
        if let Some(id) = id {
            if !id::is_valid(id) {
                return Err(Error::InvalidId(id.into()));
            }
            let ok: bool = script.key(AGENTS).arg(id).arg("reclaim").invoke(&mut self.conn)?;
            return if ok { Ok(id.to_string()) } else { Err(Error::IdTaken(id.into())) };
        }
        for _ in 0..1000 {
            let id = (self.gen_id)();
            let ok: bool = script.key(AGENTS).arg(&id).arg("new").invoke(&mut self.conn)?;
            if ok {
                return Ok(id);
            }
        }
        Err(Error::IdsExhausted)
    }

    /// Takes an agent offline. Its subscriptions are kept. Compete messages it
    /// held are released to other subscribers; broadcast messages it held
    /// are delivered again when it comes back.
    pub fn unregister(&mut self, id: &str) -> Result<()> {
        self.check_agent(id)?;
        let _: () = self.query(redis::cmd("HSET").arg(AGENTS).arg(id).arg("offline"))?;
        for queue in self.subscriptions(id)? {
            let Ok(info) = self.queue_info(&queue) else { continue };
            if info.policy.mode == Mode::Compete {
                self.release_all(&queue, &info, id)?;
            }
        }
        Ok(())
    }

    /// Moves every message `id` holds in a compete queue to [`RELEASED`].
    fn release_all(&mut self, queue: &str, info: &QueueInfo, id: &str) -> Result<()> {
        for &u in &URGENCIES {
            let key = stream(queue, u);
            let rows: Vec<PendingRow> =
                self.query(redis::cmd("XPENDING").arg(&key).arg(COMPETE_GROUP).arg("-").arg("+").arg(100_000).arg(id))?;
            let ids: Vec<String> = rows.into_iter().map(|r| r.0).collect();
            self.release(&key, COMPETE_GROUP, info, &ids)?;
        }
        Ok(())
    }

    /// Moves pending entries to [`RELEASED`], idle for a whole lease, so the
    /// next read claims them.
    fn release(&mut self, key: &str, group: &str, info: &QueueInfo, ids: &[String]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let _: Value = self.query(
            redis::cmd("XCLAIM")
                .arg(key)
                .arg(group)
                .arg(RELEASED)
                .arg(0)
                .arg(ids)
                .arg("IDLE")
                .arg(info.lease_ms())
                .arg("JUSTID"),
        )?;
        Ok(())
    }

    // queues

    pub fn create_queue(&mut self, name: &str, policy_name: &str, policy: Policy) -> Result<()> {
        if !id::is_valid(name) {
            return Err(Error::InvalidQueueName(name.into()));
        }
        if policy.max_in_flight == 0 {
            return Err(Error::Policy("max-in-flight must be at least 1".into()));
        }
        let info = QueueInfo { policy_name: policy_name.into(), policy };
        let json = serde_json::to_string(&info).unwrap();
        let created: bool = self.query(redis::cmd("HSETNX").arg(QUEUES).arg(name).arg(json))?;
        if !created {
            return Err(Error::QueueExists(name.into()));
        }
        if info.policy.mode == Mode::Compete {
            for &u in &URGENCIES {
                self.create_group(&stream(name, u), COMPETE_GROUP, "0")?;
            }
        }
        Ok(())
    }

    fn create_group(&mut self, key: &str, group: &str, start: &str) -> Result<()> {
        let result: redis::RedisResult<()> =
            redis::cmd("XGROUP").arg("CREATE").arg(key).arg(group).arg(start).arg("MKSTREAM").query(&mut self.conn);
        match result {
            Err(e) if e.code() == Some("BUSYGROUP") => Ok(()),
            r => Ok(r?),
        }
    }

    /// Removes a queue and its messages.
    pub fn destroy_queue(&mut self, name: &str) -> Result<()> {
        let removed: bool = self.query(redis::cmd("HDEL").arg(QUEUES).arg(name))?;
        if !removed {
            return Err(Error::UnknownQueue(name.into()));
        }
        let subs: Vec<String> = self.query(redis::cmd("SMEMBERS").arg(queue_subs(name)))?;
        for sub in subs {
            let _: () = self.query(redis::cmd("SREM").arg(agent_subs(&sub)).arg(name))?;
        }
        let _: () = self.query(
            redis::cmd("DEL")
                .arg(queue_subs(name))
                .arg(outstanding(name))
                .arg(stream(name, Urgency::Urgent))
                .arg(stream(name, Urgency::NonUrgent)),
        )?;
        Ok(())
    }

    pub fn subscribe(&mut self, id: &str, queue: &str, start: StartAt) -> Result<()> {
        self.check_agent(id)?;
        let info = self.queue_info(queue)?;
        if let Some(allowed) = &info.policy.subscribers {
            if !allowed.iter().any(|a| a == id) {
                return Err(Error::Policy(format!("{id:?} may not subscribe to {queue:?}")));
            }
        }
        if info.policy.mode == Mode::Broadcast {
            // an existing group is kept, so resubscribing keeps the position
            let start = match start {
                StartAt::Now => "$",
                StartAt::Beginning => "0",
            };
            for &u in &URGENCIES {
                self.create_group(&stream(queue, u), &info.group(id), start)?;
            }
        }
        let _: () = self.query(redis::cmd("SADD").arg(queue_subs(queue)).arg(id))?;
        let _: () = self.query(redis::cmd("SADD").arg(agent_subs(id)).arg(queue))?;
        Ok(())
    }

    /// Unsubscribes an agent. Compete messages it held are released to other
    /// subscribers; broadcast ones are dropped along with its position.
    pub fn unsubscribe(&mut self, id: &str, queue: &str) -> Result<()> {
        self.check_agent(id)?;
        let info = self.queue_info(queue)?;
        let removed: bool = self.query(redis::cmd("SREM").arg(queue_subs(queue)).arg(id))?;
        if !removed {
            return Err(Error::NotSubscribed { agent: id.into(), queue: queue.into() });
        }
        let _: () = self.query(redis::cmd("SREM").arg(agent_subs(id)).arg(queue))?;
        match info.policy.mode {
            Mode::Compete => {
                self.release_all(queue, &info, id)?;
                for &u in &URGENCIES {
                    let _: () =
                        self.query(redis::cmd("XGROUP").arg("DELCONSUMER").arg(stream(queue, u)).arg(COMPETE_GROUP).arg(id))?;
                }
            }
            Mode::Broadcast => {
                for &u in &URGENCIES {
                    let _: () = self.query(redis::cmd("XGROUP").arg("DESTROY").arg(stream(queue, u)).arg(info.group(id)))?;
                }
            }
        }
        Ok(())
    }

    // messages

    /// Enqueues a message, checking the queue's enqueuing policy. `urgency`
    /// overrides the queue's default if the policy allows it. Returns the
    /// message ID.
    pub fn enqueue(&mut self, id: &str, queue: &str, body: &str, urgency: Option<Urgency>) -> Result<String> {
        self.check_agent(id)?;
        let info = self.queue_info(queue)?;
        let p = &info.policy;
        if let Some(allowed) = &p.publishers {
            if !allowed.iter().any(|a| a == id) {
                return Err(Error::Policy(format!("{id:?} may not enqueue to {queue:?}")));
            }
        }
        if body.len() > p.max_bytes {
            return Err(Error::Policy(format!("message is {} bytes, more than the {} allowed", body.len(), p.max_bytes)));
        }
        if p.format == Format::Json {
            if let Err(e) = serde_json::from_str::<serde_json::Value>(body) {
                return Err(Error::Policy(format!("message is not valid JSON: {e}")));
            }
        }
        let urgency = match urgency {
            Some(u) if u != p.default_urgency && !p.allow_urgency_override => {
                return Err(Error::Policy(format!("{queue:?} does not allow overriding its urgency")));
            }
            Some(u) => u,
            None => p.default_urgency,
        };
        let track = p.mode == Mode::Compete;
        let max = p.max_outstanding_per_publisher.map_or(-1, |m| m as i64);
        let result: redis::RedisResult<String> = redis::Script::new(ENQUEUE)
            .key(stream(queue, urgency))
            .key(outstanding(queue))
            .arg(id)
            .arg(body)
            .arg(urgency_name(urgency))
            .arg(if track { "1" } else { "0" })
            .arg(max)
            .invoke(&mut self.conn);
        match result {
            Ok(entry) => Ok(format!("{queue}/{}/{entry}", urgency_name(urgency))),
            Err(e) if e.code() == Some("OUTSTANDING") => Err(Error::Policy(format!(
                "{id:?} already has {max} unfinished messages in {queue:?}"
            ))),
            Err(e) => Err(e.into()),
        }
    }

    /// Messages `id` holds in a compete queue, up to `limit`.
    fn in_flight(&mut self, queue: &str, id: &str, limit: usize) -> Result<usize> {
        let mut n = 0;
        for &u in &URGENCIES {
            let rows: Vec<PendingRow> =
                self.query(redis::cmd("XPENDING").arg(stream(queue, u)).arg(COMPETE_GROUP).arg("-").arg("+").arg(limit).arg(id))?;
            n += rows.len();
        }
        Ok(n)
    }

    /// How many more messages `id` may take from a queue now.
    fn capacity(&mut self, queue: &str, info: &QueueInfo, id: &str) -> Result<usize> {
        if info.policy.mode == Mode::Compete && info.at_least_once() {
            let max = info.policy.max_in_flight;
            Ok(max.saturating_sub(self.in_flight(queue, id, max)?))
        } else {
            Ok(usize::MAX)
        }
    }

    /// Delivers waiting messages to `id` (only urgent ones if `urgent_only`,
    /// at most `max` if given): first expired leases, then new messages.
    /// Messages from at-least-once queues are leased.
    pub fn dequeue(&mut self, id: &str, urgent_only: bool, max: Option<usize>) -> Result<Vec<Delivered>> {
        self.check_agent(id)?;
        let now = self.now()?;
        let max = max.unwrap_or(usize::MAX);
        let mut out = Vec::new();
        for queue in self.subscriptions(id)? {
            let Ok(info) = self.queue_info(&queue) else { continue };
            let group = info.group(id);
            let lease_expires_at = info.at_least_once().then(|| now + info.lease_ms());
            let mut n = (max - out.len()).min(self.capacity(&queue, &info, id)?);
            for &u in levels(urgent_only) {
                let key = stream(&queue, u);
                let mut entries: Vec<(Entry, u64)> = Vec::new();
                if info.at_least_once() && n > 0 {
                    let (_, claimed, _): (String, Vec<Entry>, Vec<String>) = self.query(
                        redis::cmd("XAUTOCLAIM").arg(&key).arg(&group).arg(id).arg(info.lease_ms()).arg("0-0").arg("COUNT").arg(n.min(1000)),
                    )?;
                    for entry in claimed {
                        let rows: Vec<PendingRow> =
                            self.query(redis::cmd("XPENDING").arg(&key).arg(&group).arg(&entry.0).arg(&entry.0).arg(1))?;
                        let attempt = rows.first().map_or(1, |r| r.3);
                        entries.push((entry, attempt));
                    }
                }
                let left = n - entries.len();
                if left > 0 {
                    let mut read = redis::cmd("XREADGROUP");
                    read.arg("GROUP").arg(&group).arg(id).arg("COUNT").arg(left.min(1000));
                    if !info.at_least_once() {
                        read.arg("NOACK");
                    }
                    read.arg("STREAMS").arg(&key).arg(">");
                    let reply: Option<Vec<(String, Vec<Entry>)>> = self.query(&read)?;
                    for entry in reply.into_iter().flatten().flat_map(|(_, e)| e) {
                        if !info.policy.echo && info.policy.mode == Mode::Broadcast && field(&entry.1, "from") == Some(id) {
                            if info.at_least_once() {
                                let _: () = self.query(redis::cmd("XACK").arg(&key).arg(&group).arg(&entry.0))?;
                            }
                            continue;
                        }
                        if info.policy.mode == Mode::Compete && !info.at_least_once() {
                            self.finish(&queue, &entry)?;
                        }
                        entries.push((entry, 1));
                    }
                }
                n -= entries.len();
                for ((entry_id, fields), attempt) in entries {
                    out.push(Delivered {
                        message: to_message(&queue, u, entry_id, fields),
                        attempt,
                        lease_expires_at,
                    });
                }
            }
            if out.len() >= max {
                break;
            }
        }
        Ok(out)
    }

    /// A compete message is finished: its publisher has one fewer
    /// outstanding.
    fn finish(&mut self, queue: &str, entry: &Entry) -> Result<()> {
        if let Some(from) = field(&entry.1, "from") {
            let _: () = self.query(redis::cmd("HINCRBY").arg(outstanding(queue)).arg(from).arg(-1))?;
        }
        Ok(())
    }

    /// Checks `message_id` is leased to `id`; returns its queue info, stream
    /// key, group and entry ID.
    fn leased(&mut self, id: &str, message_id: &str) -> Result<(QueueInfo, String, String, String)> {
        self.check_agent(id)?;
        let not_leased = || Error::NotLeased(message_id.into());
        let (queue, u, entry) = parse_message_id(message_id).ok_or_else(not_leased)?;
        let info = self.queue_info(queue).map_err(|_| not_leased())?;
        let key = stream(queue, u);
        let group = info.group(id);
        let rows: redis::RedisResult<Vec<PendingRow>> =
            redis::cmd("XPENDING").arg(&key).arg(&group).arg(entry).arg(entry).arg(1).query(&mut self.conn);
        match rows {
            Ok(rows) if rows.first().is_some_and(|r| r.1 == id) => Ok((info, key, group, entry.to_string())),
            Ok(_) => Err(not_leased()),
            Err(e) if e.code() == Some("NOGROUP") => Err(not_leased()),
            Err(e) => Err(e.into()),
        }
    }

    /// Marks a leased message as handled.
    pub fn ack(&mut self, id: &str, message_id: &str) -> Result<()> {
        let (info, key, group, entry) = self.leased(id, message_id)?;
        let _: () = self.query(redis::cmd("XACK").arg(&key).arg(&group).arg(&entry))?;
        if info.policy.mode == Mode::Compete {
            let entries: Vec<Entry> = self.query(redis::cmd("XRANGE").arg(&key).arg(&entry).arg(&entry))?;
            if let Some(e) = entries.first() {
                let queue = message_id.split('/').next().unwrap();
                self.finish(queue, e)?;
            }
        }
        Ok(())
    }

    /// Returns a leased message to its queue to be delivered again: to any
    /// subscriber of a compete queue, to the same one of a broadcast queue.
    pub fn nack(&mut self, id: &str, message_id: &str) -> Result<()> {
        let (info, key, group, entry) = self.leased(id, message_id)?;
        self.release(&key, &group, &info, &[entry])
    }

    /// The last entry `group` has read from `key`, if the group exists.
    fn last_delivered(&mut self, key: &str, group: &str) -> Result<Option<String>> {
        let groups: redis::RedisResult<Vec<HashMap<String, Value>>> =
            redis::cmd("XINFO").arg("GROUPS").arg(key).query(&mut self.conn);
        let groups = match groups {
            Err(e) if e.to_string().contains("no such key") => return Ok(None),
            g => g?,
        };
        for g in groups {
            let name: Option<String> = g.get("name").and_then(|v| redis::from_redis_value(v.clone()).ok());
            if name.as_deref() == Some(group) {
                return Ok(g.get("last-delivered-id").and_then(|v| redis::from_redis_value(v.clone()).ok()));
            }
        }
        Ok(None)
    }

    /// The streams `id` reads (only urgent ones if `urgent_only`), with each
    /// one's group, the group's last read entry, and the queue's info.
    fn readable(&mut self, id: &str, urgent_only: bool) -> Result<Vec<(String, String, String, QueueInfo)>> {
        let mut out = Vec::new();
        for queue in self.subscriptions(id)? {
            let Ok(info) = self.queue_info(&queue) else { continue };
            if self.capacity(&queue, &info, id)? == 0 {
                continue;
            }
            let group = info.group(id);
            for &u in levels(urgent_only) {
                let key = stream(&queue, u);
                if let Some(last) = self.last_delivered(&key, &group)? {
                    out.push((key, group.clone(), last, info.clone()));
                }
            }
        }
        Ok(out)
    }

    /// Whether `id` has messages it can read now (only urgent ones if
    /// `urgent_only`): new ones, or expired leases.
    pub fn pending(&mut self, id: &str, urgent_only: bool) -> Result<bool> {
        self.check_agent(id)?;
        for (key, group, last, info) in self.readable(id, urgent_only)? {
            let newer: Vec<Entry> =
                self.query(redis::cmd("XRANGE").arg(&key).arg(format!("({last}")).arg("+").arg("COUNT").arg(1))?;
            if !newer.is_empty() {
                return Ok(true);
            }
            if info.at_least_once() {
                let expired: Vec<PendingRow> = self.query(
                    redis::cmd("XPENDING").arg(&key).arg(&group).arg("IDLE").arg(info.lease_ms()).arg("-").arg("+").arg(1),
                )?;
                if !expired.is_empty() {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Blocks until `id` has messages it can read (see [`Broker::pending`]),
    /// or `timeout` passes. Returns whether there are.
    pub fn wait(&mut self, id: &str, urgent_only: bool, timeout: Option<Duration>) -> Result<bool> {
        let deadline = timeout.map(|t| Instant::now() + t);
        loop {
            if self.pending(id, urgent_only)? {
                return Ok(true);
            }
            let mut round = WAIT_ROUND;
            if let Some(deadline) = deadline {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return Ok(false);
                }
                round = round.min(left);
            }
            let streams = self.readable(id, urgent_only)?;
            if streams.is_empty() {
                std::thread::sleep(round);
                continue;
            }
            // wakes when a new entry lands; expired leases are caught by the
            // next round's check
            let mut read = redis::cmd("XREAD");
            read.arg("COUNT").arg(1).arg("BLOCK").arg(round.as_millis().max(1) as u64).arg("STREAMS");
            for (key, ..) in &streams {
                read.arg(key);
            }
            for (_, _, last, _) in &streams {
                read.arg(last);
            }
            let _: Value = self.query(&read)?;
        }
    }

    pub fn status(&mut self) -> Result<Status> {
        let raw: HashMap<String, String> = self.query(redis::cmd("HGETALL").arg(QUEUES))?;
        let mut queues = Vec::new();
        for (name, json) in raw {
            let info: QueueInfo = serde_json::from_str(&json).expect("queue info is written by the broker");
            let mut messages = 0;
            for &u in &URGENCIES {
                let n: u64 = self.query(redis::cmd("XLEN").arg(stream(&name, u)))?;
                messages += n;
            }
            let mut subscribers: Vec<String> = self.query(redis::cmd("SMEMBERS").arg(queue_subs(&name)))?;
            subscribers.sort();
            queues.push(QueueStatus { name, policy: info.policy_name, mode: info.policy.mode, messages, subscribers });
        }
        queues.sort_by(|a, b| a.name.cmp(&b.name));
        let raw: HashMap<String, String> = self.query(redis::cmd("HGETALL").arg(AGENTS))?;
        let mut agents = Vec::new();
        for (id, state) in raw {
            let subscriptions = self.subscriptions(&id)?;
            agents.push(AgentStatus { online: state == "online", id, subscriptions });
        }
        agents.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(Status { queues, agents })
    }
}

fn field<'a>(fields: &'a [String], name: &str) -> Option<&'a str> {
    fields.chunks(2).find(|kv| kv[0] == name).and_then(|kv| kv.get(1)).map(String::as_str)
}

fn to_message(queue: &str, urgency: Urgency, entry_id: String, fields: Vec<String>) -> Message {
    let enqueued_at = entry_id.split('-').next().and_then(|ms| ms.parse().ok()).unwrap_or(0);
    Message {
        id: format!("{queue}/{}/{entry_id}", urgency_name(urgency)),
        queue: queue.to_string(),
        from: field(&fields, "from").unwrap_or_default().to_string(),
        urgency,
        body: field(&fields, "body").unwrap_or_default().to_string(),
        enqueued_at,
    }
}
