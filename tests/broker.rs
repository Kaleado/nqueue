mod common;

use std::thread;
use std::time::{Duration, Instant};

use common::Server;
use nqueue::broker::{Broker, Error, StartAt};
use nqueue::policy::{Delivery, Format, Mode, Policy, Urgency};

fn broadcast() -> Policy {
    Policy { mode: Mode::Broadcast, delivery: Delivery::AtMostOnce, ..Policy::default() }
}

fn compete() -> Policy {
    Policy { mode: Mode::Compete, ..Policy::default() }
}

/// A short lease, for tests that let it run out.
fn short_lease(p: Policy) -> Policy {
    Policy { lease_secs: 1, ..p }
}

fn past_lease() {
    thread::sleep(Duration::from_millis(1100));
}

/// A server with a queue `q` using `policy`, and agents `ids` subscribed to it.
fn setup(policy: Policy, ids: &[&str]) -> (Server, Broker) {
    let server = Server::start();
    let mut b = server.broker();
    b.create_queue("q", "test", policy).unwrap();
    b.register(Some("pub")).unwrap();
    for id in ids {
        b.register(Some(id)).unwrap();
        b.subscribe(id, "q", StartAt::Now).unwrap();
    }
    (server, b)
}

fn send(b: &mut Broker, body: &str) -> String {
    b.enqueue("pub", "q", body, None).unwrap()
}

fn bodies(b: &mut Broker, id: &str) -> Vec<String> {
    b.dequeue(id, false, None).unwrap().into_iter().map(|d| d.message.body).collect()
}

// identity

#[test]
fn registers_generated_and_requested_ids() {
    let server = Server::start();
    let mut b = server.broker();
    let id = b.register(None).unwrap();
    assert_eq!(id.split('-').count(), 3);
    assert_eq!(b.register(Some("reviewer")).unwrap(), "reviewer");
    assert!(matches!(b.register(Some("reviewer")), Err(Error::IdTaken(_))));
    assert!(matches!(b.register(Some("bad id")), Err(Error::InvalidId(_))));
}

#[test]
fn generated_ids_skip_ones_in_use() {
    let server = Server::start();
    let conn = redis::Client::open(server.url.as_str()).unwrap().get_connection().unwrap();
    let mut ids = ["calm-fox-11", "calm-fox-11", "calm-fox-12"].into_iter();
    let mut b = Broker::with_id_generator(conn, move || ids.next().unwrap().to_string());
    assert_eq!(b.register(None).unwrap(), "calm-fox-11");
    b.unregister("calm-fox-11").unwrap();
    assert_eq!(b.register(None).unwrap(), "calm-fox-12", "offline IDs are not handed out either");
}

#[test]
fn offline_id_can_be_reclaimed_with_its_subscriptions() {
    let (_s, mut b) = setup(broadcast(), &["a"]);
    b.unregister("a").unwrap();
    send(&mut b, "while away");
    b.register(Some("a")).unwrap();
    assert_eq!(bodies(&mut b, "a"), ["while away"]);
}

// broadcast

#[test]
fn broadcast_delivers_every_message_to_every_subscriber() {
    let (_s, mut b) = setup(broadcast(), &["a", "b"]);
    send(&mut b, "one");
    send(&mut b, "two");
    assert_eq!(bodies(&mut b, "a"), ["one", "two"]);
    assert_eq!(bodies(&mut b, "b"), ["one", "two"]);
    assert!(bodies(&mut b, "a").is_empty(), "at-most-once: not delivered again");
}

#[test]
fn broadcast_late_subscriber_can_replay_history() {
    let (_s, mut b) = setup(broadcast(), &[]);
    send(&mut b, "old");
    b.register(Some("now")).unwrap();
    b.subscribe("now", "q", StartAt::Now).unwrap();
    b.register(Some("replay")).unwrap();
    b.subscribe("replay", "q", StartAt::Beginning).unwrap();
    send(&mut b, "new");
    assert_eq!(bodies(&mut b, "now"), ["new"]);
    assert_eq!(bodies(&mut b, "replay"), ["old", "new"]);
}

#[test]
fn resubscribing_keeps_the_position() {
    let (_s, mut b) = setup(broadcast(), &["a"]);
    send(&mut b, "one");
    assert_eq!(bodies(&mut b, "a"), ["one"]);
    b.subscribe("a", "q", StartAt::Beginning).unwrap();
    assert!(bodies(&mut b, "a").is_empty());
}

#[test]
fn broadcast_without_echo_skips_own_messages() {
    for delivery in [Delivery::AtMostOnce, Delivery::AtLeastOnce] {
        let (_s, mut b) = setup(Policy { echo: false, delivery, ..broadcast() }, &["a", "b"]);
        b.enqueue("a", "q", "from a", None).unwrap();
        b.enqueue("b", "q", "from b", None).unwrap();
        assert_eq!(bodies(&mut b, "a"), ["from b"], "{delivery:?}");
        assert_eq!(bodies(&mut b, "b"), ["from a"], "{delivery:?}");
        assert!(bodies(&mut b, "a").is_empty(), "own message is not left leased ({delivery:?})");
    }
}

#[test]
fn broadcast_echoes_own_messages_by_default() {
    let (_s, mut b) = setup(broadcast(), &["a"]);
    b.enqueue("a", "q", "mine", None).unwrap();
    assert_eq!(bodies(&mut b, "a"), ["mine"]);
}

#[test]
fn broadcast_at_least_once_redelivers_to_same_subscriber() {
    let (_s, mut b) = setup(Policy { mode: Mode::Broadcast, ..Policy::default() }, &["a", "b"]);
    let id = send(&mut b, "m");
    b.dequeue("a", false, None).unwrap();
    b.nack("a", &id).unwrap();
    let again = b.dequeue("a", false, None).unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].attempt, 2);
    assert_eq!(bodies(&mut b, "b"), ["m"], "other subscriber is unaffected");
}

#[test]
fn broadcast_lease_held_when_going_offline_is_redelivered() {
    let (_s, mut b) = setup(short_lease(Policy { mode: Mode::Broadcast, ..Policy::default() }), &["a"]);
    send(&mut b, "leased");
    b.dequeue("a", false, None).unwrap();
    send(&mut b, "unread");
    b.unregister("a").unwrap();
    b.register(Some("a")).unwrap();
    past_lease();
    assert_eq!(bodies(&mut b, "a"), ["leased", "unread"]);
}

// compete

#[test]
fn compete_delivers_each_message_once() {
    let (_s, mut b) = setup(Policy { max_in_flight: 10, ..compete() }, &["a", "b"]);
    for i in 0..4 {
        send(&mut b, &i.to_string());
    }
    assert_eq!(b.dequeue("a", false, Some(3)).unwrap().len(), 3);
    assert_eq!(bodies(&mut b, "b"), ["3"]);
    assert!(bodies(&mut b, "a").is_empty());
}

#[test]
fn compete_keeps_messages_until_someone_reads() {
    let (_s, mut b) = setup(compete(), &[]);
    send(&mut b, "early");
    b.register(Some("late")).unwrap();
    b.subscribe("late", "q", StartAt::Now).unwrap();
    assert_eq!(bodies(&mut b, "late"), ["early"]);
}

#[test]
fn compete_respects_max_in_flight() {
    let (_s, mut b) = setup(compete(), &["a"]);
    let first = send(&mut b, "1");
    send(&mut b, "2");
    assert_eq!(bodies(&mut b, "a"), ["1"]);
    assert!(!b.pending("a", false).unwrap(), "full, so nothing to read");
    assert!(bodies(&mut b, "a").is_empty(), "the lease still counts as in flight");
    b.ack("a", &first).unwrap();
    assert!(b.pending("a", false).unwrap());
    assert_eq!(bodies(&mut b, "a"), ["2"]);
}

// leases

#[test]
fn ack_finishes_a_message() {
    let (_s, mut b) = setup(short_lease(compete()), &["a"]);
    let id = send(&mut b, "m");
    let d = b.dequeue("a", false, None).unwrap();
    assert!(d[0].lease_expires_at.is_some());
    b.ack("a", &id).unwrap();
    past_lease();
    assert!(bodies(&mut b, "a").is_empty(), "acked, so not redelivered");
    assert!(matches!(b.ack("a", &id), Err(Error::NotLeased(_))));
}

#[test]
fn only_the_holder_can_ack() {
    let (_s, mut b) = setup(compete(), &["a", "b"]);
    let id = send(&mut b, "m");
    bodies(&mut b, "a");
    assert!(matches!(b.ack("b", &id), Err(Error::NotLeased(_))));
    assert!(matches!(b.ack("a", "q/urgent/0-1"), Err(Error::NotLeased(_))));
    assert!(matches!(b.ack("a", "garbage"), Err(Error::NotLeased(_))));
}

#[test]
fn nack_releases_to_another_subscriber() {
    let (_s, mut b) = setup(compete(), &["a", "b"]);
    let id = send(&mut b, "m");
    assert_eq!(bodies(&mut b, "a"), ["m"]);
    b.nack("a", &id).unwrap();
    assert!(b.pending("b", false).unwrap());
    let d = b.dequeue("b", false, None).unwrap();
    assert_eq!(d[0].message.body, "m");
    assert_eq!(d[0].attempt, 2);
    b.ack("b", &id).unwrap();
}

#[test]
fn expired_lease_is_redelivered() {
    let (_s, mut b) = setup(short_lease(compete()), &["a", "b"]);
    send(&mut b, "m");
    b.dequeue("a", false, None).unwrap();
    assert!(!b.pending("b", false).unwrap());
    past_lease();
    assert!(b.pending("b", false).unwrap());
    let d = b.dequeue("b", false, None).unwrap();
    assert_eq!(d[0].attempt, 2);
}

#[test]
fn at_most_once_has_no_lease() {
    let (_s, mut b) = setup(Policy { delivery: Delivery::AtMostOnce, ..short_lease(compete()) }, &["a"]);
    let id = send(&mut b, "m");
    send(&mut b, "n");
    let d = b.dequeue("a", false, None).unwrap();
    assert_eq!(d.len(), 2, "no lease, so max-in-flight doesn't apply");
    assert_eq!(d[0].lease_expires_at, None);
    assert!(matches!(b.ack("a", &id), Err(Error::NotLeased(_))));
    past_lease();
    assert!(bodies(&mut b, "a").is_empty());
}

#[test]
fn going_offline_releases_compete_messages() {
    let (_s, mut b) = setup(compete(), &["a", "b"]);
    send(&mut b, "m");
    assert_eq!(bodies(&mut b, "a"), ["m"]);
    b.unregister("a").unwrap();
    assert_eq!(bodies(&mut b, "b"), ["m"]);
}

#[test]
fn unsubscribe_releases_compete_messages() {
    let (_s, mut b) = setup(compete(), &["a", "b"]);
    send(&mut b, "m");
    assert_eq!(bodies(&mut b, "a"), ["m"]);
    b.unsubscribe("a", "q").unwrap();
    assert!(bodies(&mut b, "a").is_empty());
    assert_eq!(bodies(&mut b, "b"), ["m"]);
}

// urgency

#[test]
fn urgent_only_dequeue_leaves_non_urgent_messages() {
    let (_s, mut b) = setup(Policy { max_in_flight: 5, ..compete() }, &["a"]);
    send(&mut b, "later");
    b.enqueue("pub", "q", "now", Some(Urgency::Urgent)).unwrap();
    assert!(b.pending("a", true).unwrap());
    let urgent: Vec<_> = b.dequeue("a", true, None).unwrap().into_iter().map(|d| d.message.body).collect();
    assert_eq!(urgent, ["now"]);
    assert!(!b.pending("a", true).unwrap());
    assert!(b.pending("a", false).unwrap());
    assert_eq!(bodies(&mut b, "a"), ["later"]);
}

#[test]
fn messages_take_the_queue_default_urgency() {
    let (_s, mut b) = setup(Policy { default_urgency: Urgency::Urgent, ..broadcast() }, &["a"]);
    let id = send(&mut b, "m");
    assert!(id.starts_with("q/urgent/"), "{id}");
    assert_eq!(b.dequeue("a", true, None).unwrap()[0].message.urgency, Urgency::Urgent);
}

#[test]
fn urgency_override_can_be_forbidden() {
    let (_s, mut b) = setup(Policy { allow_urgency_override: false, ..broadcast() }, &["a"]);
    assert!(matches!(b.enqueue("pub", "q", "m", Some(Urgency::Urgent)), Err(Error::Policy(_))));
    b.enqueue("pub", "q", "m", Some(Urgency::NonUrgent)).unwrap();
}

#[test]
fn dequeue_max_caps_a_delivery() {
    let (_s, mut b) = setup(broadcast(), &["a"]);
    for i in 0..5 {
        send(&mut b, &i.to_string());
    }
    assert_eq!(b.dequeue("a", false, Some(2)).unwrap().len(), 2);
    assert_eq!(bodies(&mut b, "a"), ["2", "3", "4"]);
}

// waiting

#[test]
fn wait_wakes_when_a_message_arrives() {
    let (server, mut b) = setup(broadcast(), &["a"]);
    let mut waiter_broker = server.broker();
    let waiter = thread::spawn(move || {
        let start = Instant::now();
        let pending = waiter_broker.wait("a", false, Some(Duration::from_secs(10))).unwrap();
        (pending, start.elapsed())
    });
    thread::sleep(Duration::from_millis(300));
    send(&mut b, "wake up");
    let (pending, waited) = waiter.join().unwrap();
    assert!(pending);
    assert!(waited < Duration::from_secs(3), "woke promptly, after {waited:?}");
}

#[test]
fn wait_times_out() {
    let (_s, mut b) = setup(broadcast(), &["a"]);
    assert!(!b.wait("a", false, Some(Duration::from_millis(200))).unwrap());
    send(&mut b, "later");
    assert!(!b.wait("a", true, Some(Duration::from_millis(200))).unwrap(), "urgent only");
    assert!(b.wait("a", false, Some(Duration::from_millis(200))).unwrap());
}

#[test]
fn wait_wakes_when_a_lease_expires() {
    let (_s, mut b) = setup(short_lease(compete()), &["a", "b"]);
    send(&mut b, "m");
    bodies(&mut b, "a");
    assert!(b.wait("b", false, Some(Duration::from_secs(5))).unwrap());
    assert_eq!(bodies(&mut b, "b"), ["m"]);
}

// enqueuing and dequeuing policies

#[test]
fn enforces_publisher_and_subscriber_allowlists() {
    let policy = Policy { publishers: Some(vec!["pub".into()]), subscribers: Some(vec!["a".into()]), ..broadcast() };
    let (_s, mut b) = setup(policy, &["a"]);
    b.register(Some("other")).unwrap();
    assert!(matches!(b.enqueue("other", "q", "m", None), Err(Error::Policy(_))));
    assert!(matches!(b.subscribe("other", "q", StartAt::Now), Err(Error::Policy(_))));
    send(&mut b, "ok");
}

#[test]
fn enforces_message_size_and_format() {
    let (_s, mut b) = setup(Policy { max_bytes: 10, format: Format::Json, ..broadcast() }, &[]);
    assert!(matches!(b.enqueue("pub", "q", "not json", None), Err(Error::Policy(_))));
    assert!(matches!(b.enqueue("pub", "q", "[1,2,3,4,5,6]", None), Err(Error::Policy(_))));
    b.enqueue("pub", "q", "{\"a\":1}", None).unwrap();
}

#[test]
fn limits_outstanding_messages_per_publisher() {
    let (_s, mut b) = setup(Policy { max_outstanding_per_publisher: Some(1), ..compete() }, &["a"]);
    let id = send(&mut b, "1");
    assert!(matches!(b.enqueue("pub", "q", "2", None), Err(Error::Policy(_))));
    b.dequeue("a", false, None).unwrap();
    assert!(b.enqueue("pub", "q", "2", None).is_err(), "leased is still unfinished");
    b.ack("a", &id).unwrap();
    send(&mut b, "2");
}

#[test]
fn unknown_agents_are_rejected() {
    let (_s, mut b) = setup(broadcast(), &[]);
    assert!(matches!(b.enqueue("ghost", "q", "m", None), Err(Error::UnknownAgent(_))));
    assert!(matches!(b.dequeue("ghost", false, None), Err(Error::UnknownAgent(_))));
}

// queues

#[test]
fn queue_lifecycle_errors() {
    let server = Server::start();
    let mut b = server.broker();
    b.register(Some("a")).unwrap();
    b.create_queue("q", "p", broadcast()).unwrap();
    assert!(matches!(b.create_queue("q", "p", broadcast()), Err(Error::QueueExists(_))));
    assert!(matches!(b.create_queue("a/b", "p", broadcast()), Err(Error::InvalidQueueName(_))));
    assert!(matches!(b.subscribe("a", "nope", StartAt::Now), Err(Error::UnknownQueue(_))));
    assert!(matches!(b.subscribe("ghost", "q", StartAt::Now), Err(Error::UnknownAgent(_))));
    assert!(matches!(b.unsubscribe("a", "q"), Err(Error::NotSubscribed { .. })));
}

#[test]
fn destroying_a_queue_drops_its_messages() {
    let (_s, mut b) = setup(broadcast(), &["a"]);
    send(&mut b, "m");
    b.destroy_queue("q").unwrap();
    assert!(!b.pending("a", false).unwrap());
    let status = b.status().unwrap();
    assert!(status.queues.is_empty());
    assert!(status.agents.iter().all(|a| a.subscriptions.is_empty()));
}

#[test]
fn status_reports_queues_and_agents() {
    let (_s, mut b) = setup(compete(), &["a"]);
    send(&mut b, "1");
    send(&mut b, "2");
    b.unregister("pub").unwrap();
    let s = b.status().unwrap();
    assert_eq!(s.queues[0].name, "q");
    assert_eq!(s.queues[0].policy, "test");
    assert_eq!(s.queues[0].messages, 2);
    assert_eq!(s.queues[0].subscribers, ["a"]);
    let a = s.agents.iter().find(|a| a.id == "a").unwrap();
    assert!(a.online);
    assert_eq!(a.subscriptions, ["q"]);
    assert!(!s.agents.iter().find(|a| a.id == "pub").unwrap().online);
}
