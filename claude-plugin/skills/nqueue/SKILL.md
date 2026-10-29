---
name: nqueue
description: Send and receive agent-to-agent queue messages with the nqueue CLI — subscribe to queues, enqueue messages, and ack or nack leased messages delivered to you. Use when a message tagged [nqueue] arrives, when a skill or the user asks you to post to or listen on an nqueue queue, or when creating queues from a config policy.
---

# nqueue

nqueue gives agents queues to talk through. Your session is registered as an
agent automatically; its ID is in `$NQUEUE_ID`, and every `nqueue` command
acts as that agent. What each queue means is up to the skill or person using
it — nqueue only moves messages.

## Receiving

Messages from queues you subscribe to are delivered to you automatically,
tagged `[nqueue]`:

- **urgent** messages arrive between tool calls while you work
- **non-urgent** messages arrive when you finish a turn
- if you're idle, a message wakes you

Each message shows its queue, sender, and ID. If it says **leased**, it came
from an at-least-once queue and someone is relying on it being handled:

- `nqueue ack <id>` once you've handled it
- `nqueue nack <id>` if you can't, so another subscriber gets it

An unacked lease runs out and the message is delivered again (to you or
another subscriber), so ack promptly. You may occasionally get a message
twice; the delivery says which attempt it is.

## Commands

```sh
nqueue subscribe <queue>...          # start receiving (--from-start replays broadcast history)
nqueue unsubscribe <queue>...
nqueue enqueue <queue> "<body>"      # prints the message ID; body from stdin if omitted
nqueue enqueue <queue> --urgent "…"  # or --non-urgent, if the queue allows overriding
nqueue ack <id> / nqueue nack <id>
nqueue dequeue                       # read waiting messages yourself (JSON lines)
nqueue status                        # queues, agents and subscriptions
nqueue create <queue> [--policy P]   # policy from ~/.nqueue/config.toml or the repo's .nqueue.toml
nqueue config                        # policies and queues configured here
```

Enqueue errors mean the queue's policy refused the message (not allowed to
publish, too big, wrong format, too many unfinished messages): report them
rather than retrying blindly.

## Queue kinds

- **broadcast**: every subscriber gets every message (like a chat channel)
- **compete**: each message goes to one subscriber (like a work queue);
  whoever is free first gets it
