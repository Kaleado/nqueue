# nqueue - Queue System for Agents

## Goals

- Many patterns that agents use are basically queues that are flushed to insert
  messages when the agent becomes idle:
  - Chat messages
  - Review requests
  - Test requests
- Queues are a natural way to structure agent-to-agent communication because the
  lifecycle of an agent (busy vs. idle and looking for tasks) mirrors that of a
  consumer of messages
- We aim to use a common mechanism for agent-to-agent queues that inject
  messages upon flush

## Design

- A single common LLM harness extension is used to field all queue messages, but
  the meaning of each queue is application-defined -- skills assign meaning to
  specific queues and their messages
- Subscribers subscribe to queues so that messages can be assigned and delivered
  to them
- Publishers publish events to queues. Publishers can also be subscribers and
  vice versa
- Publishers and subscribers can be added at runtime
- New queues can be created and subscribers added to them

### Identity

- Agents (publishers and subscribers) are identified by a unique,
  human-readable ID, generated the same way as nchat nicks:
  `<adjective>-<noun>-<number>`, e.g. `gentle-otter-72`
  - adjective: one of swift, brave, calm, clever, eager, gentle, jolly, lucky,
    mellow, nimble, quiet, sunny, witty, zesty
  - noun: one of otter, fox, hawk, lynx, badger, heron, ibex, raven, stoat,
    tapir, wombat, gecko, puffin, marmot
  - number: 10-99
- `NQUEUE_ID` (or `--as`) gives the ID to act as, and overrides generation
- The ID is stored per session, so it survives the session being resumed,
  cleared or compacted, and is exported to the session's shell so the CLI uses
  it without passing it explicitly
- Uniqueness is enforced: registering an ID already held by an online agent
  fails, and generated IDs skip every ID already known. An offline
  agent's ID can be registered again, which brings it back with its
  subscriptions

### Valkey

nqueue is built on [Valkey](https://valkey.io) streams. There is no separate
queue daemon: `nqueue daemon` runs a `valkey-server` listening on
`~/.nqueue/valkey.sock`, persisting every write to an append-only file in
`~/.nqueue`, and the CLI talks to it directly. `NQUEUE_VALKEY` points the CLI at
another server.

### Queue storage and modes

- Each queue is a pair of Valkey streams, one per urgency level (see below), so
  urgent messages can be read on their own. A queue has one of two modes:
  - **broadcast**: every subscriber receives every message (e.g. chat
    channels). Each subscriber has its own consumer group, whose position is
    its place in the log
  - **compete**: each message goes to exactly one subscriber (e.g. review
    requests, test requests). Subscribers share one consumer group
- Because messages are kept in a log, a broadcast subscriber can start from
  any point in it, such as replaying history when it subscribes late

### Delivery

- Subscribers pull: when an agent reads (`nqueue dequeue`), it gets the next
  messages from its queues. In a compete queue a message goes to whichever
  subscriber reads first. Agents only read when they are idle or between tool
  calls, so idle agents naturally pick up work first
- A compete queue's policy caps how many messages one subscriber can have
  leased at once (`max-in-flight`); a subscriber at its cap reads nothing more
  from that queue until it acks or nacks

### Delivery guarantees

- Each queue's policy sets its delivery guarantee:
  - **at-most-once**: a message counts as handled once it is delivered. If the
    agent loses it (session ends, context is compacted) it is gone. Suitable
    for chat
  - **at-least-once**: a delivered message is leased to the subscriber (it
    stays in the consumer group's pending list). The subscriber must `ack` it
    when done, or `nack` it to return it to the queue. If the lease runs out
    without either, the next read by any subscriber claims it
    (`XAUTOCLAIM`). Suitable for review and test requests
- The lease starts when the message is read into the agent's context, so time
  spent waiting while the agent is busy does not count against it
- An agent that goes offline or unsubscribes releases the compete messages it
  holds, so other subscribers get them straight away
- Agents may receive the same message more than once under at-least-once, so
  skills should handle duplicates; each delivery says which attempt it is

### Urgency and delivery to agents

Delivery works the same way as nchat: the extension hooks into the agent's
lifecycle and injects messages from the subscriber's inbox. Each message has
one of two urgency levels; a queue sets the default and its enqueuing policy
says whether publishers may override it.

- **urgent**: delivered between tool calls while the agent is working
  (PostToolUse hook), as added context
- **non-urgent**: delivered when the agent finishes its turn (Stop hook), by
  blocking the stop with the messages as the reason so the agent handles them
  before going idle

Either level is also delivered:

- when the user submits a prompt (UserPromptSubmit hook)
- while the agent is idle, by a background waiter (an `asyncRewake` hook started
  at SessionStart and at Stop) which wakes the agent as soon as a message
  arrives

Messages that arrive close together are batched into one delivery, and each
delivery is capped in size so a backlog cannot flood the agent's context.

### Configuration

- There are two config files:
  - the home config, `~/.nqueue/config.toml`, applies everywhere
  - the repo config, `.nqueue.toml` at the top level of a git repo, applies
    anywhere inside that repo
- Where both define a policy or queue of the same name, the repo config's
  definition is used whole
- One Valkey server is shared by all repos, so config is resolved by the CLI
  from the caller's working directory, and `nqueue create` stores the
  resolved policy with the queue
- Every queue refers to a named policy from the config. `nqueue create <queue>`
  takes it from `--policy <name>`, or else from the queue's entry in the
  config; there is no built-in fallback policy

```toml
[policies.chat]
mode = "broadcast"
delivery = "at-most-once"

[policies.reviews]
mode = "compete"
delivery = "at-least-once"
lease-secs = 1800
max-in-flight = 1
default-urgency = "urgent"

[queues.reviews]
policy = "reviews"
```

### Policies

Policies are defined in the config files, and set:

- **Dequeuing policy**: Which agents can subscribe? Is the queue broadcast or
  compete? How many messages can a single subscriber have in flight? What is
  the delivery guarantee and lease duration? Do broadcast subscribers get
  their own messages back (`echo`)?
- **Enqueuing policy**: Which agents can enqueue? What is the format of a
  message? How many items can a single agent enqueue at a time? What is the
  default urgency, and can publishers override it?

### CLI

- The CLI is used by the extensions (and agents) to work with queues:
  - `nqueue daemon`: runs nqueue's Valkey server
  - `nqueue register` / `nqueue unregister`: brings an agent online or takes
    it offline
  - `nqueue enqueue`: enqueues a message to a given queue, optionally with an
    urgency. May return an error if the enqueuing policy is violated
  - `nqueue dequeue`: delivers a subscriber's waiting messages as JSON lines,
    leasing those from at-least-once queues
  - `nqueue wait`: blocks until a subscriber has messages to read; used by
    the background waiter
  - `nqueue ack`: marks a leased message as handled
  - `nqueue nack`: returns a leased message to its queue for redelivery
  - `nqueue create`: creates a queue with a policy from the config
  - `nqueue destroy`: removes a queue
  - `nqueue subscribe`: subscribes a subscriber to a given queue or queues
  - `nqueue unsubscribe`: the opposite of the above
  - `nqueue status`: shows queues and agents
  - `nqueue config`: shows the config that applies in the current directory

## Open questions

- Is an agent's ID trusted when it is claimed (as with nchat nicks), or
  verified?
- Should there be a standard way to reply to a message (a `reply_to` queue and
  correlation ID) for request/response patterns like reviews?
- What limits apply to queue depth and message age? Streams currently grow
  until their queue is destroyed

## Install

```sh
sudo pacman -S valkey          # or your distro's valkey package
cargo install --path .         # puts nqueue in ~/.cargo/bin
```

The extensions start `nqueue daemon` themselves if it isn't running.

### Claude Code plugin

`claude-plugin/` registers each session as an agent (ID kept per session in
`~/.nqueue/ids/`, exported as `NQUEUE_ID`), subscribes it to the queues in
`$NQUEUE_QUEUES` (comma-separated), and delivers messages through hooks:
PostToolUse for urgent ones, Stop and UserPromptSubmit for the rest, and an
`asyncRewake` waiter while idle.

```sh
claude plugin marketplace add ~/Repositories/nqueue
claude plugin install nqueue@nqueue-local
```

### Pi extension

`extension/nqueue.ts` does the same for pi: urgent messages are steered in
between tool calls, the rest start a new turn once a run ends, and a
background waiter wakes the idle agent. `/nqueue-check` delivers waiting
messages on demand.

```sh
ln -s ~/Repositories/nqueue/extension/nqueue.ts ~/.pi/agent/extensions/
ln -s ~/Repositories/nqueue/skills/nqueue ~/.pi/agent/skills/
```

Both share the `nqueue` skill (`claude-plugin/skills/nqueue`), which tells the
agent how to use the CLI and to ack leased messages.

## Development

```sh
cargo test                                     # needs valkey-server on PATH, or NQUEUE_VALKEY_SERVER
cargo build && node --test claude-plugin/test/*.test.mjs extension/test/*.test.mjs   # plugin hooks, pi extension
```

Each test starts its own throwaway Valkey server on a temporary socket.
