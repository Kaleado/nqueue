#!/usr/bin/env node
// nqueue Claude Code plugin: every hook runs this script, with the hook as
// its argument.
//
//   SessionStart      start the server if needed, register this session's
//                     agent, export NQUEUE_ID to the session's shell, and
//                     subscribe to $NQUEUE_QUEUES
//   PostToolUse       deliver urgent messages between tool calls
//   UserPromptSubmit  deliver every waiting message along with the prompt
//   Stop              deliver waiting messages by blocking the stop
//   Wait              (asyncRewake) wait while the session is idle, and wake
//                     it when messages arrive
//   SessionEnd        take the agent offline
//
// The agent ID is kept per session in ~/.nqueue/ids/cc-<session id>, so a
// resumed, cleared or compacted session keeps its ID and subscriptions.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync } from "node:child_process";

const NQUEUE_HOME = process.env.NQUEUE_HOME || path.join(os.homedir(), ".nqueue");
const NQUEUE_BIN = process.env.NQUEUE_BIN || "nqueue";
const IDS_DIR = path.join(NQUEUE_HOME, "ids");
const MAX_MESSAGES = 40; // per delivery, so a backlog can't flood the context
const BATCH_MS = 1000; // let a burst of messages arrive together
const WAIT_SECS = 30; // per `nqueue wait`, between checks that this waiter is still wanted

const hook = process.argv[2];
const input = JSON.parse(fs.readFileSync(0, "utf8"));
const sid = input.session_id || "default";
const idFile = path.join(IDS_DIR, `cc-${sid}`);
const waitFile = path.join(IDS_DIR, `cc-${sid}.wait`);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const read = (f) => { try { return fs.readFileSync(f, "utf8").trim() || null; } catch { return null; } };

let id = read(idFile);

function nq(args, timeout = 8000) {
  const r = spawnSync(NQUEUE_BIN, args, {
    encoding: "utf8",
    timeout,
    env: { ...process.env, ...(id ? { NQUEUE_ID: id } : {}) },
  });
  return { ok: r.status === 0, code: r.status, stdout: (r.stdout || "").trim(), stderr: (r.stderr || "").trim() || String(r.error || "") };
}

function out(obj) {
  process.stdout.write(JSON.stringify(obj));
}

// Messages waiting for this agent (urgent ones only if `urgentOnly`).
function dequeue(urgentOnly) {
  const r = nq(["dequeue", "--max", String(MAX_MESSAGES), ...(urgentOnly ? ["--urgent-only"] : [])]);
  if (!r.ok) return [];
  return r.stdout.split("\n").filter(Boolean).flatMap((l) => { try { return [JSON.parse(l)]; } catch { return []; } });
}

function format(deliveries) {
  const parts = deliveries.map((d) => {
    const m = d.message;
    const notes = [m.urgency];
    if (d.attempt > 1) notes.push(`attempt ${d.attempt}`);
    let head = `[${m.queue}] from ${m.from} (${notes.join(", ")}), id ${m.id}`;
    if (d.lease_expires_at) head += ` — leased until ${new Date(d.lease_expires_at).toISOString()}`;
    return `${head}\n${m.body}`;
  });
  let text = `[nqueue] ${deliveries.length} message(s):\n\n${parts.join("\n\n")}`;
  if (deliveries.some((d) => d.lease_expires_at)) {
    text += "\n\nLeased messages: run `nqueue ack <id>` once handled, or `nqueue nack <id>` to hand one back.";
  }
  return text;
}

// The server is `nqueue daemon` (a Valkey server); start it if it isn't up.
async function ensureServer() {
  if (nq(["status"]).ok) return true;
  let exited = false; // e.g. valkey-server isn't installed
  try {
    const child = spawn(NQUEUE_BIN, ["daemon"], { detached: true, stdio: "ignore" });
    child.on("exit", () => { exited = true; });
    child.on("error", () => { exited = true; });
    child.unref();
  } catch {
    return false;
  }
  const deadline = Date.now() + 8000;
  while (Date.now() < deadline && !exited) {
    await sleep(200);
    if (nq(["status"]).ok) return true;
  }
  return false;
}

async function sessionStart() {
  const context = [];
  if (!(await ensureServer())) {
    out({ hookSpecificOutput: { hookEventName: "SessionStart", additionalContext: "nqueue: the server is not running and could not be started (`nqueue daemon` needs valkey-server). Queue messages will not be delivered." } });
    return;
  }
  // this session's own ID may still look online if the session exited
  // without SessionEnd (a crash), so take it back
  let r;
  if (id) {
    nq(["unregister"]);
    r = nq(["register", id]);
  } else {
    r = nq(["register", ...(process.env.NQUEUE_ID ? [process.env.NQUEUE_ID] : [])]);
  }
  if (!r.ok) {
    out({ hookSpecificOutput: { hookEventName: "SessionStart", additionalContext: `nqueue: could not register: ${r.stderr}` } });
    return;
  }
  id = r.stdout;
  fs.mkdirSync(IDS_DIR, { recursive: true });
  fs.writeFileSync(idFile, id + "\n");
  if (process.env.CLAUDE_ENV_FILE) {
    try { fs.appendFileSync(process.env.CLAUDE_ENV_FILE, `export NQUEUE_ID=${id}\n`); } catch {}
  }
  const queues = (process.env.NQUEUE_QUEUES || "").split(",").map((q) => q.trim()).filter(Boolean);
  for (const q of queues) {
    const s = nq(["subscribe", q]);
    if (!s.ok) context.push(`nqueue: could not subscribe to ${q}: ${s.stderr}`);
  }
  const subs = JSON.parse(nq(["status"]).stdout || "{}").agents?.find((a) => a.id === id)?.subscriptions || [];
  context.unshift(
    `nqueue is active. Your agent ID is ${id} (exported as NQUEUE_ID, so nqueue commands use it). ` +
      `Subscribed queues: ${subs.length ? subs.join(", ") : "none"}. ` +
      "Messages from your queues are delivered automatically: urgent ones between tool calls, others when you finish a turn. " +
      "Ack leased messages with `nqueue ack <id>` once handled.",
  );
  out({ hookSpecificOutput: { hookEventName: "SessionStart", additionalContext: context.join("\n") } });
}

function deliverAsContext(urgentOnly) {
  const ds = dequeue(urgentOnly);
  if (ds.length) out({ hookSpecificOutput: { hookEventName: hook, additionalContext: format(ds) } });
}

function stop() {
  const ds = dequeue(false);
  if (ds.length) out({ decision: "block", reason: format(ds) });
}

// One waiter per session: a newer one replaces an older one, and
// SessionEnd stops it.
async function wait() {
  fs.mkdirSync(IDS_DIR, { recursive: true });
  const old = Number(read(waitFile));
  if (old && old !== process.pid) { try { process.kill(old); } catch {} }
  fs.writeFileSync(waitFile, String(process.pid));
  const mine = () => Number(read(waitFile)) === process.pid;
  await sleep(2000); // let the synchronous Stop hook take what's already there
  // at SessionStart this runs alongside registration: give it time to finish
  for (let i = 0; i < 50 && !read(idFile); i++) await sleep(500);
  for (;;) {
    id = read(idFile);
    if (!mine() || !id) process.exit(0);
    const r = await new Promise((resolve) => {
      const child = spawn(NQUEUE_BIN, ["wait", "--timeout", String(WAIT_SECS)], {
        stdio: "ignore",
        env: { ...process.env, NQUEUE_ID: id },
      });
      child.on("exit", (code) => resolve(code));
      child.on("error", () => resolve(null));
    });
    if (r === 0) {
      await sleep(BATCH_MS);
      if (!mine()) process.exit(0);
      const ds = dequeue(false);
      if (ds.length) {
        try { fs.unlinkSync(waitFile); } catch {}
        process.stderr.write(format(ds));
        process.exit(2); // wakes Claude with stderr as the message
      }
    } else if (r !== 3) {
      await sleep(5000); // server down or other error: retry later
    }
  }
}

function sessionEnd() {
  const pid = Number(read(waitFile));
  if (pid) { try { process.kill(pid); } catch {} }
  try { fs.unlinkSync(waitFile); } catch {}
  if (id) nq(["unregister"]);
}

switch (hook) {
  case "SessionStart": await sessionStart(); break;
  case "PostToolUse": if (id) deliverAsContext(true); break;
  case "UserPromptSubmit": if (id) deliverAsContext(false); break;
  case "Stop": if (id) stop(); break;
  case "Wait": await wait(); break;
  case "SessionEnd": sessionEnd(); break;
}
