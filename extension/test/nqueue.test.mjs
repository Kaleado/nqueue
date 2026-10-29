// Drives the pi extension through pi's lifecycle events with a stand-in pi
// API, against a real nqueue server. Needs a built nqueue (NQUEUE_BIN,
// default target/debug/nqueue) and valkey-server (on PATH, or
// NQUEUE_VALKEY_SERVER).
//
//   node --test extension/test/*.test.mjs
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const BIN = process.env.NQUEUE_BIN || path.join(here, "../../target/debug/nqueue");
const home = fs.mkdtempSync(path.join(os.tmpdir(), "nqueue-pi-"));
// the extension reads these when it loads
process.env.NQUEUE_HOME = home;
process.env.NQUEUE_BIN = BIN;
process.env.NQUEUE_QUEUES = "chat,work";
delete process.env.NQUEUE_ID;
delete process.env.NQUEUE_VALKEY;

function nq(args, id) {
  const env = { ...process.env };
  if (id) env.NQUEUE_ID = id;
  else delete env.NQUEUE_ID;
  const r = spawnSync(BIN, args, { encoding: "utf8", env, cwd: home });
  assert.equal(r.status, 0, `nqueue ${args.join(" ")}: ${r.stderr}`);
  return r.stdout.trim();
}

// A stand-in for pi's ExtensionAPI: records handlers and sent messages.
const handlers = {};
const sent = [];
const notes = [];
const pi = {
  on: (event, fn) => { handlers[event] = fn; },
  sendMessage: (message, options) => { sent.push({ message, options }); },
  registerCommand: (name, opts) => { handlers[`command:${name}`] = opts.handler; },
};
const ctx = (sessionId) => ({
  sessionManager: { getSessionId: () => sessionId },
  ui: { notify: (text, level) => notes.push({ text, level }), setWidget: () => {} },
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function until(pred, ms = 6000) {
  for (const end = Date.now() + ms; Date.now() < end; await sleep(50)) if (pred()) return true;
  return pred();
}
const agent = (id) => JSON.parse(nq(["status"])).agents.find((a) => a.id === id);

let id;

before(async () => {
  fs.writeFileSync(
    path.join(home, "config.toml"),
    `[policies.chat]\nmode = "broadcast"\ndelivery = "at-most-once"\n\n[policies.work]\nmode = "compete"\nmax-in-flight = 5\n`,
  );
  spawn(BIN, ["daemon"], { detached: true, stdio: "ignore", env: process.env }).unref();
  for (let i = 0; i < 100 && spawnSync(BIN, ["status"], { env: process.env }).status !== 0; i++) await sleep(100);
  nq(["register", "boss"]);
  nq(["create", "chat", "--policy", "chat"]);
  nq(["create", "work", "--policy", "work"]);
  const { default: extension } = await import("../nqueue.ts");
  extension(pi);
});

after(async () => {
  await handlers.session_shutdown?.();
  spawnSync("pkill", ["-f", `${home}/valkey.sock`]);
  fs.rmSync(home, { recursive: true, force: true });
});

test("session_start registers the session and subscribes it", async () => {
  await handlers.session_start({}, ctx("sess-1"));
  id = fs.readFileSync(path.join(home, "ids", "pi-sess-1"), "utf8").trim();
  assert.match(id, /^[a-z]+-[a-z]+-\d\d$/);
  assert.equal(process.env.NQUEUE_ID, id, "exported for the agent's own nqueue commands");
  const a = agent(id);
  assert.equal(a.online, true);
  assert.deepEqual(a.subscriptions, ["chat", "work"]);
});

test("an idle agent is woken with a new turn", async () => {
  sent.length = 0;
  nq(["enqueue", "chat", "hello idle agent"], "boss");
  assert.ok(await until(() => sent.length > 0), "delivered");
  assert.equal(sent.length, 1);
  assert.deepEqual(sent[0].options, { triggerTurn: true });
  assert.match(sent[0].message.content, /\[chat\] from boss \(non-urgent\)[^\n]*\nhello idle agent/);
  assert.equal(sent[0].message.customType, "nqueue");
});

test("while busy, urgent messages are steered in and the rest wait for the run to end", async () => {
  sent.length = 0;
  await handlers.agent_start();
  nq(["enqueue", "chat", "for later"], "boss");
  await sleep(2500);
  assert.equal(sent.length, 0, "non-urgent waits while busy");

  nq(["enqueue", "chat", "--urgent", "right now"], "boss");
  assert.ok(await until(() => sent.length > 0), "urgent delivered while busy");
  assert.deepEqual(sent[0].options, { deliverAs: "steer" });
  assert.match(sent[0].message.content, /right now/);
  assert.doesNotMatch(sent[0].message.content, /for later/);

  await handlers.agent_end();
  assert.ok(await until(() => sent.length > 1), "non-urgent delivered once idle");
  assert.deepEqual(sent[1].options, { triggerTurn: true });
  assert.match(sent[1].message.content, /for later/);
});

test("leased messages come with ack instructions, and can be acked", async () => {
  sent.length = 0;
  const msgId = nq(["enqueue", "work", "review abc123"], "boss");
  assert.ok(await until(() => sent.length > 0));
  const text = sent[0].message.content;
  assert.match(text, new RegExp(`id ${msgId.replace(/[/-]/g, "\\$&")} — leased until`));
  assert.match(text, /nqueue ack <id>/);
  nq(["ack", msgId], id);
});

test("/nqueue-check delivers on demand, or says nothing is waiting", async () => {
  notes.length = 0;
  await handlers["command:nqueue-check"]();
  assert.ok(notes.some((n) => /no messages waiting/.test(n.text)));
});

test("a burst of messages arrives as one delivery", async () => {
  sent.length = 0;
  for (const n of [1, 2, 3]) nq(["enqueue", "chat", `burst ${n}`], "boss");
  assert.ok(await until(() => sent.length > 0));
  await sleep(1500);
  assert.equal(sent.length, 1);
  assert.match(sent[0].message.content, /3 message\(s\)[\s\S]*burst 1[\s\S]*burst 2[\s\S]*burst 3/);
});

test("session_shutdown takes the agent offline and stops waiting", async () => {
  await handlers.session_shutdown();
  assert.equal(agent(id).online, false);
  sent.length = 0;
  nq(["enqueue", "chat", "nobody home"], "boss");
  await sleep(2000);
  assert.equal(sent.length, 0);
});

test("resuming the session keeps its ID and gets what came meanwhile", async () => {
  await handlers.session_start({}, ctx("sess-1"));
  assert.equal(fs.readFileSync(path.join(home, "ids", "pi-sess-1"), "utf8").trim(), id);
  assert.equal(agent(id).online, true);
  assert.ok(await until(() => sent.some((s) => /nobody home/.test(s.message.content))));
});

test("a different session gets its own ID, and the old one goes offline", async () => {
  await handlers.session_start({}, ctx("sess-2"));
  const id2 = fs.readFileSync(path.join(home, "ids", "pi-sess-2"), "utf8").trim();
  assert.notEqual(id2, id);
  assert.equal(process.env.NQUEUE_ID, id2);
  assert.equal(agent(id2).online, true);
  assert.equal(agent(id).online, false);
});
