// Runs the plugin's hooks the way Claude Code does, against a real server.
// Needs a built nqueue (NQUEUE_BIN, default target/debug/nqueue) and
// valkey-server (on PATH, or NQUEUE_VALKEY_SERVER).
//
//   node --test claude-plugin/test/*.test.mjs
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const HOOK = path.join(here, "../scripts/hook.js");
const BIN = process.env.NQUEUE_BIN || path.join(here, "../../target/debug/nqueue");
const home = fs.mkdtempSync(path.join(os.tmpdir(), "nqueue-hook-"));
const env = { ...process.env, NQUEUE_HOME: home, NQUEUE_BIN: BIN };
delete env.NQUEUE_ID;
delete env.NQUEUE_VALKEY;
delete env.NQUEUE_QUEUES;

function nq(args, id) {
  const r = spawnSync(BIN, args, { encoding: "utf8", env: { ...env, ...(id ? { NQUEUE_ID: id } : {}) }, cwd: home });
  assert.equal(r.status, 0, `nqueue ${args.join(" ")}: ${r.stderr}`);
  return r.stdout.trim();
}

function hook(name, sessionId, extraEnv = {}) {
  const r = spawnSync("node", [HOOK, name], {
    encoding: "utf8",
    input: JSON.stringify({ session_id: sessionId }),
    env: { ...env, ...extraEnv },
  });
  return { code: r.status, stdout: r.stdout, stderr: r.stderr, json: r.stdout ? JSON.parse(r.stdout) : null };
}

function hookAsync(name, sessionId) {
  return new Promise((resolve) => {
    const child = spawn("node", [HOOK, name], { env });
    let stderr = "";
    child.stderr.on("data", (d) => (stderr += d));
    child.on("exit", (code) => resolve({ code, stderr }));
    child.stdin.end(JSON.stringify({ session_id: sessionId }));
  });
}

const sessionId = () => `s${Math.random().toString(36).slice(2)}`;
const idOf = (sid) => fs.readFileSync(path.join(home, "ids", `cc-${sid}`), "utf8").trim();

before(() => {
  fs.writeFileSync(
    path.join(home, "config.toml"),
    `[policies.chat]\nmode = "broadcast"\ndelivery = "at-most-once"\n\n[policies.work]\nmode = "compete"\n`,
  );
});

after(() => {
  spawnSync("pkill", ["-f", `${home}/valkey.sock`]);
  fs.rmSync(home, { recursive: true, force: true });
});

test("SessionStart starts the server, registers, exports the ID and subscribes", () => {
  const envFile = path.join(home, "env-file");
  // the server isn't running yet: SessionStart starts it
  const sid = sessionId();
  const first = hook("SessionStart", sid, { CLAUDE_ENV_FILE: envFile });
  assert.equal(first.code, 0, first.stderr);
  nq(["register", "creator"]);
  nq(["create", "general", "--policy", "chat"]);

  const sid2 = sessionId();
  const r = hook("SessionStart", sid2, { CLAUDE_ENV_FILE: envFile, NQUEUE_QUEUES: "general" });
  const id = idOf(sid2);
  assert.match(id, /^[a-z]+-[a-z]+-\d\d$/);
  assert.match(r.json.hookSpecificOutput.additionalContext, new RegExp(`agent ID is ${id}.*Subscribed queues: general`));
  assert.match(fs.readFileSync(envFile, "utf8"), new RegExp(`export NQUEUE_ID=${id}`));

  // resuming keeps the ID, even if it still looks online
  hook("SessionStart", sid2);
  assert.equal(idOf(sid2), id);
});

test("urgent messages are delivered after tool calls; the rest at Stop", () => {
  nq(["create", "jobs", "--policy", "work"]);
  const sid = sessionId();
  hook("SessionStart", sid);
  const id = idOf(sid);
  nq(["subscribe", "jobs"], id);

  nq(["enqueue", "jobs", "later please"], "creator");
  nq(["enqueue", "jobs", "--urgent", "now please"], "creator");

  const post = hook("PostToolUse", sid);
  const ctx = post.json.hookSpecificOutput.additionalContext;
  assert.match(ctx, /now please/);
  assert.doesNotMatch(ctx, /later please/);
  assert.match(ctx, /leased until/);
  assert.equal(hook("PostToolUse", sid).stdout, "", "nothing urgent left");

  // max-in-flight is 1 and "now please" is leased, so ack it first
  const leasedId = ctx.match(/id (jobs\/urgent\/\S+)/)[1];
  nq(["ack", leasedId], id);
  const stop = hook("Stop", sid);
  assert.equal(stop.json.decision, "block");
  assert.match(stop.json.reason, /later please/);
  assert.equal(hook("Stop", sid).stdout, "", "nothing left: the stop is allowed");
});

test("UserPromptSubmit delivers everything waiting", () => {
  const sid = sessionId();
  hook("SessionStart", sid, { NQUEUE_QUEUES: "general" });
  nq(["enqueue", "general", "hello all"], "creator");
  const r = hook("UserPromptSubmit", sid);
  assert.match(r.json.hookSpecificOutput.additionalContext, /\[general\] from creator \(non-urgent\)[^\n]*\nhello all/);
});

test("Wait wakes an idle session when a message arrives", async () => {
  const sid = sessionId();
  hook("SessionStart", sid, { NQUEUE_QUEUES: "general" });
  const waiting = hookAsync("Wait", sid);
  await new Promise((r) => setTimeout(r, 2500));
  nq(["enqueue", "general", "wake up"], "creator");
  const r = await waiting;
  assert.equal(r.code, 2);
  assert.match(r.stderr, /wake up/);
});

test("SessionEnd takes the agent offline and stops the waiter", async () => {
  const sid = sessionId();
  hook("SessionStart", sid);
  const id = idOf(sid);
  const waiting = hookAsync("Wait", sid);
  await new Promise((r) => setTimeout(r, 500));
  hook("SessionEnd", sid);
  const r = await waiting;
  assert.notEqual(r.code, 2);
  const status = JSON.parse(nq(["status"]));
  assert.equal(status.agents.find((a) => a.id === id).online, false);
});
