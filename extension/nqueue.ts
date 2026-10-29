// nqueue pi extension: registers this pi session as an nqueue agent and
// delivers messages from the queues it subscribes to. Urgent messages are
// steered in between tool calls while the agent works; others wait until the
// run ends. While idle, a background `nqueue wait` wakes the agent as soon as
// messages arrive.
//
// The agent ID is kept per session in ~/.nqueue/ids/pi-<session id>, so a
// resumed session keeps its ID and subscriptions. It is exported as
// NQUEUE_ID, so nqueue commands the agent runs use it. $NQUEUE_QUEUES
// (comma-separated) lists queues to subscribe to at startup.
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const NQUEUE_HOME = process.env.NQUEUE_HOME || path.join(os.homedir(), ".nqueue");
const NQUEUE_BIN = process.env.NQUEUE_BIN || "nqueue";
const IDS_DIR = path.join(NQUEUE_HOME, "ids");
const MAX_MESSAGES = 40; // per delivery, so a backlog can't flood the context
const BATCH_MS = 1000; // let a burst of messages arrive together
const WAIT_SECS = 30;
const QUEUES = (process.env.NQUEUE_QUEUES || "").split(",").map((q) => q.trim()).filter(Boolean);

interface Delivered {
  message: { id: string; queue: string; from: string; urgency: string; body: string };
  attempt: number;
  lease_expires_at: number | null;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const read = (f: string) => { try { return fs.readFileSync(f, "utf8").trim() || null; } catch { return null; } };

function nq(args: string[], id?: string | null) {
  const r = spawnSync(NQUEUE_BIN, args, {
    encoding: "utf8",
    timeout: 8000,
    env: { ...process.env, ...(id ? { NQUEUE_ID: id } : {}) },
  });
  return { ok: r.status === 0, stdout: (r.stdout || "").trim(), stderr: (r.stderr || "").trim() || String(r.error || "") };
}

function format(deliveries: Delivered[]): string {
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

export default function nqueueExtension(pi: ExtensionAPI) {
  // pi can load this file twice (from ~/.pi/agent/extensions and from
  // --extension): only the first copy runs, or every message comes twice
  const g = globalThis as any;
  if (g.__nqueueExtensionLoaded) return;
  g.__nqueueExtensionLoaded = true;

  let id: string | null = null;
  let busy = false; // a run is in progress
  let closed = true;
  let lastCtx: ExtensionContext | null = null;
  let waiter: ChildProcess | null = null;
  let loopGen = 0; // bumped to stop the current wait loop

  function notifyHuman(text: string, level = "info") {
    try { (lastCtx as any)?.ui?.notify?.(text, level); } catch {}
  }

  function updateWidget(extra?: string) {
    try {
      (lastCtx as any)?.ui?.setWidget?.("nqueue", [`nqueue: ${id ?? "-"}${extra ? ` ${extra}` : ""}`], { placement: "belowEditor" });
    } catch {}
  }

  async function ensureServer(): Promise<boolean> {
    if (nq(["status"]).ok) return true;
    let exited = false; // e.g. valkey-server isn't installed
    try {
      const child = spawn(NQUEUE_BIN, ["daemon"], { detached: true, stdio: "ignore" });
      child.on("exit", () => { exited = true; });
      child.on("error", () => { exited = true; });
      child.unref();
    } catch { return false; }
    const deadline = Date.now() + 8000;
    while (Date.now() < deadline && !exited) {
      await sleep(200);
      if (nq(["status"]).ok) return true;
    }
    return false;
  }

  function register(sessionId: string | null): string | null {
    const idFile = sessionId ? path.join(IDS_DIR, `pi-${sessionId}`) : null;
    const recorded = idFile && read(idFile);
    let r;
    if (recorded) {
      // our own ID may still look online after a crash: take it back
      nq(["unregister"], recorded);
      r = nq(["register", recorded]);
    } else {
      r = nq(["register", ...(process.env.NQUEUE_ID ? [process.env.NQUEUE_ID] : [])]);
    }
    if (!r.ok) {
      notifyHuman(`nqueue: could not register: ${r.stderr}`, "error");
      return null;
    }
    const newId = r.stdout;
    if (idFile) {
      try {
        fs.mkdirSync(IDS_DIR, { recursive: true });
        fs.writeFileSync(idFile, newId + "\n");
      } catch {}
    }
    for (const q of QUEUES) {
      const s = nq(["subscribe", q], newId);
      if (!s.ok) notifyHuman(`nqueue: could not subscribe to ${q}: ${s.stderr}`, "warning");
    }
    return newId;
  }

  // Delivers waiting messages: urgent ones are steered into a run in
  // progress, the rest start a new turn once the agent is idle.
  function deliver(): boolean {
    const urgentOnly = busy;
    const r = nq(["dequeue", "--max", String(MAX_MESSAGES), ...(urgentOnly ? ["--urgent-only"] : [])], id);
    if (!r.ok) return false;
    const deliveries: Delivered[] = r.stdout.split("\n").filter(Boolean).flatMap((l) => { try { return [JSON.parse(l)]; } catch { return []; } });
    if (!deliveries.length) return false;
    notifyHuman(`nqueue: ${deliveries.length} new message(s)`);
    pi.sendMessage(
      { customType: "nqueue", content: format(deliveries), display: true },
      busy ? { deliverAs: "steer" } : { triggerTurn: true },
    );
    return true;
  }

  // Waits for messages (only urgent ones while busy) and delivers them.
  async function loop(gen: number) {
    while (!closed && gen === loopGen && id) {
      const args = ["wait", "--timeout", String(WAIT_SECS), ...(busy ? ["--urgent-only"] : [])];
      const code = await new Promise<number | null>((resolve) => {
        const child = spawn(NQUEUE_BIN, args, { stdio: "ignore", env: { ...process.env, NQUEUE_ID: id! } });
        waiter = child;
        child.on("exit", (c) => resolve(c));
        child.on("error", () => resolve(null));
      });
      waiter = null;
      if (closed || gen !== loopGen) return;
      if (code === 0) {
        await sleep(BATCH_MS);
        if (closed || gen !== loopGen) return;
        deliver();
      } else if (code !== 3) {
        await sleep(5000); // server down or other error: retry later
      }
    }
  }

  // Restarts the wait loop, e.g. when busy changes what it waits for.
  function restartLoop() {
    const gen = ++loopGen;
    if (waiter) { try { waiter.kill(); } catch {} waiter = null; }
    void loop(gen);
  }

  pi.on("session_start", async (_event, ctx) => {
    lastCtx = ctx;
    closed = false;
    busy = false;
    if (!(await ensureServer())) {
      notifyHuman("nqueue: the server is not running and could not be started (`nqueue daemon` needs valkey-server)", "error");
      updateWidget("(offline)");
      return;
    }
    let sessionId: string | null = null;
    try { sessionId = ctx.sessionManager.getSessionId(); } catch {}
    if (id && id !== read(path.join(IDS_DIR, `pi-${sessionId}`))) nq(["unregister"], id); // switched sessions
    id = register(sessionId);
    if (id) process.env.NQUEUE_ID = id; // so the agent's nqueue commands act as this agent
    updateWidget();
    restartLoop();
  });

  // urgent messages only while a run is in progress; the rest when it ends
  pi.on("agent_start", async () => { busy = true; restartLoop(); });
  pi.on("agent_end", async () => { busy = false; restartLoop(); });

  pi.registerCommand("nqueue-check", {
    description: "Deliver any waiting nqueue messages now",
    handler: async () => {
      if (!deliver()) notifyHuman("nqueue: no messages waiting");
    },
  });

  pi.on("session_shutdown", async () => {
    closed = true;
    loopGen++;
    g.__nqueueExtensionLoaded = false; // a reload loads it afresh
    if (waiter) { try { waiter.kill(); } catch {} waiter = null; }
    if (id) nq(["unregister"], id);
    updateWidget("(offline)");
    lastCtx = null;
  });
}
