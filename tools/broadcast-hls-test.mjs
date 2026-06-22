#!/usr/bin/env node

import { spawn } from "node:child_process";

const args = new Map();
for (let i = 2; i < process.argv.length; i += 1) {
  const arg = process.argv[i];
  if (!arg.startsWith("--")) continue;
  const key = arg.slice(2);
  const next = process.argv[i + 1];
  if (!next || next.startsWith("--")) {
    args.set(key, "true");
  } else {
    args.set(key, next);
    i += 1;
  }
}

const apiBase = (args.get("api") || process.env.CASTLINK_API_BASE || "https://api.castlink.stream").replace(/\/+$/, "");
const broadcastUrlArg = args.get("broadcast-url") || process.env.CASTLINK_BROADCAST_URL || "";
const email = args.get("email") || process.env.CASTLINK_TEST_EMAIL || "";
const mode = args.get("mode") || process.env.CASTLINK_TEST_MODE || "hls";
const title = args.get("title") || `Castlink HLS broadcast test ${new Date().toISOString()}`;
const durationSeconds = numberArg("duration", 30);
const segments = numberArg("segments", 1);
const gapSeconds = numberArg("gap", 8);
const finishAtEnd = args.get("finish") !== "false";
const ffmpegBin = args.get("ffmpeg") || process.env.FFMPEG_BIN || "ffmpeg";

if (!["hls", "both"].includes(mode)) {
  die("This script pushes RTMPS/HLS only. Use --mode hls or --mode both.");
}

if (!broadcastUrlArg && !email) {
  die("Missing input. Use --broadcast-url <url> for an existing stream, or --email you@example.com to create a test stream.");
}

function numberArg(name, fallback) {
  const raw = Number(args.get(name) || fallback);
  return Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : fallback;
}

function die(message, code = 2) {
  console.error(message);
  process.exit(code);
}

async function request(path, options = {}) {
  const res = await fetch(`${apiBase}${path}`, {
    ...options,
    headers: {
      "content-type": "application/json",
      ...(options.headers || {}),
    },
  });
  const text = await res.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }
  if (!res.ok) {
    const detail = json?.error || json?.message || text || `HTTP ${res.status}`;
    throw new Error(`${options.method || "GET"} ${path} failed: ${detail}`);
  }
  return json;
}

function linkParts(value) {
  const url = new URL(value);
  return {
    eventId: url.searchParams.get("event") || "",
    key: url.searchParams.get("key") || "",
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function run(command, commandArgs, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, commandArgs, {
      stdio: options.stdio || "inherit",
      shell: false,
    });
    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`${command} exited with ${code}`));
    });
  });
}

async function ensureFfmpeg() {
  try {
    await run(ffmpegBin, ["-version"], { stdio: "ignore" });
  } catch {
    die([
      `Cannot run ${ffmpegBin}. Install ffmpeg and make sure it is on PATH.`,
      "Windows winget example:",
      "  winget install Gyan.FFmpeg",
      "Then open a new PowerShell window and rerun this script.",
    ].join("\n"));
  }
}

async function createOrUseBroadcastLink() {
  if (broadcastUrlArg) {
    const parts = linkParts(broadcastUrlArg);
    if (!parts.eventId || !parts.key) throw new Error("Broadcast URL is missing event or key query params.");
    return { ...parts, broadcastUrl: broadcastUrlArg, watchUrl: "" };
  }

  console.log(`Creating ${mode} test stream for ${email}`);
  const created = await request("/api/test-events", {
    method: "POST",
    body: JSON.stringify({ email, title, mode }),
  });
  const parts = linkParts(created.broadcast_url);
  if (!parts.eventId || !parts.key) throw new Error("API returned an incomplete broadcast URL.");
  return { ...parts, broadcastUrl: created.broadcast_url, watchUrl: created.watch_url || "" };
}

function ffmpegArgs(outputUrl) {
  return [
    "-hide_banner",
    "-loglevel",
    "info",
    "-re",
    "-f",
    "lavfi",
    "-i",
    `testsrc2=size=1280x720:rate=30`,
    "-f",
    "lavfi",
    "-i",
    "sine=frequency=1000:sample_rate=48000",
    "-t",
    String(durationSeconds),
    "-c:v",
    "libx264",
    "-preset",
    "veryfast",
    "-tune",
    "zerolatency",
    "-pix_fmt",
    "yuv420p",
    "-b:v",
    "2500k",
    "-maxrate",
    "2500k",
    "-bufsize",
    "5000k",
    "-g",
    "60",
    "-c:a",
    "aac",
    "-b:a",
    "128k",
    "-ar",
    "48000",
    "-f",
    "flv",
    outputUrl,
  ];
}

await ensureFfmpeg();

const stream = await createOrUseBroadcastLink();
console.log(`Event: ${stream.eventId}`);
console.log(`Broadcast: ${stream.broadcastUrl}`);
if (stream.watchUrl) console.log(`Watch: ${stream.watchUrl}`);

try {
  console.log("Provisioning HLS ingest...");
  const provisioned = await request(`/api/events/${encodeURIComponent(stream.eventId)}/hls/provision?key=${encodeURIComponent(stream.key)}`, {
    method: "POST",
    body: "{}",
  });

  const rtmpsUrl = provisioned?.ingest?.rtmpsUrl;
  const streamKey = provisioned?.ingest?.streamKey;
  if (!rtmpsUrl || !streamKey) throw new Error("HLS provision response did not include RTMPS URL and stream key.");

  console.log(`Playback: ${provisioned?.playback?.url || "(pending)"}`);
  console.log("Opening live window...");
  await request(`/api/events/${encodeURIComponent(stream.eventId)}/start?key=${encodeURIComponent(stream.key)}`, {
    method: "POST",
    body: JSON.stringify({ open_window: true }),
  });

  const outputUrl = `${rtmpsUrl}${streamKey}`;
  for (let i = 1; i <= segments; i += 1) {
    console.log(`Broadcasting segment ${i}/${segments} for ${durationSeconds}s...`);
    await run(ffmpegBin, ffmpegArgs(outputUrl));
    if (i < segments) {
      console.log(`Waiting ${gapSeconds}s before next segment...`);
      await sleep(gapSeconds * 1000);
    }
  }

  console.log("Broadcast test completed.");
} finally {
  if (finishAtEnd) {
    try {
      console.log("Finishing event...");
      await request(`/api/events/${encodeURIComponent(stream.eventId)}/finish?key=${encodeURIComponent(stream.key)}`, {
        method: "POST",
        body: "{}",
      });
      console.log("Finish requested.");
    } catch (e) {
      console.error(`Finish failed: ${e.message}`);
    }
  }
}
