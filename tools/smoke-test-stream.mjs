#!/usr/bin/env node

const args = new Map();
for (let i = 2; i < process.argv.length; i += 1) {
  const arg = process.argv[i];
  if (arg.startsWith("--")) {
    const key = arg.slice(2);
    const next = process.argv[i + 1];
    if (!next || next.startsWith("--")) {
      args.set(key, "true");
    } else {
      args.set(key, next);
      i += 1;
    }
  }
}

const apiBase = (args.get("api") || process.env.CASTLINK_API_BASE || "https://api.castlink.stream").replace(/\/+$/, "");
const email = args.get("email") || process.env.CASTLINK_TEST_EMAIL;
const mode = args.get("mode") || process.env.CASTLINK_TEST_MODE || "rtc";
const title = args.get("title") || `Castlink smoke test ${new Date().toISOString()}`;
const finishAtEnd = args.get("finish") !== "false";
const timeoutMs = Number(args.get("timeout-ms") || 180000);
const pollMs = Number(args.get("poll-ms") || 5000);

if (!email) {
  console.error("Missing email. Use --email you@example.com or CASTLINK_TEST_EMAIL.");
  process.exit(2);
}

if (!["rtc", "hls", "both"].includes(mode)) {
  console.error("Invalid mode. Use --mode rtc, --mode hls, or --mode both.");
  process.exit(2);
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

function readinessSummary(readiness) {
  if (!readiness) return "no readiness";
  return [
    `state=${readiness.state}`,
    `detail=${readiness.detail}`,
    `stage=${!!readiness.stage_exists}`,
    `hls=${!!readiness.hls_channel_exists}`,
    `composition=${!!readiness.composition_started}`,
    `playback=${!!readiness.playback_url_exists}`,
    `can_go_live=${!!readiness.can_go_live}`,
    `can_watch_rtc=${!!readiness.can_watch_rtc}`,
    `can_watch_hls=${!!readiness.can_watch_hls}`,
  ].join(" ");
}

console.log(`API: ${apiBase}`);
console.log(`Creating ${mode} test stream for ${email}`);

const created = await request("/api/test-events", {
  method: "POST",
  body: JSON.stringify({ email, title, mode }),
});

const broadcast = linkParts(created.broadcast_url);
const watch = linkParts(created.watch_url);
if (!broadcast.eventId || !broadcast.key || !watch.key) {
  throw new Error("API returned incomplete broadcast/watch links.");
}

console.log(`Event: ${broadcast.eventId}`);
console.log(`Broadcast: ${created.broadcast_url}`);
console.log(`Watch: ${created.watch_url}`);

try {
  if (mode === "hls") {
    console.log("Provisioning HLS infrastructure...");
    const hls = await request(`/api/events/${encodeURIComponent(broadcast.eventId)}/hls/provision?key=${encodeURIComponent(broadcast.key)}`, {
      method: "POST",
      body: "{}",
    });
    console.log(`HLS playback URL: ${hls.playback?.url || "(pending)"}`);
  } else {
    console.log("Provisioning RTC/WHIP infrastructure...");
    const rtc = await request(`/api/events/${encodeURIComponent(broadcast.eventId)}/rtc/provision?key=${encodeURIComponent(broadcast.key)}`, {
      method: "POST",
      body: "{}",
    });
    console.log(`RTC stage: ${rtc.stageArn || "(missing)"}`);
    if (mode === "both") {
      const whip = await request(`/api/events/${encodeURIComponent(broadcast.eventId)}/whip/provision?key=${encodeURIComponent(broadcast.key)}`, {
        method: "POST",
        body: "{}",
      });
      console.log(`WHIP URL: ${whip.whip_url || "(missing)"}`);
    }
  }

  console.log("Opening test stream window and starting server-side stream path...");
  await request(`/api/events/${encodeURIComponent(broadcast.eventId)}/start?key=${encodeURIComponent(broadcast.key)}`, {
    method: "POST",
    body: JSON.stringify({ open_window: true }),
  });

  const deadline = Date.now() + timeoutMs;
  let last = null;
  while (Date.now() < deadline) {
    last = await request(`/api/events/${encodeURIComponent(broadcast.eventId)}/readiness?key=${encodeURIComponent(broadcast.key)}`);
    console.log(readinessSummary(last.readiness));

    const r = last.readiness || {};
    const broadcasterReady = !!r.can_go_live || r.state === "live_window_open";
    const watchReady = mode === "rtc" ? !!r.can_watch_rtc : mode === "hls" ? !!r.can_watch_hls : (!!r.can_watch_rtc || !!r.can_watch_hls);
    if (broadcasterReady && watchReady) {
      console.log("PASS: stream resources are provisioned and access checks are ready.");
      process.exitCode = 0;
      break;
    }
    await sleep(pollMs);
  }

  if (process.exitCode !== 0) {
    console.error(`FAIL: stream did not become ready within ${timeoutMs}ms.`);
    if (last?.readiness) console.error(readinessSummary(last.readiness));
    process.exitCode = 1;
  }
} finally {
  if (finishAtEnd) {
    try {
      console.log("Finishing test event for cleanup...");
      await request(`/api/events/${encodeURIComponent(broadcast.eventId)}/finish?key=${encodeURIComponent(broadcast.key)}`, {
        method: "POST",
        body: "{}",
      });
      console.log("Cleanup requested.");
    } catch (e) {
      console.error(`Cleanup request failed: ${e.message}`);
    }
  }
}
