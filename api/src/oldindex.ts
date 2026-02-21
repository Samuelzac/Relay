// src/index.ts
import { Router } from "itty-router";
import { getClient } from "./db";
import { SeatsDO } from "./seats_do";
import { BroadcastLockDO } from "./broadcast_lock_do";

// If you already added awsIvs.ts in your repo, keep these imports.
// If you haven't added them yet, comment them out for now to confirm / works.
// import { createChannel, createStreamKey } from "./awsIvs";

export { SeatsDO, BroadcastLockDO };

const router = Router();

// ---------- helpers ----------
function json(data: any, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function requireAdmin(request: Request, env: any) {
  // If RELAY_ADMIN_KEY is set, enforce it. If not set, allow (dev only).
  if (!env.RELAY_ADMIN_KEY) return null;
  const key = request.headers.get("x-relay-admin-key");
  if (!key || key !== env.RELAY_ADMIN_KEY) {
    return json({ error: "unauthorized" }, 401);
  }
  return null;
}

async function getEvent(client: any, id: string) {
  const { rows } = await client.query(
    `
    select id, title, tier, viewer_limit, white_label,
           status, starts_at, expires_at,
           ivs_channel_arn, ivs_ingest_endpoint, ivs_playback_url
    from public.events
    where id = $1
  `,
    [id]
  );
  return rows[0];
}

// ---------- routes ----------

// ✅ Root route (this must respond instantly)
router.get("/", () => new Response("Relay API OK - build 2026-02-21-0135", { status: 200 }));
// ✅ Simple health check (helps debugging proxies/caching)
router.get("/healthz", () => {
  console.log("HIT /healthz");
  return new Response("ok", { status: 200 });
});

// Example: existing GET event route (keep yours if you already had it)
router.get("/api/events/:id", async (request: any, env: any) => {
  console.log("HIT /api/events/:id", request.params.id);
  const client = await getClient(env);
  try {
    const ev = await getEvent(client, request.params.id);
    if (!ev) return json({ error: "not_found" }, 404);
    return json(ev);
  } finally {
    await client.end();
  }
});

// ✅ Catch-all (guarantees router.handle() always has a match)
router.all("*", (request: Request) => {
  console.log("HIT fallback", request.method, new URL(request.url).pathname);
  return new Response("Not Found", { status: 404 });
});

// ---------- worker ----------
export default {
  async fetch(request: Request, env: any, ctx: any) {
    try {
      const res = await router.handle(request, env, ctx);
      // Extra guard: never allow undefined to bubble
      return res ?? new Response("Not Found", { status: 404 });
    } catch (err) {
      console.error("UNCAUGHT WORKER ERROR:", err);
      return new Response("Internal Server Error", { status: 500 });
    }
  },

  async scheduled(event: any, env: any, ctx: any) {
    const client = await getClient(env);
    try {
      await client.query(`
        update public.events
        set status = 'expired'
        where expires_at is not null
          and expires_at < now()
          and status != 'expired'
      `);
    } catch (err) {
      console.error("Cron error:", err);
    } finally {
      await client.end();
    }
  },
};