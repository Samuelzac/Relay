import { withDb } from "./db";
import { randomSecretUrlSafe, encryptString, decryptString, sha256Hex } from "./crypto";
import { stripeClient, priceForTier, hoursForTier } from "./stripe";
import { createIvsChannel, deleteIvsChannel } from "./ivs";
import { SeatsDO } from "./seats_do";
import { BroadcastLockDO } from "./broadcast_lock_do";

export { SeatsDO, BroadcastLockDO };

type Env = any;

function json(data: any, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }
  });
}

function bad(msg: string, status = 400) {
  return json({ error: msg }, status);
}

function isExpiredRow(row: any) {
  if (!row) return true;
  if (row.expires_at && new Date(row.expires_at).getTime() <= Date.now()) return true;
  if (row.status === "expired") return true;
  return false;
}

function isDisabledRow(row: any): boolean {
  return Boolean(row?.disabled);
}

async function maybeMarkExpired(env: Env, eventId: string) {
  await withDb(env, async (c) => {
    await c.query(
      `update events
       set status='expired'
       where id=$1 and status <> 'expired'
         and (
           (expires_at is not null and expires_at <= now())
           or (starts_at is null and created_at <= now() - interval '24 hours')
         )`,
      [eventId]
    );
  });
}

async function broadcastStillOwner(env: any, req: Request, eventId: string, sid: string) {
  const id = env.BROADCAST_LOCK.idFromName(eventId);
  const stub = env.BROADCAST_LOCK.get(id);

  const doUrl = new URL(req.url);
  doUrl.pathname = "/do/heartbeat";

  return await stub.fetch(doUrl.toString(), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ broadcast_session_id: sid })
  });
}

export default {
  async fetch(req: Request, env: Env, ctx: ExecutionContext) {
    const url = new URL(req.url);

    // CORS for your Pages frontend
    if (req.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: {
          "Access-Control-Allow-Origin": env.APP_ORIGIN,
          "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type,Stripe-Signature",
          "Access-Control-Max-Age": "86400"
        }
      });
    }

    const addCors = (res: Response) => {
      const h = new Headers(res.headers);
      h.set("Access-Control-Allow-Origin", env.APP_ORIGIN);
      h.set("Vary", "Origin");
      return new Response(res.body, { status: res.status, headers: h });
    };

    // ---- ROUTES ----

    // Public watch metadata
    const mEvent = url.pathname.match(/^\/api\/events\/([0-9a-f-]+)$/i);
    if (req.method === "GET" && mEvent) {
      const eventId = mEvent[1];
      await maybeMarkExpired(env, eventId);

      const row = await withDb(env, async (c) => {
        const r = await c.query(
          `select id,title,tier,viewer_limit,white_label,status,starts_at,expires_at,ivs_playback_url
           from events where id=$1`,
          [eventId]
        );
        return r.rows[0];
      });

      if (!row) return addCors(bad("not_found", 404));

      return addCors(
        json({
          id: row.id,
          title: row.title,
          tier: row.tier,
          viewer_limit: row.viewer_limit,
          white_label: row.white_label,
          status: row.status,
          starts_at: row.starts_at,
          expires_at: row.expires_at,
          playback_url: row.ivs_playback_url,
          expired: isExpiredRow(row)
        })
      );
    }

    // Create event + Stripe Checkout
    if (req.method === "POST" && url.pathname === "/api/events") {
      const body = await req.json<any>().catch(() => null);
      if (!body) return addCors(bad("invalid_json"));
      const email = String(body.email ?? "").trim();
      const title = String(body.title ?? "").trim();
      const tier = Number(body.tier);
      const whiteLabel = Boolean(body.white_label);

      if (!email || !email.includes("@")) return addCors(bad("invalid_email"));
      if (!title) return addCors(bad("missing_title"));
      if (![3, 8].includes(tier)) return addCors(bad("invalid_tier"));

      const secretKey = randomSecretUrlSafe(48);
      const successToken = randomSecretUrlSafe(32);
      const successTokenHash = await sha256Hex(successToken);

      const viewerLimit = Number(env.DEFAULT_VIEWER_LIMIT ?? 150);

      const eventRow = await withDb(env, async (c) => {
        const r = await c.query(
          `insert into events (email,title,tier,viewer_limit,white_label,status,secret_key,success_token_hash)
           values ($1,$2,$3,$4,$5,'pending',$6,$7)
           returning id`,
          [email, title, tier, viewerLimit, whiteLabel, secretKey, successTokenHash]
        );
        return { id: r.rows[0].id as string };
      });

      const stripe = stripeClient(env);
      const amount = priceForTier(env, tier);

      const session = await stripe.checkout.sessions.create({
        mode: "payment",
        customer_email: email,
        line_items: [
          {
            price_data: {
              currency: "nzd",
              unit_amount: amount,
              product_data: {
                name: tier === 3 ? "Standard Live Event (3 hours)" : "Extended Live Event (8 hours)",
                description: whiteLabel ? "Includes white-label add-on" : undefined
              }
            },
            quantity: 1
          }
        ],
        metadata: {
          action: "create",
          event_id: eventRow.id,
          tier: String(tier),
          white_label: String(whiteLabel)
        },
        success_url: `${env.APP_ORIGIN}/success/${eventRow.id}?st=${encodeURIComponent(successToken)}`,
        cancel_url: `${env.APP_ORIGIN}/cancel/${eventRow.id}`
      });

      await withDb(env, async (c) => {
        await c.query(`update events set stripe_session_id=$1 where id=$2`, [session.id, eventRow.id]);
      });

      return addCors(
        json({
          event_id: eventRow.id,
          checkout_url: session.url
        })
      );
    }

    // Success links (requires st)
    const mLinks = url.pathname.match(/^\/api\/events\/([0-9a-f-]+)\/links$/i);
    if (req.method === "GET" && mLinks) {
      const eventId = mLinks[1];
      const st = url.searchParams.get("st") ?? "";
      if (!st) return addCors(bad("missing_st"));

      await maybeMarkExpired(env, eventId);

      const row = await withDb(env, async (c) => {
        const r = await c.query(
          `select id,status,expires_at,secret_key,success_token_hash,title
           from events where id=$1`,
          [eventId]
        );
        return r.rows[0];
      });

      if (!row) return addCors(bad("not_found", 404));
      if (isExpiredRow(row) || isDisabledRow(row)) return addCors(bad("expired", 410));
      if (row.status === "pending") return addCors(bad("not_paid", 402));

      const stHash = await sha256Hex(st);
      if (!row.success_token_hash || row.success_token_hash !== stHash) {
        return addCors(bad("forbidden", 403));
      }

      const watchUrl = `${env.APP_ORIGIN}/watch/${eventId}`;
      const broadcastUrl = `${env.APP_ORIGIN}/broadcast/${eventId}?key=${encodeURIComponent(row.secret_key)}`;

      return addCors(json({ watch_url: watchUrl, broadcast_url: broadcastUrl }));
    }

    // Start upgrade checkout (difference)
    const mUp = url.pathname.match(/^\/api\/events\/([0-9a-f-]+)\/upgrade$/i);
    if (req.method === "POST" && mUp) {
      const eventId = mUp[1];
      const body = await req.json<any>().catch(() => null);
      if (!body) return addCors(bad("invalid_json"));

      const st = String(body.st ?? "");
      const toTier = Number(body.to_tier ?? 8);
      if (!st) return addCors(bad("missing_st"));
      if (toTier !== 8) return addCors(bad("invalid_to_tier"));

      await maybeMarkExpired(env, eventId);

      const row = await withDb(env, async (c) => {
        const r = await c.query(
          `select id, tier, status, expires_at, starts_at, success_token_hash
           from events where id=$1`,
          [eventId]
        );
        return r.rows[0];
      });

      if (!row) return addCors(bad("not_found", 404));
      if (isExpiredRow(row) || isDisabledRow(row)) return addCors(bad("expired", 410));
      if (row.status === "pending") return addCors(bad("not_paid", 402));
      if (Number(row.tier) >= 8) return addCors(bad("already_extended", 409));

      const stHash = await sha256Hex(st);
      if (!row.success_token_hash || row.success_token_hash !== stHash) {
        return addCors(bad("forbidden", 403));
      }

      const stripe = stripeClient(env);
      const diff = Number(env.EXTENDED_PRICE_NZD) - Number(env.STANDARD_PRICE_NZD);
      if (diff <= 0) return addCors(bad("invalid_pricing_config", 500));

      const session = await stripe.checkout.sessions.create({
        mode: "payment",
        line_items: [
          {
            price_data: {
              currency: "nzd",
              unit_amount: diff,
              product_data: { name: "Upgrade to Extended Event (8 hours)" }
            },
            quantity: 1
          }
        ],
        metadata: {
          action: "upgrade",
          event_id: eventId,
          to_tier: "8"
        },
        success_url: `${env.APP_ORIGIN}/success/${eventId}?st=${encodeURIComponent(st)}`,
        cancel_url: `${env.APP_ORIGIN}/success/${eventId}?st=${encodeURIComponent(st)}`
      });

      await withDb(env, async (c) => {
        await c.query(
          `update events
           set stripe_upgrade_session_id=$2,
               pending_upgrade_to=$3
           where id=$1`,
          [eventId, session.id, 8]
        );
      });

      return addCors(json({ checkout_url: session.url }));
    }

    // Stripe webhook
    if (req.method === "POST" && url.pathname === "/api/stripe/webhook") {
      const sig = req.headers.get("Stripe-Signature");
      if (!sig) return bad("missing_stripe_signature", 400);

      const raw = await req.arrayBuffer();
      const stripe = stripeClient(env);

      let evt: any;
      try {
        // Stripe library expects Buffer; nodejs_compat provides it
        // @ts-ignore
        evt = stripe.webhooks.constructEvent(Buffer.from(raw), sig, env.STRIPE_WEBHOOK_SECRET);
      } catch {
        return bad(`webhook_signature_failed`, 400);
      }

      if (evt.type === "checkout.session.completed") {
        const session = evt.data.object;
        const action = session.metadata?.action;

        if (action === "create") {
          const eventId = session.metadata?.event_id;
          if (eventId) {
            await withDb(env, async (c) => {
              await c.query(`update events set status='paid' where id=$1 and status='pending'`, [eventId]);
            });

            const ivs = await createIvsChannel(env, eventId);
            const encrypted = await encryptString(ivs.streamKey, env.STREAMKEY_ENC_KEY_B64);

            await withDb(env, async (c) => {
              await c.query(
                `update events
                 set ivs_channel_arn=$2,
                     ivs_ingest_endpoint=$3,
                     ivs_stream_key_encrypted=$4,
                     ivs_playback_url=$5
                 where id=$1`,
                [eventId, ivs.channelArn, ivs.ingestEndpoint, encrypted, ivs.playbackUrl]
              );
            });
          }
        }

        if (action === "upgrade") {
          const eventId = session.metadata?.event_id;
          const toTier = Number(session.metadata?.to_tier ?? 8);

          if (eventId && toTier === 8) {
            const grace = Number(env.GRACE_MINUTES ?? 30);

            await withDb(env, async (c) => {
              const r = await c.query(
                `select id, tier, starts_at
                 from events
                 where id=$1 and stripe_upgrade_session_id=$2 and pending_upgrade_to=8`,
                [eventId, session.id]
              );
              const row = r.rows[0];
              if (!row) return;

              if (row.starts_at) {
                await c.query(
                  `update events
                   set tier=8,
                       pending_upgrade_to=null,
                       stripe_upgrade_session_id=null,
                       expires_at = starts_at + interval '8 hours' + ($2::text || ' minutes')::interval
                   where id=$1`,
                  [eventId, grace]
                );
              } else {
                await c.query(
                  `update events
                   set tier=8,
                       pending_upgrade_to=null,
                       stripe_upgrade_session_id=null
                   where id=$1`,
                  [eventId]
                );
              }
            });
          }
        }
      }

      return json({ ok: true });
    }

    // Broadcaster lock claim
    if (req.method === "POST" && url.pathname === "/api/broadcast/claim") {
      const body = await req.json<any>().catch(() => null);
      if (!body) return addCors(bad("invalid_json"));

      const eventId = String(body.eventId ?? "");
      const key = String(body.key ?? "");
      if (!eventId || !key) return addCors(bad("missing_params"));

      await maybeMarkExpired(env, eventId);

      const row = await withDb(env, async (c) => {
        const r = await c.query(
          `select id,status,secret_key,expires_at from events where id=$1`,
          [eventId]
        );
        return r.rows[0];
      });

      if (!row) return addCors(bad("not_found", 404));
      if (row.secret_key !== key) return addCors(bad("forbidden", 403));
      if (row.status === "pending") return addCors(bad("not_paid", 402));
      if (isExpiredRow(row) || isDisabledRow(row)) return addCors(bad("expired", 410));

      const id = env.BROADCAST_LOCK.idFromName(eventId);
      const stub = env.BROADCAST_LOCK.get(id);

      const doRes = await stub.fetch(new URL("/do/claim", req.url).toString(), { method: "POST" });
      return addCors(doRes);
    }

    // Broadcaster lock heartbeat
    if (req.method === "POST" && url.pathname === "/api/broadcast/heartbeat") {
      const body = await req.json<any>().catch(() => null);
      if (!body) return addCors(bad("invalid_json"));

      const eventId = String(body.eventId ?? "");
      const key = String(body.key ?? "");
      const sid = String(body.broadcast_session_id ?? "");
      if (!eventId || !key || !sid) return addCors(bad("missing_params"));

      await maybeMarkExpired(env, eventId);

      const row = await withDb(env, async (c) => {
        const r = await c.query(`select id,status,secret_key,expires_at from events where id=$1`, [eventId]);
        return r.rows[0];
      });

      if (!row) return addCors(bad("not_found", 404));
      if (row.secret_key !== key) return addCors(bad("forbidden", 403));
      if (row.status === "pending") return addCors(bad("not_paid", 402));
      if (isExpiredRow(row) || isDisabledRow(row)) return addCors(bad("expired", 410));

      const doRes = await broadcastStillOwner(env, req, eventId, sid);
      return addCors(doRes);
    }

    // Broadcaster lock release
    if (req.method === "POST" && url.pathname === "/api/broadcast/release") {
      const body = await req.json<any>().catch(() => null);
      if (!body) return addCors(bad("invalid_json"));

      const eventId = String(body.eventId ?? "");
      const key = String(body.key ?? "");
      const sid = String(body.broadcast_session_id ?? "");
      if (!eventId || !key || !sid) return addCors(bad("missing_params"));

      await maybeMarkExpired(env, eventId);

      const row = await withDb(env, async (c) => {
        const r = await c.query(`select id,status,secret_key,expires_at from events where id=$1`, [eventId]);
        return r.rows[0];
      });

      if (!row) return addCors(bad("not_found", 404));
      if (row.secret_key !== key) return addCors(bad("forbidden", 403));
      if (row.status === "pending") return addCors(bad("not_paid", 402));
      if (isExpiredRow(row) || isDisabledRow(row)) return addCors(bad("expired", 410));

      const id = env.BROADCAST_LOCK.idFromName(eventId);
      const stub = env.BROADCAST_LOCK.get(id);

      const doUrl = new URL(req.url);
      doUrl.pathname = "/do/release";

      const doRes = await stub.fetch(doUrl.toString(), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ broadcast_session_id: sid })
      });

      return addCors(doRes);
    }

    // Private broadcast config (requires sid ownership)
    if (req.method === "GET" && url.pathname === "/api/broadcast/config") {
      const eventId = url.searchParams.get("eventId") ?? "";
      const key = url.searchParams.get("key") ?? "";
      const sid = url.searchParams.get("sid") ?? "";
      if (!eventId || !key) return addCors(bad("missing_params"));
      if (!sid) return addCors(bad("missing_sid", 400));

      await maybeMarkExpired(env, eventId);

      const row = await withDb(env, async (c) => {
        const r = await c.query(
          `select id,status,secret_key,starts_at,expires_at,ivs_ingest_endpoint,ivs_stream_key_encrypted,ivs_playback_url
           from events where id=$1`,
          [eventId]
        );
        return r.rows[0];
      });

      if (!row) return addCors(bad("not_found", 404));
      if (row.secret_key !== key) return addCors(bad("forbidden", 403));
      if (row.status !== "paid" && row.status !== "live") return addCors(bad("not_paid", 402));
      if (isExpiredRow(row) || isDisabledRow(row)) return addCors(bad("expired", 410));
      if (!row.ivs_stream_key_encrypted) return addCors(bad("ivs_not_ready", 503));

      // verify lock ownership
      const lockRes = await broadcastStillOwner(env, req, eventId, sid);
      if (!lockRes.ok) return addCors(lockRes);
      const lockJson = await lockRes.json<any>();
      if (!lockJson.still_owner) return addCors(bad("not_owner", 409));

      const streamKey = await decryptString(row.ivs_stream_key_encrypted, env.STREAMKEY_ENC_KEY_B64);

      return addCors(
        json({
          ingestEndpoint: row.ivs_ingest_endpoint,
          streamKey,
          playbackUrl: row.ivs_playback_url
        })
      );
    }

    // Broadcast started (sets starts_at/expires_at on first start)
    if (req.method === "POST" && url.pathname === "/api/broadcast/started") {
      const body = await req.json<any>().catch(() => null);
      if (!body) return addCors(bad("invalid_json"));

      const eventId = String(body.eventId ?? "");
      const key = String(body.key ?? "");
      if (!eventId || !key) return addCors(bad("missing_params"));

      const row = await withDb(env, async (c) => {
        const r = await c.query(`select id,tier,status,secret_key,starts_at,expires_at from events where id=$1`, [eventId]);
        return r.rows[0];
      });
      if (!row) return addCors(bad("not_found", 404));
      if (row.secret_key !== key) return addCors(bad("forbidden", 403));
      if (row.status !== "paid" && row.status !== "live") return addCors(bad("not_paid", 402));
      if (isExpiredRow(row) || isDisabledRow(row)) return addCors(bad("expired", 410));

      if (!row.starts_at) {
        const hours = hoursForTier(env, Number(row.tier));
        const grace = Number(env.GRACE_MINUTES ?? 30);

        await withDb(env, async (c) => {
          await c.query(
            `update events
             set status='live',
                 starts_at=now(),
                 expires_at=now() + ($2::text || ' hours')::interval + ($3::text || ' minutes')::interval
             where id=$1 and starts_at is null`,
            [eventId, hours, grace]
          );
        });
      } else {
        await withDb(env, async (c) => {
          await c.query(`update events set status='live' where id=$1 and status='paid'`, [eventId]);
        });
      }

      return addCors(json({ ok: true }));
    }

    // Seat endpoints -> Durable Object
    const mSeat = url.pathname.match(/^\/api\/events\/([0-9a-f-]+)\/(view-session|heartbeat|leave)$/i);
    if (req.method === "POST" && mSeat) {
      const eventId = mSeat[1];
      const action = mSeat[2];

      await maybeMarkExpired(env, eventId);

      const row = await withDb(env, async (c) => {
        const r = await c.query(`select id,status,viewer_limit,expires_at,starts_at,created_at from events where id=$1`, [eventId]);
        return r.rows[0];
      });
      if (!row) return addCors(bad("not_found", 404));
      if (isExpiredRow(row) || isDisabledRow(row)) return addCors(bad("expired", 410));
      if (row.status === "pending") return addCors(bad("not_paid", 402));

      const id = env.SEATS.idFromName(eventId);
      const stub = env.SEATS.get(id);
      const doUrl = new URL(req.url);
      doUrl.pathname = `/do/${action}`;
      doUrl.searchParams.set("limit", String(row.viewer_limit));

      const bodyText = await req.text();
      const doRes = await stub.fetch(doUrl.toString(), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: bodyText && bodyText.length ? bodyText : "{}"
      });

      return addCors(doRes);
    }

    // Seat stats
    const mSeatStats = url.pathname.match(/^\/api\/events\/([0-9a-f-]+)\/seat-stats$/i);
    if (req.method === "GET" && mSeatStats) {
      const eventId = mSeatStats[1];

      await maybeMarkExpired(env, eventId);

      const row = await withDb(env, async (c) => {
        const r = await c.query(`select id,status,viewer_limit,expires_at from events where id=$1`, [eventId]);
        return r.rows[0];
      });

      if (!row) return addCors(bad("not_found", 404));
      if (isExpiredRow(row) || isDisabledRow(row)) return addCors(bad("expired", 410));
      if (row.status === "pending") return addCors(bad("not_paid", 402));

      const id = env.SEATS.idFromName(eventId);
      const stub = env.SEATS.get(id);

      const doUrl = new URL(req.url);
      doUrl.pathname = "/do/stats";
      doUrl.searchParams.set("limit", String(row.viewer_limit));

      const doRes = await stub.fetch(doUrl.toString(), { method: "GET" });
      return addCors(doRes);
    }


    // ---- TEST STREAMS (2-minute) ----

    // Start a test stream (requires st)
    const mTestStart = url.pathname.match(/^\/api\/events\/([0-9a-f-]+)\/test\/start$/i);
    if (req.method === "POST" && mTestStart) {
      const eventId = mTestStart[1];
      const body = await req.json<any>().catch(() => null);
      if (!body) return addCors(bad("invalid_json"));

      const st = String(body.st ?? "");
      if (!st) return addCors(bad("missing_st"));

      await maybeMarkExpired(env, eventId);

      const eventRow = await withDb(env, async (c) => {
        const r = await c.query(
          `select id,status,expires_at,disabled,success_token_hash
           from events where id=$1`,
          [eventId]
        );
        return r.rows[0];
      });

      if (!eventRow) return addCors(bad("not_found", 404));
      if (isExpiredRow(eventRow) || isDisabledRow(eventRow)) return addCors(bad("expired", 410));
      if (eventRow.status === "pending") return addCors(bad("not_paid", 402));

      const stHash = await sha256Hex(st);
      if (eventRow.success_token_hash !== stHash) return addCors(bad("forbidden", 403));

      // Only one active test at a time per event (prevents spam)
      const activeTest = await withDb(env, async (c) => {
        const r = await c.query(
          `select id, secret_key
           from test_streams
           where event_id=$1 and status='active' and expires_at > now()
           order by created_at desc
           limit 1`,
          [eventId]
        );
        return r.rows[0];
      });

      if (activeTest) {
        const watchUrl = `${env.APP_ORIGIN}/test/watch/${activeTest.id}`;
        const broadcastUrl = `${env.APP_ORIGIN}/test/broadcast/${activeTest.id}?key=${encodeURIComponent(activeTest.secret_key)}`;
        return addCors(json({
          ok: true,
          reused: true,
          test_id: activeTest.id,
          watch_url: watchUrl,
          broadcast_url: broadcastUrl,
          expires_in_seconds: 120
        }));
      }

      const testSecret = randomSecretUrlSafe(40);

      const ivs = await createIvsChannel(env, `test-${eventId}-${crypto.randomUUID().slice(0, 8)}`);
      const encrypted = await encryptString(ivs.streamKey, env.STREAMKEY_ENC_KEY_B64);

      const testRow = await withDb(env, async (c) => {
        const r = await c.query(
          `insert into test_streams
            (event_id,status,expires_at,ivs_channel_arn,ivs_ingest_endpoint,ivs_stream_key_encrypted,ivs_playback_url,secret_key)
           values ($1,'active', now() + interval '2 minutes', $2,$3,$4,$5,$6)
           returning id`,
          [eventId, ivs.channelArn, ivs.ingestEndpoint, encrypted, ivs.playbackUrl, testSecret]
        );
        return r.rows[0];
      });

      const watchUrl = `${env.APP_ORIGIN}/test/watch/${testRow.id}`;
      const broadcastUrl = `${env.APP_ORIGIN}/test/broadcast/${testRow.id}?key=${encodeURIComponent(testSecret)}`;

      return addCors(json({
        ok: true,
        test_id: testRow.id,
        watch_url: watchUrl,
        broadcast_url: broadcastUrl,
        expires_in_seconds: 120
      }));
    }

    // Test metadata (watch page)
    const mTestMeta = url.pathname.match(/^\/api\/test\/([0-9a-f-]+)$/i);
    if (req.method === "GET" && mTestMeta) {
      const testId = mTestMeta[1];

      const row = await withDb(env, async (c) => {
        const r = await c.query(
          `select id,status,expires_at,ivs_playback_url
           from test_streams where id=$1`,
          [testId]
        );
        return r.rows[0];
      });

      if (!row) return addCors(bad("not_found", 404));

      const expired = new Date(row.expires_at).getTime() <= Date.now() || row.status === "expired";

      return addCors(json({
        id: row.id,
        status: row.status,
        expires_at: row.expires_at,
        playback_url: row.ivs_playback_url,
        expired
      }));
    }

    // Test broadcast config (requires key)
    const mTestCfg = url.pathname.match(/^\/api\/test\/([0-9a-f-]+)\/config$/i);
    if (req.method === "GET" && mTestCfg) {
      const testId = mTestCfg[1];
      const key = url.searchParams.get("key") ?? "";
      if (!key) return addCors(bad("missing_key"));

      const row = await withDb(env, async (c) => {
        const r = await c.query(
          `select id,status,expires_at,secret_key,ivs_ingest_endpoint,ivs_stream_key_encrypted,ivs_playback_url
           from test_streams where id=$1`,
          [testId]
        );
        return r.rows[0];
      });

      if (!row) return addCors(bad("not_found", 404));

      const expired = new Date(row.expires_at).getTime() <= Date.now() || row.status === "expired";
      if (expired) return addCors(bad("expired", 410));
      if (row.secret_key !== key) return addCors(bad("forbidden", 403));

      const streamKey = await decryptString(row.ivs_stream_key_encrypted, env.STREAMKEY_ENC_KEY_B64);

      return addCors(json({
        ingestEndpoint: row.ivs_ingest_endpoint,
        streamKey,
        playbackUrl: row.ivs_playback_url,
        expires_at: row.expires_at
      }));
    }


    // ---- REPORTS (viewer abuse/copyright) ----

    // Submit a report (public)
    if (req.method === "POST" && url.pathname === "/api/report") {
      const body = await req.json<any>().catch(() => null);
      if (!body) return addCors(bad("invalid_json"));

      const eventId = String(body.event_id ?? "");
      const reason = String(body.reason ?? "").slice(0, 80);
      const description = String(body.description ?? "").slice(0, 2000);
      const page = String(body.page ?? "watch").slice(0, 40);

      if (!reason) return addCors(bad("missing_reason"));
      // event_id optional (allows reporting even if event lookup fails), but validate UUID-ish if provided
      if (eventId && !/^[0-9a-f-]{10,}$/i.test(eventId)) return addCors(bad("bad_event_id"));

      const ip = req.headers.get("CF-Connecting-IP") || req.headers.get("X-Forwarded-For") || "";
      const ua = req.headers.get("User-Agent") || "";

      await withDb(env, async (c) => {
        await c.query(
          `insert into reports (event_id, page, reason, description, ip_address, user_agent)
           values ($1,$2,$3,$4,$5,$6)`,
          [eventId || null, page || "watch", reason, description || null, ip || null, ua || null]
        );
      });

      return addCors(json({ ok: true }));
    }

    // Admin: list recent reports (requires ADMIN_KEY)
    const mAdminReports = url.pathname === "/api/admin/reports";
    if (req.method === "GET" && mAdminReports) {
      const key = url.searchParams.get("key") ?? "";
      if (!env.ADMIN_KEY || key !== env.ADMIN_KEY) return addCors(bad("forbidden", 403));

      const rows = await withDb(env, async (c) => {
        const r = await c.query(
          `select r.id, r.event_id, r.page, r.reason, r.description, r.ip_address, r.created_at,
                  e.title as event_title, e.status as event_status, e.disabled as event_disabled
           from reports r
           left join events e on e.id = r.event_id
           order by r.created_at desc
           limit 200`
        );
        return r.rows;
      });

      return addCors(json({ ok: true, reports: rows }));
    }

    // Admin: disable an event (requires ADMIN_KEY)
    const mAdminDisable = url.pathname.match(/^\/api\/admin\/events\/([0-9a-f-]+)\/disable$/i);
    if (req.method === "POST" && mAdminDisable) {
      const eventId = mAdminDisable[1];
      const key = url.searchParams.get("key") ?? "";
      if (!env.ADMIN_KEY || key !== env.ADMIN_KEY) return addCors(bad("forbidden", 403));

      await withDb(env, async (c) => {
        await c.query(`update events set disabled=true where id=$1`, [eventId]);
      });

      return addCors(json({ ok: true }));
    }

    return addCors(bad("not_found", 404));
  },

  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    const expired = await withDb(env, async (c) => {
      const r = await c.query(
        `update events
         set status='expired'
         where status <> 'expired'
           and (
             (expires_at is not null and expires_at <= now())
             or (starts_at is null and created_at <= now() - interval '24 hours')
           )
         returning id, ivs_channel_arn`
      );
      return r.rows as Array<{ id: string; ivs_channel_arn: string | null }>;
    });


    // expire tests
    const expiredTests = await withDb(env, async (c) => {
      const r = await c.query(
        `update test_streams
         set status='expired'
         where status <> 'expired' and expires_at <= now()
         returning id, ivs_channel_arn`
      );
      return r.rows as Array<{ id: string; ivs_channel_arn: string | null }>;
    });

    if (String(env.DELETE_IVS_ON_EXPIRE) === "true" && expiredTests.length) {
      for (const t of expiredTests) {
        if (!t.ivs_channel_arn) continue;
        ctx.waitUntil(deleteIvsChannel(env, t.ivs_channel_arn).catch(() => {}));
      }
    }

    if (String(env.DELETE_IVS_ON_EXPIRE) === "true" && expired.length) {
      for (const e of expired) {
        if (!e.ivs_channel_arn) continue;
        ctx.waitUntil(
          (async () => {
            try {
              await deleteIvsChannel(env, e.ivs_channel_arn!);
            } catch {
              // ignore
            }
          })()
        );
      }
    }
  }
};
