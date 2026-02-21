// src/index.ts
import { getClient } from "./db";
import { randomSecretUrlSafe, sha256Hex, encryptString, decryptString } from "./crypto";
import { SeatsDO } from "./seats_do";
import { BroadcastLockDO } from "./broadcast_lock_do";
import { stripeClient, priceForTierAndMode, hoursForTier, StreamMode } from "./stripe";
import { createChannel, createStreamKey } from "./awsIvs";
import { deleteIvsChannel } from "./ivs";
import { createStage, createParticipantToken, deleteStage } from "./awsIvsRealtime";

export { SeatsDO, BroadcastLockDO };

type Env = any;

const JSON_HEADERS = {
  "content-type": "application/json; charset=utf-8",
  "cache-control": "no-store",
};

function corsHeaders(env: Env) {
  const origin = env.APP_ORIGIN || "*";
  return {
    "access-control-allow-origin": origin,
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "access-control-allow-headers": "content-type,x-relay-admin-key",
    "access-control-allow-credentials": "true",
    vary: "Origin",
  };
}

function json(env: Env, data: any, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { ...JSON_HEADERS, ...corsHeaders(env) },
  });
}

function text(env: Env, body: string, status = 200) {
  return new Response(body, {
    status,
    headers: {
      "content-type": "text/plain; charset=utf-8",
      "cache-control": "no-store",
      ...corsHeaders(env),
    },
  });
}

async function readJson(req: Request) {
  return await req.json<any>().catch(() => ({}));
}

function match(pathname: string, re: RegExp) {
  const m = pathname.match(re);
  return m?.slice(1) ?? null;
}

function pagesBase(env: Env) {
  return env.APP_ORIGIN || "https://relay-cp1.pages.dev";
}

function watchUrl(env: Env, eventId: string, secretKey: string) {
  return `${pagesBase(env)}/watch/${eventId}?key=${encodeURIComponent(secretKey)}`;
}

function broadcastUrl(env: Env, eventId: string, broadcastKey: string) {
  return `${pagesBase(env)}/broadcast/${eventId}?key=${encodeURIComponent(broadcastKey)}`;
}

function requireExact(key: string | null, expected: string, env: Env) {
  if (!key || key !== expected) return json(env, { error: "unauthorized" }, 401);
  return null;
}

function toMode(v: any): StreamMode {
  if (v === "webrtc" || v === "hls" || v === "both") return v;
  // backwards compat: infer from flags
  return "webrtc";
}

function ingestHostFromDb(value: string) {
  if (!value) return value;
  if (value.startsWith("rtmp")) {
    try {
      const u = new URL(value);
      return u.hostname;
    } catch {
      return value.replace(/^rtmps?:\/\//, "").split(":")[0].split("/")[0];
    }
  }
  return value;
}

function rtmpsUrlFromHost(host: string) {
  return `rtmps://${host}:443/app/`;
}

async function getEvent(client: any, id: string) {
  const { rows } = await client.query(
    `
    select
      id, email, title, tier, viewer_limit, white_label,
      status, starts_at, expires_at, created_at,
      ivs_channel_arn, ivs_ingest_endpoint, ivs_playback_url, ivs_stream_key_encrypted,
      secret_key, broadcast_key, success_token_hash, stripe_session_id,
      rtc_stage_arn, rtc_stage_endpoints, rtc_enabled, hls_enabled
    from public.events
    where id = $1
  `,
    [id]
  );
  return rows[0] || null;
}

async function markPaid(client: any, id: string) {
  await client.query(`update public.events set status='paid' where id=$1 and status!='expired'`, [id]);
}

async function ensureBroadcastKey(client: any, eventId: string) {
  const { rows } = await client.query(`select broadcast_key from public.events where id=$1`, [eventId]);
  if (rows[0]?.broadcast_key) return rows[0].broadcast_key as string;
  const bk = randomSecretUrlSafe(24);
  await client.query(`update public.events set broadcast_key=$1 where id=$2`, [bk, eventId]);
  return bk;
}

async function updateIvs(client: any, id: string, channelArn: string, ingestHost: string, playbackUrl: string, streamKeyEnc: string) {
  await client.query(
    `
    update public.events
    set ivs_channel_arn=$1,
        ivs_ingest_endpoint=$2,
        ivs_playback_url=$3,
        ivs_stream_key_encrypted=$4,
        starts_at=coalesce(starts_at, now())
    where id=$5
  `,
    [channelArn, ingestHost, playbackUrl, streamKeyEnc, id]
  );
}

async function updateRtc(client: any, id: string, stageArn: string, endpoints: any) {
  await client.query(
    `
    update public.events
    set rtc_stage_arn=$1,
        rtc_stage_endpoints=$2,
        starts_at=coalesce(starts_at, now())
    where id=$3
  `,
    [stageArn, endpoints ? JSON.stringify(endpoints) : null, id]
  );
}

export default {
  async fetch(request: Request, env: Env) {
    const url = new URL(request.url);
    const { pathname } = url;
    const method = request.method.toUpperCase();

    if (method === "OPTIONS") return new Response("", { status: 204, headers: corsHeaders(env) });

    try {
      // Root + health
      if (method === "GET" && pathname === "/") return text(env, "Relay API OK", 200);
      if (method === "GET" && pathname === "/healthz") return text(env, "ok", 200);

      // Pricing config (for Pages UI)
      if (method === "GET" && pathname === "/api/pricing") {
        return json(env, {
          ok: true,
          tiers: {
            "3": { hours: Number(env.STANDARD_HOURS), base_nzd: Number(env.STANDARD_PRICE_NZD) },
            "8": { hours: Number(env.EXTENDED_HOURS), base_nzd: Number(env.EXTENDED_PRICE_NZD) },
          },
          addons: {
            hls_nzd: Number(env.HLS_ADDON_NZD ?? 0),
            webrtc_nzd: Number(env.WEBRTC_ADDON_NZD ?? 0),
            both_nzd: Number(env.BOTH_ADDON_NZD ?? 0),
          },
        });
      }

      // Create event -> Stripe Checkout
      if (method === "POST" && pathname === "/api/events") {
        const body = await readJson(request);

        const email = String(body.email ?? "").trim();
        const title = String(body.title ?? "").trim();
        const tier = Number(body.tier);
        const whiteLabel = !!body.white_label;
        const viewerLimit = Number(body.viewer_limit ?? env.DEFAULT_VIEWER_LIMIT ?? 150);

        const mode = toMode(body.mode);
        const rtcEnabled = mode === "webrtc" || mode === "both";
        const hlsEnabled = mode === "hls" || mode === "both";

        if (!email || !title) return json(env, { error: "email_and_title_required" }, 400);
        if (![3, 8].includes(tier)) return json(env, { error: "invalid_tier" }, 400);

        const secretKey = randomSecretUrlSafe(24);
        const broadcastKey = randomSecretUrlSafe(24);
        const successToken = randomSecretUrlSafe(24);
        const successHash = await sha256Hex(successToken);

        const amountNzd = priceForTierAndMode(env, tier, mode);

        const client = await getClient(env);
        try {
          const { rows } = await client.query(
            `
            insert into public.events
              (email, title, tier, viewer_limit, white_label, status,
               secret_key, broadcast_key, success_token_hash,
               rtc_enabled, hls_enabled)
            values
              ($1,$2,$3,$4,$5,'pending',
               $6,$7,$8,
               $9,$10)
            returning id
          `,
            [email, title, tier, viewerLimit, whiteLabel, secretKey, broadcastKey, successHash, rtcEnabled, hlsEnabled]
          );

          const eventId = rows[0].id as string;

          const stripe = stripeClient(env);
          const successUrl = `${pagesBase(env)}/success/${eventId}?st=${encodeURIComponent(successToken)}`;
          const cancelUrl = `${pagesBase(env)}/create?canceled=1`;

          const session = await stripe.checkout.sessions.create({
            mode: "payment",
            customer_email: email,
            line_items: [
              {
                price_data: {
                  currency: "nzd",
                  unit_amount: amountNzd,
                  product_data: { name: `Relay Live Event (${tier}h • ${mode.toUpperCase()})` },
                },
                quantity: 1,
              },
            ],
            metadata: { relay_event_id: eventId },
            success_url: successUrl,
            cancel_url: cancelUrl,
          });

          await client.query(`update public.events set stripe_session_id=$1 where id=$2`, [session.id, eventId]);

          return json(env, { ok: true, event_id: eventId, checkout_url: session.url }, 200);
        } finally {
          await client.end();
        }
      }

      // Stripe webhook -> mark paid
      if (method === "POST" && pathname === "/api/stripe/webhook") {
        const sig = request.headers.get("stripe-signature");
        if (!sig) return json(env, { error: "missing_signature" }, 400);
        if (!env.STRIPE_WEBHOOK_SECRET) return json(env, { error: "missing_STRIPE_WEBHOOK_SECRET" }, 500);

        const stripe = stripeClient(env);
        const raw = await request.arrayBuffer();
        let evt: any;

        try {
          evt = stripe.webhooks.constructEvent(raw, sig, env.STRIPE_WEBHOOK_SECRET);
        } catch (e: any) {
          return json(env, { error: "invalid_signature", message: e?.message }, 400);
        }

        if (evt.type === "checkout.session.completed") {
          const session = evt.data.object;
          const eventId = session?.metadata?.relay_event_id;
          if (eventId) {
            const client = await getClient(env);
            try {
              await markPaid(client, eventId);
            } finally {
              await client.end();
            }
          }
        }

        return json(env, { ok: true }, 200);
      }

      // Success -> return watch + broadcast links
      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/links$/);
        if (method === "GET" && m) {
          const eventId = m[0];
          const st = url.searchParams.get("st");

          const client = await getClient(env);
          try {
            const ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);

            const stHash = st ? await sha256Hex(st) : "";
            if (!st || stHash !== ev.success_token_hash) return json(env, { error: "unauthorized" }, 401);

            // If webhook lagged, try verify payment status via Stripe
            if (ev.status === "pending" && ev.stripe_session_id) {
              try {
                const stripe = stripeClient(env);
                const s = await stripe.checkout.sessions.retrieve(ev.stripe_session_id);
                if (s?.payment_status === "paid") await markPaid(client, eventId);
              } catch {}
            }

            const bk = ev.broadcast_key || (await ensureBroadcastKey(client, eventId));

            return json(env, {
              ok: true,
              event_id: eventId,
              watch_url: watchUrl(env, eventId, ev.secret_key),
              broadcast_url: broadcastUrl(env, eventId, bk),
              rtc_enabled: !!ev.rtc_enabled,
              hls_enabled: !!ev.hls_enabled,
            });
          } finally {
            await client.end();
          }
        }
      }

      // Public event info (watch page uses this)
      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/public$/);
        if (method === "GET" && m) {
          const eventId = m[0];
          const key = url.searchParams.get("key");

          const client = await getClient(env);
          try {
            const ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);

            const authRes = requireExact(key, ev.secret_key, env);
            if (authRes) return authRes;

            const expired = ev.status === "expired" || (ev.expires_at && new Date(ev.expires_at).getTime() < Date.now());

            return json(env, {
              ok: true,
              id: ev.id,
              title: ev.title,
              status: ev.status,
              expired,
              playback_url: ev.ivs_playback_url || null,
              rtc_stage_arn: ev.rtc_stage_arn || null,
              rtc_enabled: !!ev.rtc_enabled,
              hls_enabled: !!ev.hls_enabled,
              white_label: !!ev.white_label,
            });
          } finally {
            await client.end();
          }
        }
      }

      // SeatsDO proxy (optional viewer limit)
      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/(view-session|heartbeat|leave|stats)$/);
        if (m) {
          const eventId = m[0];
          const action = m[1];

          const id = env.SEATS.idFromName(`seats:${eventId}`);
          const stub = env.SEATS.get(id);

          const doUrl = new URL(request.url);
          doUrl.pathname = `/seats/${action}`;

          return await stub.fetch(new Request(doUrl.toString(), request));
        }
      }

      // BroadcastLockDO proxy (gated by broadcast key)
      if (pathname.startsWith("/api/broadcast/")) {
        const action = pathname.split("/").pop() || "";
        const body = method === "POST" ? await readJson(request) : {};
        const eventId = String(body.eventId ?? url.searchParams.get("eventId") ?? "");
        const key = String(body.key ?? url.searchParams.get("key") ?? "");

        if (!eventId) return json(env, { error: "missing_eventId" }, 400);

        const client = await getClient(env);
        try {
          const ev = await getEvent(client, eventId);
          if (!ev) return json(env, { error: "not_found" }, 404);

          const authRes = requireExact(key, ev.broadcast_key, env);
          if (authRes) return authRes;

          const id = env.BROADCAST_LOCK.idFromName(`lock:${eventId}`);
          const stub = env.BROADCAST_LOCK.get(id);

          const doUrl = new URL(request.url);
          doUrl.pathname = `/lock/${action}`;

          return await stub.fetch(new Request(doUrl.toString(), request));
        } finally {
          await client.end();
        }
      }

      // HLS provisioning (IVS Channel)
      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/hls\/provision$/);
        if (method === "POST" && m) {
          const eventId = m[0];
          const key = url.searchParams.get("key");

          const client = await getClient(env);
          try {
            const ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);
            if (ev.status !== "paid") return json(env, { error: "not_paid" }, 400);
            if (!ev.hls_enabled) return json(env, { error: "hls_disabled" }, 400);

            const authRes = requireExact(key, ev.broadcast_key, env);
            if (authRes) return authRes;

            // Return existing
            if (ev.ivs_channel_arn && ev.ivs_ingest_endpoint && ev.ivs_playback_url && ev.ivs_stream_key_encrypted) {
              const host = ingestHostFromDb(ev.ivs_ingest_endpoint);
              const streamKey = await decryptString(ev.ivs_stream_key_encrypted, env.STREAMKEY_ENC_KEY_B64);
              return json(env, {
                ok: true,
                alreadyProvisioned: true,
                ingest: { rtmpsUrl: rtmpsUrlFromHost(host), streamKey },
                playback: { url: ev.ivs_playback_url },
              });
            }

            const ch = await createChannel(env, `relay-${eventId}`);
            const sk = await createStreamKey(env, ch.channelArn);
            const enc = await encryptString(sk.streamKeyValue, env.STREAMKEY_ENC_KEY_B64);

            await updateIvs(client, eventId, ch.channelArn, ch.ingestEndpoint, ch.playbackUrl, enc);

            const host = ingestHostFromDb(ch.ingestEndpoint);

            return json(env, {
              ok: true,
              ingest: { rtmpsUrl: rtmpsUrlFromHost(host), streamKey: sk.streamKeyValue },
              playback: { url: ch.playbackUrl },
            });
          } finally {
            await client.end();
          }
        }
      }

      // WebRTC provisioning (Stage) -> returns host token
      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/rtc\/provision$/);
        if (method === "POST" && m) {
          const eventId = m[0];
          const key = url.searchParams.get("key");

          const client = await getClient(env);
          try {
            const ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);
            if (ev.status !== "paid") return json(env, { error: "not_paid" }, 400);
            if (!ev.rtc_enabled) return json(env, { error: "rtc_disabled" }, 400);

            const authRes = requireExact(key, ev.broadcast_key, env);
            if (authRes) return authRes;

            let stageArn = ev.rtc_stage_arn as string | null;
            let endpoints: any = ev.rtc_stage_endpoints ?? null;

            if (!stageArn) {
              const st = await createStage(env, `relay-${eventId}`);
              stageArn = st.stageArn;
              endpoints = st.endpoints || null;
              await updateRtc(client, eventId, stageArn, endpoints);
            }

            const token = await createParticipantToken(env, stageArn, `host-${eventId}`, ["PUBLISH", "SUBSCRIBE"], 3600);

            return json(env, { ok: true, stageArn, participantToken: token.token, endpoints });
          } finally {
            await client.end();
          }
        }
      }

      // WebRTC viewer token mint (anyone with watch key)
      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/rtc\/token$/);
        if (method === "POST" && m) {
          const eventId = m[0];
          const key = url.searchParams.get("key");

          const client = await getClient(env);
          try {
            const ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);
            if (ev.status !== "paid") return json(env, { error: "not_paid" }, 400);
            if (!ev.rtc_enabled) return json(env, { error: "rtc_disabled" }, 400);

            const authRes = requireExact(key, ev.secret_key, env);
            if (authRes) return authRes;

            const stageArn = ev.rtc_stage_arn as string | null;
            if (!stageArn) return json(env, { error: "not_live_yet" }, 409);

            const token = await createParticipantToken(env, stageArn, `viewer-${randomSecretUrlSafe(10)}`, ["SUBSCRIBE"], 1800);

            return json(env, { ok: true, stageArn, participantToken: token.token });
          } finally {
            await client.end();
          }
        }
      }

      // GET /api/events/:id (legacy/simple)
      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)$/);
        if (method === "GET" && m) {
          const eventId = m[0];
          const client = await getClient(env);
          try {
            const ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);

            const expired = ev.status === "expired" || (ev.expires_at && new Date(ev.expires_at).getTime() < Date.now());

            return json(env, {
              id: ev.id,
              title: ev.title,
              tier: ev.tier,
              viewer_limit: ev.viewer_limit,
              white_label: ev.white_label,
              status: ev.status,
              expired,
              playback_url: ev.ivs_playback_url || null,
              rtc_stage_arn: ev.rtc_stage_arn || null,
              rtc_enabled: !!ev.rtc_enabled,
              hls_enabled: !!ev.hls_enabled,
            });
          } finally {
            await client.end();
          }
        }
      }

      return text(env, "Not Found", 404);
    } catch (err) {
      console.error("Worker error:", err);
      return text(env, "Internal Server Error", 500);
    }
  },

  async scheduled(_event: any, env: Env) {
    const client = await getClient(env);
    try {
      const { rows } = await client.query(
        `
        update public.events
        set status='expired'
        where expires_at is not null
          and expires_at < now()
          and status != 'expired'
        returning id, ivs_channel_arn, rtc_stage_arn
      `
      );

      const doDelete = String(env.DELETE_IVS_ON_EXPIRE ?? "false").toLowerCase() === "true";
      if (doDelete && rows?.length) {
        for (const r of rows) {
          try {
            if (r.ivs_channel_arn) await deleteIvsChannel(env, r.ivs_channel_arn);
          } catch (e) {
            console.error("DeleteChannel failed", r.id, e);
          }
          try {
            if (r.rtc_stage_arn) await deleteStage(env, r.rtc_stage_arn);
          } catch (e) {
            console.error("DeleteStage failed", r.id, e);
          }
        }
      }
    } catch (err) {
      console.error("Cron error:", err);
    } finally {
      await client.end();
    }
  },
};
