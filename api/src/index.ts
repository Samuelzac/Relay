// src/index.ts
import { getClient } from "./db";
import { randomSecretUrlSafe, sha256Hex, encryptString, decryptString } from "./crypto";
import { SeatsDO } from "./seats_do";
import { BroadcastLockDO } from "./broadcast_lock_do";
import { stripeClient, priceForTierAndMode, hoursForTier, StreamMode, normalizeMode } from "./stripe";
import { createChannel, createStreamKey, getChannel } from "./awsIvs";
import { deleteIvsChannel } from "./ivs";
import {
  createStage,
  createParticipantToken,
  deleteStage,
  createEncoderConfiguration,
  createComposition,
  stopComposition,
} from "./awsIvsRealtime";

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
    headers: { "content-type": "text/plain; charset=utf-8", ...corsHeaders(env) },
  });
}

function match(pathname: string, re: RegExp): string[] | null {
  const m = pathname.match(re);
  if (!m) return null;
  return m.slice(1);
}

function requireExact(got: string | null, expected: string | null, env: Env) {
  if (!expected) return json(env, { error: "missing_key" }, 401);
  if (!got) return json(env, { error: "missing_key" }, 401);
  if (got !== expected) return json(env, { error: "unauthorized" }, 401);
  return null;
}

function requireAdminKey(request: Request, env: Env) {
  const got = request.headers.get("x-relay-admin-key");
  const expected = env.ADMIN_KEY || "";
  if (!expected) return json(env, { error: "admin_key_not_configured" }, 500);
  return requireExact(got, expected, env);
}

function requireStreamBroadcastKey(key: string | null, env: Env) {
  const expected = env.STREAM_BROADCAST_KEY || "";
  if (!expected) return json(env, { error: "stream_broadcast_key_not_configured" }, 500);
  return requireExact(key, expected, env);
}

function ingestHostFromDb(s: any): string {
  const v = String(s || "");
  return v.replace(/^https?:\/\//, "").replace(/\/+$/, "");
}

function rtmpsUrlFromHost(host: string) {
  return `rtmps://${host}:443/app/`;
}

function isExpired(ev: any) {
  if (!ev) return true;
  if (ev.status === "expired") return true;
  const exp = ev.expires_at ? new Date(ev.expires_at) : null;
  if (!exp) return false;
  return exp.getTime() <= Date.now();
}

// --- IVS Real-Time Composition helpers (Stage -> Channel (HLS)) ---
// We store encoderConfigurationArn + compositionArn in rtc_stage_endpoints JSON for now.

function getCompositionArnFromEndpoints(endpoints: any): string | null {
  try {
    return (endpoints && (endpoints.compositionArn || endpoints.composition_arn)) || null;
  } catch {
    return null;
  }
}

function withCompositionArn(endpoints: any, compositionArn: string) {
  const base = endpoints && typeof endpoints === "object" ? endpoints : {};
  return { ...base, compositionArn };
}

function getEncoderConfigArnFromEndpoints(endpoints: any): string | null {
  try {
    return (endpoints && (endpoints.encoderConfigurationArn || endpoints.encoder_configuration_arn)) || null;
  } catch {
    return null;
  }
}

function withEncoderConfigArn(endpoints: any, encoderConfigurationArn: string) {
  const base = endpoints && typeof endpoints === "object" ? endpoints : {};
  return { ...base, encoderConfigurationArn };
}


async function ensureHlsInfrastructure(client: any, env: Env, eventId: string, evIn: any) {
  let ev = evIn;
  if (!ev || !ev.hls_enabled) return ev;

  let stageArn: string | null = (ev.rtc_stage_arn as string) || null;
  let endpoints: any = ev.rtc_stage_endpoints ?? null;

  if (!stageArn) {
    const st = await createStage(env, `relay-${eventId}`);
    stageArn = st.stageArn;
    endpoints = st.endpoints || null;
    await updateRtc(client, eventId, stageArn, endpoints);
    ev = await getEvent(client, eventId);
    stageArn = (ev?.rtc_stage_arn as string) || stageArn;
    endpoints = ev?.rtc_stage_endpoints ?? endpoints;
  }

  let channelArn: string | null = (ev.ivs_channel_arn as string) || null;

  if (channelArn) {
    try {
      const ch = await getChannel(env, channelArn);
      await updateIvs(client, eventId, ch.channelArn, ch.ingestEndpoint, ch.playbackUrl, ev.ivs_stream_key_encrypted || null);
    } catch (e) {
      console.warn("ensureHlsInfrastructure: existing channel refresh failed", eventId, e);
      channelArn = null;
    }
  }

  if (!channelArn) {
    const ch = await createChannel(env, `relay-${eventId}`);
    await updateIvs(client, eventId, ch.channelArn, ch.ingestEndpoint, ch.playbackUrl, ev.ivs_stream_key_encrypted || null);
    channelArn = ch.channelArn;
  }

  ev = await getEvent(client, eventId);
  endpoints = ev.rtc_stage_endpoints ?? endpoints;

  let encoderConfigurationArn = getEncoderConfigArnFromEndpoints(endpoints);
  if (!encoderConfigurationArn) {
    const enc = await createEncoderConfiguration(env, `relay-${eventId}`);
    encoderConfigurationArn = enc?.arn || enc?.encoderConfiguration?.arn || null;
    if (encoderConfigurationArn) {
      endpoints = withEncoderConfigArn(endpoints, encoderConfigurationArn);
      await updateRtcEndpoints(client, eventId, endpoints);
    }
  }

  return await getEvent(client, eventId);
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
  await client.query(`update public.events set status='paid' where id=$1`, [id]);
}

async function updateIvs(
  client: any,
  id: string,
  channelArn: string,
  ingestEndpoint: string,
  playbackUrl: string,
  streamKeyEncrypted: string | null
) {
  await client.query(
    `
    update public.events
    set ivs_channel_arn=$1,
        ivs_ingest_endpoint=$2,
        ivs_playback_url=$3,
        ivs_stream_key_encrypted=$4
    where id=$5
  `,
    [channelArn, ingestEndpoint, playbackUrl, streamKeyEncrypted, id]
  );
}

async function updateRtc(client: any, id: string, stageArn: string, endpoints: any) {
  const endpointsJson = endpoints ? JSON.stringify(endpoints) : null;
  const { rows } = await client.query(
    `
    update public.events
    set rtc_stage_arn = $1,
        rtc_stage_endpoints = $2::jsonb
    where id = $3
    returning rtc_stage_arn, rtc_stage_endpoints
  `,
    [stageArn, endpointsJson, id]
  );
  return rows[0] || null;
}

async function updateRtcEndpoints(client: any, id: string, endpoints: any) {
  const endpointsJson = endpoints ? JSON.stringify(endpoints) : null;
  const { rows } = await client.query(
    `
    update public.events
    set rtc_stage_endpoints = $1::jsonb
    where id = $2
    returning rtc_stage_endpoints
  `,
    [endpointsJson, id]
  );
  return rows[0] || null;
}

function dollarsToCents(n: any): number {
  const v = Number(n || 0);
  if (!Number.isFinite(v)) return 0;
  return Math.round(v * 100);
}

export default {
  async fetch(request: Request, env: Env) {
    const url = new URL(request.url);
    const { pathname } = url;
    const method = request.method.toUpperCase();

    if (method === "OPTIONS") return new Response(null, { status: 204, headers: corsHeaders(env) });

    try {
      if (method === "GET" && pathname === "/") return text(env, "Relay API OK", 200);
      if (method === "GET" && pathname === "/healthz") return text(env, "ok", 200);

      if (method === "GET" && pathname === "/api/pricing") {
        const standardHours = hoursForTier(env, 3);
        const extendedHours = hoursForTier(env, 8);

        const standardBase = Number(env.STANDARD_PRICE_NZD || 0);
        const extendedBase = Number(env.EXTENDED_PRICE_NZD || 0);

        const hlsAddon = Number(env.HLS_ADDON_NZD ?? 0);
        const rtcAddon = Number(env.RTC_ADDON_NZD ?? env.WEBRTC_ADDON_NZD ?? 0);
        const bothAddon = Number(env.BOTH_ADDON_NZD ?? 0);

        return json(env, {
          ok: true,
          tiers: {
            "3": { hours: standardHours, base_nzd: dollarsToCents(standardBase) },
            "8": { hours: extendedHours, base_nzd: dollarsToCents(extendedBase) },
          },
          addons: {
            hls_nzd: dollarsToCents(hlsAddon),
            rtc_nzd: dollarsToCents(rtcAddon),
            both_nzd: dollarsToCents(bothAddon),
            webrtc_nzd: dollarsToCents(rtcAddon),
          },
        });
      }

      if (method === "GET" && pathname === "/api/admin/events") {
        const auth = requireAdminKey(request, env);
        if (auth) return auth;

        const client = await getClient(env);
        try {
          const { rows } = await client.query(
            `
            select id, email, title, tier, viewer_limit, white_label,
                   status, starts_at, expires_at, created_at,
                   rtc_enabled, hls_enabled
            from public.events
            order by created_at desc
            limit 200
          `
          );
          return json(env, { ok: true, events: rows });
        } finally {
          await client.end();
        }
      }

      if (method === "POST" && pathname === "/api/checkout") {
        const body = await request.json().catch(() => ({}));
        const email = String(body.email || "").trim();
        const title = String(body.title || "").trim();
        const tier = Number(body.tier || 3);
        const viewerLimitRaw = body.viewer_limit;
        const viewerLimit = Number.isFinite(Number(viewerLimitRaw))
          ? Number(viewerLimitRaw)
          : Number(env.DEFAULT_VIEWER_LIMIT || 0);
        const whiteLabel = !!body.white_label;

        const mode: StreamMode = normalizeMode(body.mode || "rtc");

        if (!email) return json(env, { error: "missing_email" }, 400);
        if (!title) return json(env, { error: "missing_title" }, 400);
        if (![3, 8].includes(tier)) return json(env, { error: "invalid_tier" }, 400);

        const rtcEnabled = mode === "rtc" || mode === "both";
        const hlsEnabled = mode === "hls" || mode === "both";

        const secretKey = randomSecretUrlSafe(24);
        const broadcastKey = randomSecretUrlSafe(24);
        const successToken = randomSecretUrlSafe(24);
        const successTokenHash = await sha256Hex(successToken);

        const client = await getClient(env);
        let eventId: string;
        try {
          const { rows } = await client.query(
            `
            insert into public.events
              (email, title, tier, viewer_limit, white_label,
               status, secret_key, broadcast_key, success_token_hash,
               rtc_enabled, hls_enabled)
            values
              ($1,$2,$3,$4,$5,'created',$6,$7,$8,$9,$10)
            returning id
          `,
            [email, title, tier, viewerLimit, whiteLabel, secretKey, broadcastKey, successTokenHash, rtcEnabled, hlsEnabled]
          );
          eventId = rows[0].id;
        } finally {
          await client.end();
        }

        const stripe = stripeClient(env);
        const price = priceForTierAndMode(env, tier, mode);

        const successUrl = `${env.APP_ORIGIN}/success?event=${encodeURIComponent(eventId)}&st=${encodeURIComponent(successToken)}`;
        const cancelUrl = `${env.APP_ORIGIN}/`;

        const session = await stripe.checkout.sessions.create({
          mode: "payment",
          customer_email: email,
          line_items: [
            {
              price_data: {
                currency: "nzd",
                unit_amount: Math.round(price * 100),
                product_data: { name: `Relay stream (${tier}h, ${mode})` },
              },
              quantity: 1,
            },
          ],
          success_url: successUrl,
          cancel_url: cancelUrl,
          metadata: { relay_event_id: eventId, relay_mode: mode, relay_tier: String(tier) },
        });

        const client2 = await getClient(env);
        try {
          await client2.query(`update public.events set stripe_session_id=$1 where id=$2`, [session.id, eventId]);
        } finally {
          await client2.end();
        }

        return json(env, { ok: true, url: session.url, eventId }, 200);
      }

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

              let ev = await getEvent(client, eventId);
              if ((ev?.rtc_enabled || ev?.hls_enabled) && !ev.rtc_stage_arn && ev.status === "paid" && !isExpired(ev)) {
                try {
                  const st = await createStage(env, `relay-${eventId}`);
                  await updateRtc(client, eventId, st.stageArn, st.endpoints || null);
                  ev = await getEvent(client, eventId);
                } catch (e) {
                  console.error("RTC stage provision failed (webhook)", eventId, e);
                }
              }

              if (ev?.hls_enabled && ev.status === "paid" && !isExpired(ev)) {
                try {
                  ev = await ensureHlsInfrastructure(client, env, eventId, ev);
                } catch (e) {
                  console.error("HLS pre-provision failed (webhook)", eventId, e);
                }
              }
            } finally {
              await client.end();
            }
          }
        }

        return json(env, { ok: true }, 200);
      }

      // POST /api/events/:id/start
      // IMPORTANT: This route is now IDEMPOTENT.
      // If starts_at is already set, we DO NOT early-return.
      // We still attempt to provision HLS (stage+channel+encoder+composition) until it is working.
      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/start$/);
        if (method === "POST" && m) {
          const eventId = m[0];
          const key = url.searchParams.get("key") || "";

          const client = await getClient(env);
          try {
            let ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);

            // Allow either broadcast_key (broadcaster control) OR secret_key (watch link) to call /start.
            if ((!ev.broadcast_key && !ev.secret_key) || !key) return json(env, { error: "missing_key" }, 401);
            if (key !== ev.broadcast_key && key !== ev.secret_key) return json(env, { error: "unauthorized" }, 401);

if (ev.status !== "paid") return json(env, { error: "not_paid" }, 403);
            if (isExpired(ev)) return json(env, { error: "expired" }, 410);

            console.log("START route", JSON.stringify({ eventId, hls_enabled: !!ev.hls_enabled, starts_at: ev.starts_at || null }));

            // Start clock only once
            let startedNow = false;
            let startsAtIso = ev.starts_at ? new Date(ev.starts_at).toISOString() : null;
            let expiresAtIso = ev.expires_at ? new Date(ev.expires_at).toISOString() : null;

            if (!ev.starts_at) {
              const now = new Date();
              const tierHours = hoursForTier(env, Number(ev.tier));
              const graceMin = Number(env.GRACE_MINUTES || "0");
              const expires = new Date(now.getTime() + (tierHours * 60 + graceMin) * 60 * 1000);

              await client.query(
                `update public.events
                 set starts_at=$1, expires_at=$2
                 where id=$3 and starts_at is null`,
                [now.toISOString(), expires.toISOString(), eventId]
              );

              startedNow = true;
              startsAtIso = now.toISOString();
              expiresAtIso = expires.toISOString();

              // refresh ev
              ev = await getEvent(client, eventId);
            }

            // Provision HLS path if enabled
            let compositionStarted = false;
            let compositionArn: string | null = null;

            if (ev.hls_enabled) {
              console.log("START(HLS) entered", eventId);

              // 1) Ensure Stage exists
              let stageArn: string | null = (ev.rtc_stage_arn as string) || null;
              let endpoints: any = ev.rtc_stage_endpoints ?? null;

              if (!stageArn) {
                console.log("START(HLS): creating stage...");
                const st = await createStage(env, `relay-${eventId}`);
                stageArn = st.stageArn;
                endpoints = st.endpoints || null;
                await updateRtc(client, eventId, stageArn, endpoints);
                console.log("START(HLS): stageArn", stageArn);
              }

              // refresh after stage update
              ev = await getEvent(client, eventId);
              endpoints = ev.rtc_stage_endpoints ?? endpoints;

              // 2) Ensure Channel exists and store playback_url
              let channelArn: string | null = (ev.ivs_channel_arn as string) || null;

              if (channelArn) {
                try {
                  const ch = await getChannel(env, channelArn);
                  await updateIvs(client, eventId, ch.channelArn, ch.ingestEndpoint, ch.playbackUrl, ev.ivs_stream_key_encrypted || null);
                  console.log("START(HLS): refreshed channel playbackUrl");
                } catch (e) {
                  console.warn("START(HLS): GetChannel refresh failed (will try create)", eventId, e);
                  channelArn = null;
                }
              }

              if (!channelArn) {
                console.log("START(HLS): creating channel...");
                const ch = await createChannel(env, `relay-${eventId}`);
                await updateIvs(client, eventId, ch.channelArn, ch.ingestEndpoint, ch.playbackUrl, ev.ivs_stream_key_encrypted || null);
                channelArn = ch.channelArn;
                console.log("START(HLS): channelArn", channelArn);
              }

              // refresh after channel update
              ev = await getEvent(client, eventId);

              // 3) Ensure Encoder Configuration exists
              let encoderConfigurationArn = getEncoderConfigArnFromEndpoints(endpoints);
              if (!encoderConfigurationArn) {
                console.log("START(HLS): creating encoder configuration...");
                try {
                  const enc = await createEncoderConfiguration(env, `relay-${eventId}`);
                  encoderConfigurationArn = enc?.arn || enc?.encoderConfiguration?.arn || null;

                  if (encoderConfigurationArn) {
                    endpoints = withEncoderConfigArn(endpoints, encoderConfigurationArn);
                    await updateRtcEndpoints(client, eventId, endpoints);
                    console.log("START(HLS): encoderConfigurationArn", encoderConfigurationArn);
                  } else {
                    console.error("START(HLS): encoder config response missing arn", JSON.stringify(enc));
                  }
                } catch (e) {
                  console.error("START(HLS): CreateEncoderConfiguration failed", eventId, e);
                }
              }

              // 4) Ensure Composition is STARTED (stored ARN does NOT mean running)
              compositionArn = getCompositionArnFromEndpoints(endpoints);

              if (stageArn && channelArn && encoderConfigurationArn) {
                console.log("START(HLS): ensuring composition started...");
                try {
                  // Use a stable idempotency token so retries won't create duplicate compositions.
                  // (AWS allows replays with the same token to be treated idempotently.)
                  const idempotencyToken = `relay-${eventId}-hls`;

                  const comp = await createComposition(env, stageArn, channelArn, encoderConfigurationArn, idempotencyToken);

                  const compArn = (comp && (comp as any).arn) || (comp as any)?.composition?.arn || null;
                  if (compArn) {
                    compositionArn = compArn;
                    endpoints = withCompositionArn(endpoints, compositionArn);
                    await updateRtcEndpoints(client, eventId, endpoints);
                    compositionStarted = true;
                    console.log("START(HLS): compositionArn", compositionArn);
                  } else {
                    console.error("START(HLS): StartComposition response missing arn", JSON.stringify(comp));
                  }
                } catch (e) {
                  console.error("START(HLS): StartComposition failed", eventId, e);
                }
              } else {
                console.log(
                  "START(HLS): composition not started (missing prereqs)",
                  JSON.stringify({ stageArn, channelArn, encoderConfigurationArn })
                );
              }

}

            // Re-read to return updated playback_url
            const evOut = await getEvent(client, eventId);

            return json(env, {
              ok: true,
              started: startedNow,
              starts_at: startsAtIso,
              expires_at: expiresAtIso,
              playback_url: evOut.ivs_playback_url || null,
              composition_started: compositionStarted,
              composition_arn: compositionArn,
            });
          } finally {
            await client.end();
          }
        }
      }

      // POST /api/events/:id/stop
      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/stop$/);
        if (method === "POST" && m) {
          const eventId = m[0];
          const key = url.searchParams.get("key") || "";

          const client = await getClient(env);
          try {
            const ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);

            const authRes = requireExact(key, ev.broadcast_key, env);
            if (authRes) return authRes;

            const endpoints: any = ev.rtc_stage_endpoints ?? null;
            const compositionArn = getCompositionArnFromEndpoints(endpoints);

            if (compositionArn) {
              try {
                await stopComposition(env, compositionArn);
              } catch (e) {
                console.error("StopComposition failed", eventId, e);
              }

              try {
                const cleaned = { ...(endpoints || {}) };
                delete cleaned.compositionArn;
                delete cleaned.composition_arn;
                await updateRtcEndpoints(client, eventId, cleaned);
              } catch (e) {
                console.error("Failed clearing compositionArn from endpoints", eventId, e);
              }
            }

            return json(env, { ok: true, stopped: true }, 200);
          } finally {
            await client.end();
          }
        }
      }

      // Success -> return links
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

            if (ev.status !== "paid" && ev.stripe_session_id) {
              try {
                const stripe = stripeClient(env);
                const sess = await stripe.checkout.sessions.retrieve(ev.stripe_session_id);
                if (sess.payment_status === "paid") {
                  await markPaid(client, eventId);
                }
              } catch (e) {
                console.error("stripe verify failed", eventId, e);
              }
            }

            const ev2 = await getEvent(client, eventId);

            return json(env, {
              ok: true,
              watch_url: `${env.APP_ORIGIN}/watch?event=${encodeURIComponent(eventId)}&key=${encodeURIComponent(ev2.secret_key)}`,
              broadcast_url: `${env.APP_ORIGIN}/broadcast?event=${encodeURIComponent(eventId)}&key=${encodeURIComponent(ev2.broadcast_key)}`,
              rtc_enabled: !!ev2.rtc_enabled,
              hls_enabled: !!ev2.hls_enabled,
              playback_url: ev2.ivs_playback_url || null,
            });
          } finally {
            await client.end();
          }
        }
      }

      // Public event info
      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/public$/);
        if (method === "GET" && m) {
          const eventId = m[0];
          const key = url.searchParams.get("key");

          const client = await getClient(env);
          try {
            const ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);

            // Allow either the watch secret_key or the broadcast_key to read public status.
            // This keeps watch links working exactly as before and lets the broadcast page
            // poll readiness without tripping a 401.
            if ((!ev.secret_key && !ev.broadcast_key) || !key) return json(env, { error: "missing_key" }, 401);
            if (key !== ev.secret_key && key !== ev.broadcast_key) return json(env, { error: "unauthorized" }, 401);

            const expired = isExpired(ev);

            return json(env, {
              ok: true,
              id: ev.id,
              title: ev.title,
              status: ev.status,
              expired,
              starts_at: ev.starts_at || null,
              expires_at: ev.expires_at || null,
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

      // SeatsDO proxy
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

      // BroadcastLockDO proxy
      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/broadcast-lock\/(acquire|release|status)$/);
        if (m) {
          const eventId = m[0];
          const action = m[1];
          const key = url.searchParams.get("key") || "";

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
      }

      // HLS provisioning (RTMP ingest path) - kept for backwards compatibility
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
            if (isExpired(ev)) return json(env, { error: "expired" }, 410);
            if (!ev.hls_enabled) return json(env, { error: "hls_disabled" }, 400);

            const authRes = requireExact(key, ev.broadcast_key, env);
            if (authRes) return authRes;

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

            if (ev.ivs_channel_arn && ev.ivs_ingest_endpoint && ev.ivs_playback_url && !ev.ivs_stream_key_encrypted) {
              try {
                const sk = await createStreamKey(env, ev.ivs_channel_arn);
                const enc = await encryptString(sk.streamKeyValue, env.STREAMKEY_ENC_KEY_B64);

                await updateIvs(client, eventId, ev.ivs_channel_arn, ev.ivs_ingest_endpoint, ev.ivs_playback_url, enc);

                const host = ingestHostFromDb(ev.ivs_ingest_endpoint);
                return json(env, {
                  ok: true,
                  alreadyProvisioned: false,
                  ingest: { rtmpsUrl: rtmpsUrlFromHost(host), streamKey: sk.streamKeyValue },
                  playback: { url: ev.ivs_playback_url },
                });
              } catch (e: any) {
                const msg = String(e?.message || e || "");
                if (!msg.includes("ServiceQuotaExceededException") && !msg.includes("quota exceeded")) throw e;
              }
            }

            const oldArn = ev.ivs_channel_arn || null;

            const ch = await createChannel(env, `relay-${eventId}`);
            const sk = await createStreamKey(env, ch.channelArn);
            const enc = await encryptString(sk.streamKeyValue, env.STREAMKEY_ENC_KEY_B64);

            await updateIvs(client, eventId, ch.channelArn, ch.ingestEndpoint, ch.playbackUrl, enc);

            if (oldArn && oldArn !== ch.channelArn) {
              try {
                await deleteIvsChannel(env, oldArn);
              } catch (e) {
                console.error("deleteIvsChannel failed (best-effort)", eventId, e);
              }
            }

            const host = ingestHostFromDb(ch.ingestEndpoint);

            return json(env, {
              ok: true,
              alreadyProvisioned: false,
              ingest: { rtmpsUrl: rtmpsUrlFromHost(host), streamKey: sk.streamKeyValue },
              playback: { url: ch.playbackUrl },
            });
          } finally {
            await client.end();
          }
        }
      }

      // WebRTC host token mint
      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/rtc\/host$/);
        if (method === "POST" && m) {
          const eventId = m[0];
          const key = url.searchParams.get("key");

          const client = await getClient(env);
          try {
            let ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);
            if (ev.status !== "paid") return json(env, { error: "not_paid" }, 400);
            if (isExpired(ev)) return json(env, { error: "expired" }, 410);

            if (!(ev.rtc_enabled || ev.hls_enabled)) return json(env, { error: "rtc_ingest_disabled" }, 400);

            const authRes = requireExact(key, ev.broadcast_key, env);
            if (authRes) return authRes;

            let stageArn: string | null = ev.rtc_stage_arn as string | null;
            let endpoints: any = ev.rtc_stage_endpoints ?? null;

            if (!stageArn) {
              const st = await createStage(env, `relay-${eventId}`);
              stageArn = st.stageArn;
              endpoints = st.endpoints || null;

              await updateRtc(client, eventId, stageArn, endpoints);

              ev = await getEvent(client, eventId);
              stageArn = (ev?.rtc_stage_arn as string) || stageArn;
              endpoints = ev?.rtc_stage_endpoints ?? endpoints;
            }

            const token = await createParticipantToken(env, stageArn!, `host-${eventId}`, ["PUBLISH", "SUBSCRIBE"], 3600);

            return json(env, { ok: true, stageArn, participantToken: token.token, endpoints });
          } finally {
            await client.end();
          }
        }
      }

      // WebRTC viewer token mint (rtc_enabled only)
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
            if (isExpired(ev)) return json(env, { error: "expired" }, 410);

            if (!ev.rtc_enabled) return json(env, { error: "rtc_disabled" }, 400);

            // Allow either the watch secret_key or the broadcast_key to read public status.
            // This keeps watch links working exactly as before and lets the broadcast page
            // poll readiness without tripping a 401.
            if ((!ev.secret_key && !ev.broadcast_key) || !key) return json(env, { error: "missing_key" }, 401);
            if (key !== ev.secret_key && key !== ev.broadcast_key) return json(env, { error: "unauthorized" }, 401);

            if (!ev.rtc_stage_arn) return json(env, { error: "rtc_not_provisioned" }, 400);

            const token = await createParticipantToken(
              env,
              ev.rtc_stage_arn as string,
              `viewer-${randomSecretUrlSafe(8)}`,
              ["SUBSCRIBE"],
              3600
            );

            return json(env, { ok: true, stageArn: ev.rtc_stage_arn, participantToken: token.token });
          } finally {
            await client.end();
          }
        }
      }

      // Delete event (admin)
      {
        const m = match(pathname, /^\/api\/admin\/events\/([^\/]+)\/delete$/);
        if (method === "POST" && m) {
          const auth = requireAdminKey(request, env);
          if (auth) return auth;

          const eventId = m[0];

          const client = await getClient(env);
          try {
            const ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);

            try {
              if (ev.ivs_channel_arn) await deleteIvsChannel(env, ev.ivs_channel_arn);
            } catch (e) {
              console.error("deleteIvsChannel failed", eventId, e);
            }
            try {
              if (ev.rtc_stage_arn) await deleteStage(env, ev.rtc_stage_arn);
            } catch (e) {
              console.error("deleteStage failed", eventId, e);
            }

            await client.query(`delete from public.events where id=$1`, [eventId]);

            return json(env, { ok: true }, 200);
          } finally {
            await client.end();
          }
        }
      }

      // Event WebRTC ingest provisioning (broadcast page) - supports /rtc/provision and /rtc/host
      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/rtc\/(provision|host)$/);
        if (method === "POST" && m) {
          const eventId = m[0];
          const key = url.searchParams.get("key");

          const client = await getClient(env);
          try {
            let ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);

            if (ev.status !== "paid") return json(env, { error: "not_paid" }, 400);
            if (isExpired(ev)) return json(env, { error: "expired" }, 410);

            if (!(ev.rtc_enabled || ev.hls_enabled)) return json(env, { error: "rtc_ingest_disabled" }, 400);

            const authRes = requireExact(key, ev.broadcast_key, env);
            if (authRes) return authRes;

            let stageArn: string | null = ev.rtc_stage_arn as string | null;
            let endpoints: any = ev.rtc_stage_endpoints ?? null;

            if (!stageArn) {
              const st = await createStage(env, `relay-${eventId}`);
              stageArn = st.stageArn;
              endpoints = st.endpoints || null;

              await updateRtc(client, eventId, stageArn, endpoints);

              ev = await getEvent(client, eventId);
              stageArn = (ev?.rtc_stage_arn as string) || stageArn;
              endpoints = ev?.rtc_stage_endpoints ?? endpoints;
            }

            const token = await createParticipantToken(env, stageArn!, `host-${eventId}`, ["PUBLISH", "SUBSCRIBE"], 3600);

            return json(env, { ok: true, stageArn, participantToken: token.token, endpoints });
          } finally {
            await client.end();
          }
        }
      }

      // Fixed stream names ingest provisioning
      {
        const m = match(pathname, /^\/api\/streams\/([^\/]+)\/rtc\/provision$/);
        if (method === "POST" && m) {
          const streamName = decodeURIComponent(m[0]);
          const key = url.searchParams.get("key");

          const client = await getClient(env);
          try {
            const st = await (async () => {
              const { rows } = await client.query(
                `select stream_name, stage_arn, endpoints, is_enabled from public.streams where stream_name=$1`,
                [streamName]
              );
              return rows[0] || null;
            })();

            if (!st) return json(env, { error: "not_found" }, 404);
            if (!st.is_enabled) return json(env, { error: "disabled" }, 403);

            const authRes = requireStreamBroadcastKey(key, env);
            if (authRes) return authRes;

            let stageArn: string | null = st.stage_arn || null;
            let endpoints: any = st.endpoints || null;

            if (!stageArn) {
              const created = await createStage(env, `stream-${streamName}`);
              stageArn = created.stageArn;
              endpoints = created.endpoints || null;

              await client.query(
                `update public.streams set stage_arn=$1, endpoints=$2::jsonb where stream_name=$3`,
                [stageArn, endpoints ? JSON.stringify(endpoints) : null, streamName]
              );
            }

            const token = await createParticipantToken(env, stageArn, `host-${streamName}`, ["PUBLISH", "SUBSCRIBE"], 3600);

            return json(env, { ok: true, stageArn, participantToken: token.token, endpoints });
          } finally {
            await client.end();
          }
        }
      }

      return json(env, { error: "not_found" }, 404);
    } catch (e: any) {
      console.error("Unhandled error:", e);
      return json(env, { error: "server_error", message: e?.message || String(e) }, 500);
    }
  },
};

// Cron trigger handler
export async function scheduled(event: ScheduledEvent, env: any, ctx: ExecutionContext) {
  console.log("cron tick", new Date().toISOString());
}
