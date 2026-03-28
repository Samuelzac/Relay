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
type AccessRole = "viewer" | "broadcaster";

type Readiness = {
  role: AccessRole;
  paid: boolean;
  expired: boolean;
  stage_exists: boolean;
  hls_enabled: boolean;
  rtc_enabled: boolean;
  hls_channel_exists: boolean;
  encoder_configuration_exists: boolean;
  composition_started: boolean;
  playback_url_exists: boolean;
  whip_url_exists: boolean;
  stream_window_open: boolean;
  can_issue_broadcaster_token: boolean;
  can_issue_viewer_token: boolean;
  can_go_live: boolean;
  can_watch_hls: boolean;
  can_watch_rtc: boolean;
  state: string;
  detail: string;
  playback_url: string | null;
  expires_at: string | null;
  starts_at: string | null;
};

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

function withoutCompositionArn(endpoints: any) {
  const base = endpoints && typeof endpoints === "object" ? { ...endpoints } : {};
  delete (base as any).compositionArn;
  delete (base as any).composition_arn;
  return base;
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

function getSharedEncoderConfigurationArn(env: Env): string | null {
  const v =
    env.SHARED_ENCODER_CONFIGURATION_ARN ||
    env.IVS_SHARED_ENCODER_CONFIGURATION_ARN ||
    env.ENCODER_CONFIGURATION_ARN ||
    env.IVS_ENCODER_CONFIGURATION_ARN ||
    null;
  return v ? String(v) : null;
}

function isCompositionConflictError(err: any): boolean {
  const msg = String(err?.message || err || "");
  return (
    msg.includes("ConflictException") ||
    msg.includes("already exists with the given attributes") ||
    msg.includes("409")
  );
}

function accessRoleForKey(ev: any, key: string | null): AccessRole | null {
  if (!ev || !key) return null;
  if (key === ev.broadcast_key) return "broadcaster";
  if (key === ev.secret_key) return "viewer";
  return null;
}

async function resolveEncoderConfigurationArn(
  client: any,
  env: Env,
  eventId: string,
  endpoints: any,
  createName: string
): Promise<{
  encoderConfigurationArn: string | null;
  endpoints: any;
  source: "shared" | "stored" | "created" | "missing";
}> {
  const sharedArn = getSharedEncoderConfigurationArn(env);
  if (sharedArn) {
    const nextEndpoints = withEncoderConfigArn(endpoints, sharedArn);
    try {
      const current = getEncoderConfigArnFromEndpoints(endpoints);
      if (current !== sharedArn) {
        await updateRtcEndpoints(client, eventId, nextEndpoints);
      }
    } catch {}
    return { encoderConfigurationArn: sharedArn, endpoints: nextEndpoints, source: "shared" };
  }

  const storedArn = getEncoderConfigArnFromEndpoints(endpoints);
  if (storedArn) {
    return { encoderConfigurationArn: storedArn, endpoints, source: "stored" };
  }

  const enc = await createEncoderConfiguration(env, createName);
  const createdArn = enc?.arn || (enc as any)?.encoderConfiguration?.arn || null;
  if (createdArn) {
    const nextEndpoints = withEncoderConfigArn(endpoints, createdArn);
    await updateRtcEndpoints(client, eventId, nextEndpoints);
    return { encoderConfigurationArn: createdArn, endpoints: nextEndpoints, source: "created" };
  }

  return { encoderConfigurationArn: null, endpoints, source: "missing" };
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

async function ensureEventStartedWindow(client: any, env: Env, eventId: string, ev: any) {
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
    ev = await getEvent(client, eventId);
  }

  return { ev, startedNow, startsAtIso, expiresAtIso };
}

async function ensureRtcStage(client: any, env: Env, eventId: string, evIn: any) {
  let ev = evIn;
  let stageArn: string | null = (ev?.rtc_stage_arn as string) || null;
  let endpoints: any = ev?.rtc_stage_endpoints ?? null;

  if (stageArn) {
    return { ev, stageArn, endpoints, created: false };
  }

  const st = await createStage(env, `relay-${eventId}`);
  stageArn = st.stageArn;
  endpoints = st.endpoints || null;
  await updateRtc(client, eventId, stageArn, endpoints);
  ev = await getEvent(client, eventId);

  return {
    ev,
    stageArn: (ev?.rtc_stage_arn as string) || stageArn,
    endpoints: ev?.rtc_stage_endpoints ?? endpoints,
    created: true,
  };
}

async function ensureHlsChannel(client: any, env: Env, eventId: string, evIn: any) {
  let ev = evIn;
  if (!ev || !ev.hls_enabled) {
    return { ev, created: false };
  }

  let channelArn: string | null = (ev.ivs_channel_arn as string) || null;

  if (channelArn) {
    try {
      const ch = await getChannel(env, channelArn);
      await updateIvs(client, eventId, ch.channelArn, ch.ingestEndpoint, ch.playbackUrl, ev.ivs_stream_key_encrypted || null);
      ev = await getEvent(client, eventId);
      return { ev, created: false };
    } catch (e) {
      console.warn("ensureHlsChannel: existing channel refresh failed", eventId, e);
      channelArn = null;
    }
  }

  const ch = await createChannel(env, `relay-${eventId}`);
  await updateIvs(client, eventId, ch.channelArn, ch.ingestEndpoint, ch.playbackUrl, ev.ivs_stream_key_encrypted || null);
  ev = await getEvent(client, eventId);
  return { ev, created: true };
}

async function ensureStreamKey(client: any, env: Env, eventId: string, evIn: any) {
  let ev = evIn;
  if (!ev?.hls_enabled) return { ev, streamKeyPlaintext: null, created: false };
  if (!ev.ivs_channel_arn || !ev.ivs_ingest_endpoint || !ev.ivs_playback_url) {
    throw new Error("hls_channel_not_ready");
  }

  if (ev.ivs_stream_key_encrypted) {
    const streamKeyPlaintext = await decryptString(ev.ivs_stream_key_encrypted, env.STREAMKEY_ENC_KEY_B64);
    return { ev, streamKeyPlaintext, created: false };
  }

  const sk = await createStreamKey(env, ev.ivs_channel_arn);
  const enc = await encryptString(sk.streamKeyValue, env.STREAMKEY_ENC_KEY_B64);
  await updateIvs(client, eventId, ev.ivs_channel_arn, ev.ivs_ingest_endpoint, ev.ivs_playback_url, enc);
  ev = await getEvent(client, eventId);
  return { ev, streamKeyPlaintext: sk.streamKeyValue, created: true };
}

async function ensureHlsInfrastructure(client: any, env: Env, eventId: string, evIn: any) {
  let ev = evIn;
  if (!ev || !ev.hls_enabled) return ev;

  const rtc = await ensureRtcStage(client, env, eventId, ev);
  ev = rtc.ev;

  const ch = await ensureHlsChannel(client, env, eventId, ev);
  ev = ch.ev;

  const endpoints = ev.rtc_stage_endpoints ?? rtc.endpoints;
  const encoderResolution = await resolveEncoderConfigurationArn(client, env, eventId, endpoints, `relay-${eventId}`);
  if (encoderResolution.source === "shared") {
    console.log("ensureHlsInfrastructure: using shared encoder configuration", encoderResolution.encoderConfigurationArn);
  }

  return await getEvent(client, eventId);
}

async function ensureCompositionStarted(client: any, env: Env, eventId: string, evIn: any) {
  let ev = evIn;
  let compositionStarted = false;
  let compositionArn: string | null = null;

  if (!ev?.hls_enabled) {
    return { ev, compositionStarted, compositionArn };
  }

  ev = await ensureHlsInfrastructure(client, env, eventId, ev);

  const stageArn: string | null = (ev.rtc_stage_arn as string) || null;
  const channelArn: string | null = (ev.ivs_channel_arn as string) || null;
  let endpoints: any = ev.rtc_stage_endpoints ?? null;
  const encoderConfigurationArn = getEncoderConfigArnFromEndpoints(endpoints);

  compositionArn = getCompositionArnFromEndpoints(endpoints);
  if (compositionArn) {
    return { ev, compositionStarted: true, compositionArn };
  }

  if (!stageArn || !channelArn || !encoderConfigurationArn) {
    console.log(
      "ensureCompositionStarted: missing prerequisites",
      JSON.stringify({ stageArn, channelArn, encoderConfigurationArn })
    );
    return { ev, compositionStarted: false, compositionArn: null };
  }

  try {
    const idempotencyToken = `relay-${eventId}-hls`;
    const comp = await createComposition(env, stageArn, channelArn, encoderConfigurationArn, idempotencyToken);
    const compArn = (comp && (comp as any).arn) || (comp as any)?.composition?.arn || null;

    if (compArn) {
      compositionArn = compArn;
      endpoints = withCompositionArn(endpoints, compositionArn);
      await updateRtcEndpoints(client, eventId, endpoints);
      compositionStarted = true;
      ev = await getEvent(client, eventId);
      return { ev, compositionStarted, compositionArn };
    }
  } catch (e: any) {
    if (isCompositionConflictError(e)) {
      compositionArn = getCompositionArnFromEndpoints(endpoints) || "existing";
      compositionStarted = true;
      return { ev, compositionStarted, compositionArn };
    }
    console.error("ensureCompositionStarted: failed", eventId, e);
  }

  return { ev, compositionStarted, compositionArn };
}

async function preProvisionPaidEvent(client: any, env: Env, eventId: string) {
  let ev = await getEvent(client, eventId);
  if (!ev || ev.status !== "paid" || isExpired(ev)) return ev;

  if (ev.rtc_enabled || ev.hls_enabled) {
    try {
      ev = (await ensureRtcStage(client, env, eventId, ev)).ev;
    } catch (e) {
      console.error("preProvisionPaidEvent: stage failed", eventId, e);
    }
  }

  if (ev?.hls_enabled) {
    try {
      ev = await ensureHlsInfrastructure(client, env, eventId, ev);
    } catch (e) {
      console.error("preProvisionPaidEvent: hls infra failed", eventId, e);
    }
  }

  return await getEvent(client, eventId);
}

function buildReadiness(ev: any, role: AccessRole): Readiness {
  const endpoints = ev?.rtc_stage_endpoints ?? null;
  const stageExists = !!ev?.rtc_stage_arn;
  const hlsEnabled = !!ev?.hls_enabled;
  const rtcEnabled = !!ev?.rtc_enabled;
  const channelExists = !!ev?.ivs_channel_arn && !!ev?.ivs_ingest_endpoint && !!ev?.ivs_playback_url;
  const encoderExists = !!getEncoderConfigArnFromEndpoints(endpoints);
  const compositionStarted = !!getCompositionArnFromEndpoints(endpoints);
  const playbackUrlExists = !!ev?.ivs_playback_url;
  const whipUrlExists = !!endpoints?.whip;
  const paid = ev?.status === "paid";
  const expired = isExpired(ev);
  const streamWindowOpen = !!ev?.starts_at && !expired;
  const canIssueBroadcasterToken = paid && !expired && stageExists && (rtcEnabled || hlsEnabled);
  const canIssueViewerToken = paid && !expired && rtcEnabled && stageExists;
  const canGoLive = role === "broadcaster" && canIssueBroadcasterToken;
  const canWatchHls = paid && !expired && hlsEnabled && playbackUrlExists;
  const canWatchRtc = paid && !expired && rtcEnabled && stageExists;

  let state = "preparing";
  let detail = "Preparing stream infrastructure.";

  if (!paid) {
    state = "awaiting_payment";
    detail = "Waiting for payment confirmation.";
  } else if (expired) {
    state = "expired";
    detail = "This event has expired.";
  } else if (!stageExists && (rtcEnabled || hlsEnabled)) {
    state = "preparing_stage";
    detail = "Creating RTC stage.";
  } else if (hlsEnabled && !channelExists) {
    state = "preparing_hls_channel";
    detail = "Creating HLS channel.";
  } else if (hlsEnabled && !encoderExists) {
    state = "preparing_encoder";
    detail = "Resolving encoder configuration.";
  } else if (role === "broadcaster" && canGoLive) {
    state = streamWindowOpen ? "live_window_open" : "ready_to_go_live";
    detail = streamWindowOpen ? "Broadcast window is open." : "Ready to go live.";
  } else if (role === "viewer" && hlsEnabled && compositionStarted) {
    state = "stream_detected";
    detail = "Stream path detected. Waiting for media.";
  } else if (role === "viewer" && (canWatchHls || canWatchRtc)) {
    state = "waiting_for_stream";
    detail = "Waiting for broadcaster to publish media.";
  }

  return {
    role,
    paid,
    expired,
    stage_exists: stageExists,
    hls_enabled: hlsEnabled,
    rtc_enabled: rtcEnabled,
    hls_channel_exists: channelExists,
    encoder_configuration_exists: encoderExists,
    composition_started: compositionStarted,
    playback_url_exists: playbackUrlExists,
    whip_url_exists: whipUrlExists,
    stream_window_open: streamWindowOpen,
    can_issue_broadcaster_token: canIssueBroadcasterToken,
    can_issue_viewer_token: canIssueViewerToken,
    can_go_live: canGoLive,
    can_watch_hls: canWatchHls,
    can_watch_rtc: canWatchRtc,
    state,
    detail,
    playback_url: ev?.ivs_playback_url || null,
    expires_at: ev?.expires_at || null,
    starts_at: ev?.starts_at || null,
  };
}

async function maybeRefreshPaidStatus(client: any, env: Env, ev: any) {
  if (!ev) return ev;
  if (ev.status === "paid" || !ev.stripe_session_id) return ev;

  try {
    const stripe = stripeClient(env);
    const sess = await stripe.checkout.sessions.retrieve(ev.stripe_session_id);
    if (sess.payment_status === "paid") {
      await markPaid(client, ev.id);
      return await getEvent(client, ev.id);
    }
  } catch (e) {
    console.error("maybeRefreshPaidStatus failed", ev.id, e);
  }
  return ev;
}

export default {
  async fetch(request: Request, env: Env) {
    const url = new URL(request.url);
    const { pathname } = url;
    const method = request.method.toUpperCase();

    if (method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders(env) });
    }

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
              await preProvisionPaidEvent(client, env, eventId);
            } finally {
              await client.end();
            }
          }
        }

        return json(env, { ok: true }, 200);
      }

      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/readiness$/);
        if (method === "GET" && m) {
          const eventId = m[0];
          const key = url.searchParams.get("key") || "";
          const client = await getClient(env);
          try {
            let ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);

            const role = accessRoleForKey(ev, key);
            if (!role) return json(env, { error: "unauthorized" }, 401);

            ev = await maybeRefreshPaidStatus(client, env, ev);
            if (ev.status === "paid" && !isExpired(ev) && (ev.rtc_enabled || ev.hls_enabled)) {
              ev = await preProvisionPaidEvent(client, env, eventId);
            }

            const readiness = buildReadiness(ev, role);
            return json(env, {
              ok: true,
              id: ev.id,
              title: ev.title,
              status: ev.status,
              white_label: !!ev.white_label,
              readiness,
            });
          } finally {
            await client.end();
          }
        }
      }

      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/start$/);
        if (method === "POST" && m) {
          const eventId = m[0];
          const key = url.searchParams.get("key") || "";
          const body = await request.json().catch(() => ({}));
          const openWindow = body?.open_window !== false;

          const client = await getClient(env);
          try {
            let ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);

            const role = accessRoleForKey(ev, key);
            if (!role) return json(env, { error: "unauthorized" }, 401);
            if (ev.status !== "paid") return json(env, { error: "not_paid" }, 403);
            if (isExpired(ev)) return json(env, { error: "expired" }, 410);

            ev = await preProvisionPaidEvent(client, env, eventId);

            let startedWindow = { ev, startedNow: false, startsAtIso: ev.starts_at || null, expiresAtIso: ev.expires_at || null };
            if (openWindow) {
              startedWindow = await ensureEventStartedWindow(client, env, eventId, ev);
              ev = startedWindow.ev;
            }

            const hlsResult = await ensureCompositionStarted(client, env, eventId, ev);
            ev = hlsResult.ev;

            const readiness = buildReadiness(ev, role);

            return json(env, {
              ok: true,
              started: startedWindow.startedNow,
              start_mode: openWindow ? "window_opened" : "warm_only",
              starts_at: startedWindow.startsAtIso,
              expires_at: startedWindow.expiresAtIso,
              playback_url: ev.ivs_playback_url || null,
              composition_started: hlsResult.compositionStarted,
              composition_arn: hlsResult.compositionArn,
              readiness,
            });
          } finally {
            await client.end();
          }
        }
      }

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

            if (compositionArn && compositionArn !== "existing") {
              try {
                await stopComposition(env, compositionArn);
              } catch (e) {
                console.error("StopComposition failed", eventId, e);
              }

              try {
                await updateRtcEndpoints(client, eventId, withoutCompositionArn(endpoints));
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

      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/links$/);
        if (method === "GET" && m) {
          const eventId = m[0];
          const st = url.searchParams.get("st");

          const client = await getClient(env);
          try {
            let ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);

            const stHash = st ? await sha256Hex(st) : "";
            if (!st || stHash !== ev.success_token_hash) return json(env, { error: "unauthorized" }, 401);

            ev = await maybeRefreshPaidStatus(client, env, ev);
            if (ev.status === "paid" && !isExpired(ev) && (ev.rtc_enabled || ev.hls_enabled)) {
              ev = await preProvisionPaidEvent(client, env, eventId);
            }

            return json(env, {
              ok: true,
              watch_url: `${env.APP_ORIGIN}/watch?event=${encodeURIComponent(eventId)}&key=${encodeURIComponent(ev.secret_key)}`,
              broadcast_url: `${env.APP_ORIGIN}/broadcast?event=${encodeURIComponent(eventId)}&key=${encodeURIComponent(ev.broadcast_key)}`,
              rtc_enabled: !!ev.rtc_enabled,
              hls_enabled: !!ev.hls_enabled,
              playback_url: ev.ivs_playback_url || null,
            });
          } finally {
            await client.end();
          }
        }
      }

      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/public$/);
        if (method === "GET" && m) {
          const eventId = m[0];
          const key = url.searchParams.get("key") || "";

          const client = await getClient(env);
          try {
            let ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);

            const role = accessRoleForKey(ev, key);
            if (!role) return json(env, { error: "unauthorized" }, 401);

            ev = await maybeRefreshPaidStatus(client, env, ev);
            if (ev.status === "paid" && !isExpired(ev) && (ev.rtc_enabled || ev.hls_enabled)) {
              ev = await preProvisionPaidEvent(client, env, eventId);
            }

            const readiness = buildReadiness(ev, role);

            return json(env, {
              ok: true,
              id: ev.id,
              title: ev.title,
              status: ev.status,
              expired: isExpired(ev),
              starts_at: ev.starts_at || null,
              expires_at: ev.expires_at || null,
              playback_url: ev.ivs_playback_url || null,
              rtc_stage_arn: role === "broadcaster" ? ev.rtc_stage_arn || null : null,
              rtc_enabled: !!ev.rtc_enabled,
              hls_enabled: !!ev.hls_enabled,
              white_label: !!ev.white_label,
              readiness,
            });
          } finally {
            await client.end();
          }
        }
      }

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

      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/hls\/provision$/);
        if (method === "POST" && m) {
          const eventId = m[0];
          const key = url.searchParams.get("key") || "";

          const client = await getClient(env);
          try {
            let ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);
            if (ev.status !== "paid") return json(env, { error: "not_paid" }, 400);
            if (isExpired(ev)) return json(env, { error: "expired" }, 410);
            if (!ev.hls_enabled) return json(env, { error: "hls_disabled" }, 400);

            const authRes = requireExact(key, ev.broadcast_key, env);
            if (authRes) return authRes;

            ev = await ensureHlsInfrastructure(client, env, eventId, ev);
            const keyResult = await ensureStreamKey(client, env, eventId, ev);
            ev = keyResult.ev;

            const host = ingestHostFromDb(ev.ivs_ingest_endpoint);
            return json(env, {
              ok: true,
              alreadyProvisioned: !keyResult.created,
              ingest: { rtmpsUrl: rtmpsUrlFromHost(host), streamKey: keyResult.streamKeyPlaintext },
              playback: { url: ev.ivs_playback_url },
              readiness: buildReadiness(ev, "broadcaster"),
            });
          } finally {
            await client.end();
          }
        }
      }

      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/rtc\/(provision|host)$/);
        if (method === "POST" && m) {
          const eventId = m[0];
          const key = url.searchParams.get("key") || "";

          const client = await getClient(env);
          try {
            let ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);
            if (ev.status !== "paid") return json(env, { error: "not_paid" }, 400);
            if (isExpired(ev)) return json(env, { error: "expired" }, 410);
            if (!(ev.rtc_enabled || ev.hls_enabled)) return json(env, { error: "rtc_ingest_disabled" }, 400);

            const authRes = requireExact(key, ev.broadcast_key, env);
            if (authRes) return authRes;

            const rtc = await ensureRtcStage(client, env, eventId, ev);
            ev = rtc.ev;

            const token = await createParticipantToken(
              env,
              rtc.stageArn!,
              `host-${eventId}`,
              ["PUBLISH", "SUBSCRIBE"],
              3600
            );

            return json(env, {
              ok: true,
              stageArn: rtc.stageArn,
              participantToken: token.token,
              endpoints: rtc.endpoints,
              readiness: buildReadiness(ev, "broadcaster"),
            });
          } finally {
            await client.end();
          }
        }
      }

      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/whip\/provision$/);
        if (method === "POST" && m) {
          const eventId = m[0];
          const key = url.searchParams.get("key") || "";

          const client = await getClient(env);
          try {
            let ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);
            if (ev.status !== "paid") return json(env, { error: "not_paid" }, 400);
            if (isExpired(ev)) return json(env, { error: "expired" }, 410);
            if (!(ev.rtc_enabled || ev.hls_enabled)) return json(env, { error: "rtc_ingest_disabled" }, 400);

            const authRes = requireExact(key, ev.broadcast_key, env);
            if (authRes) return authRes;

            const rtc = await ensureRtcStage(client, env, eventId, ev);
            ev = rtc.ev;
            const whipUrl = rtc.endpoints?.whip || null;
            if (!whipUrl) return json(env, { error: "whip_not_available", endpoints: rtc.endpoints }, 500);

            const token = await createParticipantToken(
              env,
              rtc.stageArn!,
              `obs-${eventId}`,
              ["PUBLISH"],
              3600
            );

            return json(env, {
              ok: true,
              publish_mode: "whip",
              event_id: eventId,
              whip_url: whipUrl,
              bearer_token: token.token,
              expires_at: ev.expires_at || null,
              readiness: buildReadiness(ev, "broadcaster"),
            });
          } finally {
            await client.end();
          }
        }
      }

      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/rtc\/token$/);
        if (method === "POST" && m) {
          const eventId = m[0];
          const key = url.searchParams.get("key") || "";

          const client = await getClient(env);
          try {
            let ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);
            if (ev.status !== "paid") return json(env, { error: "not_paid" }, 400);
            if (isExpired(ev)) return json(env, { error: "expired" }, 410);
            if (!ev.rtc_enabled) return json(env, { error: "rtc_disabled" }, 400);

            const authRes = requireExact(key, ev.secret_key, env);
            if (authRes) return authRes;

            const rtc = await ensureRtcStage(client, env, eventId, ev);
            ev = rtc.ev;

            const token = await createParticipantToken(
              env,
              rtc.stageArn as string,
              `viewer-${randomSecretUrlSafe(8)}`,
              ["SUBSCRIBE"],
              3600
            );

            return json(env, {
              ok: true,
              stageArn: rtc.stageArn,
              participantToken: token.token,
              readiness: buildReadiness(ev, "viewer"),
            });
          } finally {
            await client.end();
          }
        }
      }

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
              const compositionArn = getCompositionArnFromEndpoints(ev.rtc_stage_endpoints ?? null);
              if (compositionArn && compositionArn !== "existing") await stopComposition(env, compositionArn);
            } catch (e) {
              console.error("stopComposition failed", eventId, e);
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

            const token = await createParticipantToken(
              env,
              stageArn,
              `host-${streamName}`,
              ["PUBLISH", "SUBSCRIBE"],
              3600
            );

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

export async function scheduled(_event: ScheduledEvent, _env: any, _ctx: ExecutionContext) {
  console.log("cron tick", new Date().toISOString());
}
