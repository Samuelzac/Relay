import { getClient } from "./db";
import { randomSecretUrlSafe, sha256Hex, encryptString, decryptString } from "./crypto";
import { SeatsDO } from "./seats_do";
import { BroadcastLockDO } from "./broadcast_lock_do";
import { stripeClient, priceForTierAndMode, hoursForTier, StreamMode, normalizeMode } from "./stripe";
import { createChannel, getChannel } from "./awsIvs";
import { deleteIvsChannel } from "./ivs";
import {
  createStage,
  createParticipantToken,
  deleteStage,
  createEncoderConfiguration,
  deleteEncoderConfiguration,
  createComposition,
  stopComposition,
} from "./awsIvsRealtime";

export { SeatsDO, BroadcastLockDO };

type Env = any;
type AccessRole = "viewer" | "broadcaster";
type InventoryMode = "rtc" | "hls" | "both";

type Readiness = {
  role: AccessRole;
  paid: boolean;
  disabled: boolean;
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
  seconds_remaining: number | null;
  warning: string | null;
};

type InventorySlot = {
  id: string;
  mode: InventoryMode;
  status: string;
  assigned_event_id?: string | null;
  ivs_channel_arn?: string | null;
  ivs_ingest_endpoint?: string | null;
  ivs_playback_url?: string | null;
  ivs_stream_key_encrypted?: string | null;
  rtc_stage_arn?: string | null;
  rtc_stage_endpoints?: any;
};

const JSON_HEADERS = {
  "content-type": "application/json; charset=utf-8",
  "cache-control": "no-store",
};

const inventoryFillBackoffUntil: Record<string, number> = {};
let readyEmailColumnsEnsured = false;

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

function withCors(env: Env, response: Response) {
  const headers = new Headers(response.headers);
  for (const [key, value] of Object.entries(corsHeaders(env))) headers.set(key, value);
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

function htmlEscape(value: any) {
  return String(value ?? "").replace(/[&<>"']/g, (c) => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;" } as any)[c]);
}

function emailButton(label: string, href: string) {
  return `<a href="${htmlEscape(href)}" style="display:inline-block;background:#111827;color:#fff;text-decoration:none;padding:11px 16px;border-radius:6px;font-weight:700">${htmlEscape(label)}</a>`;
}

function emailShell(brand: string, heading: string, body: string) {
  return `
    <div style="font-family:Arial,sans-serif;line-height:1.55;color:#111;background:#f8fafc;padding:24px">
      <div style="max-width:620px;margin:0 auto;background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:24px">
        <div style="font-size:14px;font-weight:700;color:#111827;margin-bottom:18px">${htmlEscape(brand)}</div>
        <h2 style="margin:0 0 14px;font-size:22px;line-height:1.25;color:#111827">${htmlEscape(heading)}</h2>
        ${body}
        <p style="margin:22px 0 0;color:#6b7280;font-size:13px">Keep private broadcast links and OBS tokens secure. Anyone with broadcaster access may be able to control the stream.</p>
      </div>
    </div>
  `;
}

function emailProvider(env: Env) {
  return String(env.EMAIL_PROVIDER || (env.POSTMARK_SERVER_TOKEN ? "postmark" : "resend")).toLowerCase();
}

function emailConfigured(env: Env) {
  const provider = emailProvider(env);
  if (!env.EMAIL_FROM) return false;
  if (provider === "postmark") return !!env.POSTMARK_SERVER_TOKEN;
  return !!env.RESEND_API_KEY;
}

async function sendEmail(env: Env, opts: { to: string; subject: string; html: string; text: string; tag?: string }) {
  if (!emailConfigured(env)) {
    console.log("email skipped: provider token/EMAIL_FROM not configured", opts.subject);
    return { skipped: true };
  }

  if (emailProvider(env) === "postmark") {
    const res = await fetch("https://api.postmarkapp.com/email", {
      method: "POST",
      headers: {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Postmark-Server-Token": env.POSTMARK_SERVER_TOKEN,
      },
      body: JSON.stringify({
        From: env.EMAIL_FROM,
        To: opts.to,
        Subject: opts.subject,
        HtmlBody: opts.html,
        TextBody: opts.text,
        Tag: opts.tag || undefined,
        MessageStream: env.POSTMARK_MESSAGE_STREAM || "outbound",
      }),
    });

    const body = await res.text();
    let data: any = null;
    try { data = body ? JSON.parse(body) : null; } catch {}
    if (!res.ok || (data && Number(data.ErrorCode || 0) !== 0)) {
      throw new Error(`postmark_send_failed ${res.status} ${body}`);
    }
    return data || { ok: true };
  }

  const res = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${env.RESEND_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      from: env.EMAIL_FROM,
      to: [opts.to],
      subject: opts.subject,
      html: opts.html,
      text: opts.text,
      tags: opts.tag ? [{ name: "kind", value: opts.tag }] : undefined,
    }),
  });

  const body = await res.text();
  let data: any = null;
  try { data = body ? JSON.parse(body) : null; } catch {}
  if (!res.ok) throw new Error(`email_send_failed ${res.status} ${body}`);
  return data || { ok: true };
}

function appName(env: Env) {
  return String(env.BRAND_NAME || "Castlink");
}

function slugifyEventTitle(value: any) {
  const slug = String(value || "event")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
  return slug || "event";
}

function emailSubjectEventLabel(ev: any) {
  const title = String(ev?.title || "Untitled event").trim().replace(/\s+/g, " ").slice(0, 80);
  const id = String(ev?.id || "").replace(/-/g, "").slice(0, 8);
  return id ? `${title} #${id}` : title;
}

function eventLinks(env: Env, ev: any) {
  const slug = slugifyEventTitle(ev?.title);
  const eventHash = slug ? `#${encodeURIComponent(slug)}` : "";
  return {
    watchUrl: `${env.APP_ORIGIN}/watch/?event=${encodeURIComponent(ev.id)}&key=${encodeURIComponent(ev.secret_key)}${eventHash}`,
    broadcastUrl: `${env.APP_ORIGIN}/broadcast/?event=${encodeURIComponent(ev.id)}&key=${encodeURIComponent(ev.broadcast_key)}${eventHash}`,
  };
}

function apiOrigin(env: Env) {
  return String(env.API_ORIGIN || "https://api.castlink.stream").replace(/\/+$/, "");
}

function extensionCheckoutUrl(env: Env, ev: any) {
  return `${apiOrigin(env)}/api/events/${encodeURIComponent(ev.id)}/extend/checkout?key=${encodeURIComponent(ev.broadcast_key)}`;
}

function normalizeEmailList(value: any) {
  const source = Array.isArray(value) ? value.join(",") : String(value || "");
  const seen = new Set<string>();
  const emails: string[] = [];
  for (const part of source.split(/[\s,;]+/)) {
    const email = part.trim().toLowerCase();
    if (!email || seen.has(email)) continue;
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) continue;
    seen.add(email);
    emails.push(email);
    if (emails.length >= 50) break;
  }
  return emails;
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

function isDisabled(ev: any) {
  return !!ev?.disabled;
}

function isTestEvent(ev: any) {
  return !!ev?.is_test;
}

function testStreamMinutes(env: Env) {
  const raw = Number(env.TEST_STREAM_MINUTES ?? 3);
  return Number.isFinite(raw) && raw > 0 ? Math.min(raw, 10) : 3;
}

function testUnusedExpiryMinutes(env: Env) {
  const raw = Number(env.TEST_UNUSED_EXPIRY_MINUTES ?? 15);
  return Number.isFinite(raw) && raw > 0 ? Math.min(raw, 60) : 15;
}

function testViewerLimit(env: Env) {
  const raw = Number(env.TEST_VIEWER_LIMIT ?? 3);
  return Number.isFinite(raw) && raw > 0 ? Math.min(raw, 10) : 3;
}

function testDailyLimit(env: Env) {
  const raw = Number(env.TEST_STREAMS_PER_EMAIL_PER_DAY ?? 2);
  return Number.isFinite(raw) && raw > 0 ? Math.min(raw, 10) : 2;
}

function requestIp(request: Request) {
  return (
    request.headers.get("cf-connecting-ip") ||
    request.headers.get("x-forwarded-for") ||
    ""
  ).split(",")[0].trim();
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

function withoutCleanupOnlyEndpointState(endpoints: any, removeEncoderConfiguration: boolean) {
  const base = withoutCompositionArn(endpoints);
  if (removeEncoderConfiguration) {
    delete (base as any).encoderConfigurationArn;
    delete (base as any).encoder_configuration_arn;
  }
  return Object.keys(base).length ? base : null;
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
  if (!readyEmailColumnsEnsured) {
    await ensureReadyEmailColumns(client);
    readyEmailColumnsEnsured = true;
  }
  const { rows } = await client.query(
    `
    select
      id, email, title, tier, viewer_limit, white_label,
      status, starts_at, expires_at, created_at,
      ivs_channel_arn, ivs_ingest_endpoint, ivs_playback_url, ivs_stream_key_encrypted,
      secret_key, broadcast_key, success_token_hash, stripe_session_id,
      rtc_stage_arn, rtc_stage_endpoints, rtc_enabled, hls_enabled, disabled,
      cleanup_started_at, cleanup_completed_at, cleanup_attempts, cleanup_error,
      warning_email_sent_at, warning_email_error,
      ready_email_sent_at, ready_email_error,
      viewer_recipient_emails, viewer_invites_sent_at, viewer_invites_error,
      is_test, test_created_ip, test_expires_unused_at
    from public.events
    where id = $1
  `,
    [id]
  );
  return rows[0] || null;
}

async function withAdvisoryLock<T>(client: any, key: string, fn: () => Promise<T>): Promise<T> {
  await client.query(`select pg_advisory_lock(hashtext($1))`, [key]);
  try {
    return await fn();
  } finally {
    await client.query(`select pg_advisory_unlock(hashtext($1))`, [key]).catch(() => {});
  }
}

function streamWarningMinutes(env: Env) {
  const raw = Number(env.STREAM_WARNING_MINUTES ?? 10);
  return Number.isFinite(raw) && raw > 0 ? raw : 10;
}

function paidUnusedExpiryDays(env: Env) {
  const raw = Number(env.PAID_UNUSED_EXPIRY_DAYS ?? 7);
  return Number.isFinite(raw) && raw > 0 ? Math.min(raw, 90) : 7;
}

function formatNzMoney(value: number) {
  return `$${Number(value || 0).toFixed(2).replace(/\.00$/, "")} NZD`;
}

function shouldDeleteIvsOnExpire(env: Env) {
  return String(env.DELETE_IVS_ON_EXPIRE || "").toLowerCase() === "true";
}

function isAwsAlreadyGoneError(err: any): boolean {
  const msg = String(err?.message || err || "");
  return (
    msg.includes("ResourceNotFoundException") ||
    msg.includes("NotFoundException") ||
    msg.includes("404") ||
    msg.includes("not found")
  );
}

function isAwsStreamKeyQuotaError(err: any): boolean {
  const msg = String(err?.message || err || "");
  return msg.includes("stream-key quota exceeded") || msg.includes("ServiceQuotaExceededException");
}

async function bestEffortCleanupStep(label: string, eventId: string, fn: () => Promise<void>, errors: string[]) {
  try {
    await fn();
    return true;
  } catch (e: any) {
    if (isAwsAlreadyGoneError(e)) {
      console.log(`cleanup: ${label} already gone`, eventId, e?.message || String(e));
      return true;
    }
    const msg = `${label}: ${e?.message || String(e)}`;
    errors.push(msg);
    console.error(`cleanup: ${label} failed`, eventId, e);
    return false;
  }
}

async function cleanupEventResources(client: any, env: Env, ev: any, reason: string) {
  const eventId = ev.id;
  const deleteResources = shouldDeleteIvsOnExpire(env);
  const errors: string[] = [];
  const endpoints = ev.rtc_stage_endpoints ?? null;
  const compositionArn = getCompositionArnFromEndpoints(endpoints);
  const encoderConfigurationArn = getEncoderConfigArnFromEndpoints(endpoints);
  const sharedEncoderConfigurationArn = getSharedEncoderConfigurationArn(env);
  let encoderConfigurationDeleted = !encoderConfigurationArn || encoderConfigurationArn === sharedEncoderConfigurationArn;

  await client.query(
    `
    update public.events
    set cleanup_started_at = coalesce(cleanup_started_at, now()),
        cleanup_attempts = cleanup_attempts + 1,
        cleanup_error = null
    where id=$1
  `,
    [eventId]
  );

  if (compositionArn && compositionArn !== "existing") {
    const stopped = await bestEffortCleanupStep("stopComposition", eventId, () => stopComposition(env, compositionArn), errors);
    if (stopped) {
      await updateRtcEndpoints(client, eventId, withoutCompositionArn(endpoints));
    }
  } else if (compositionArn === "existing") {
    await updateRtcEndpoints(client, eventId, withoutCompositionArn(endpoints));
  }

  if (deleteResources) {
    if (encoderConfigurationArn && encoderConfigurationArn !== sharedEncoderConfigurationArn) {
      encoderConfigurationDeleted = await bestEffortCleanupStep(
        "deleteEncoderConfiguration",
        eventId,
        () => deleteEncoderConfiguration(env, encoderConfigurationArn),
        errors
      );
    }

    if (ev.rtc_stage_arn) {
      const deleted = await bestEffortCleanupStep("deleteStage", eventId, () => deleteStage(env, ev.rtc_stage_arn), errors);
      if (deleted) {
        await client.query(`update public.events set rtc_stage_arn=null, rtc_stage_endpoints=$2::jsonb where id=$1`, [
          eventId,
          JSON.stringify(withoutCleanupOnlyEndpointState(endpoints, encoderConfigurationDeleted)),
        ]);
      }
    }

    if (ev.ivs_channel_arn) {
      const deleted = await bestEffortCleanupStep("deleteIvsChannel", eventId, () => deleteIvsChannel(env, ev.ivs_channel_arn), errors);
      if (deleted) {
        await client.query(
          `
          update public.events
          set ivs_channel_arn=null,
              ivs_ingest_endpoint=null,
              ivs_playback_url=null,
              ivs_stream_key_encrypted=null
          where id=$1
        `,
          [eventId]
        );
      }
    }
  }

  if (errors.length) {
    const error = errors.join(" | ").slice(0, 2000);
    await client.query(`update public.events set cleanup_error=$2 where id=$1`, [eventId, error]);
    return { ok: false, eventId, reason, delete_resources: deleteResources, errors };
  }

  await client.query(
    `
    update public.events
    set cleanup_completed_at=now(),
        cleanup_error=null
    where id=$1
  `,
    [eventId]
  );

  return { ok: true, eventId, reason, delete_resources: deleteResources };
}

async function retireAssignedInventoryForEvent(client: any, eventId: string, reason: string) {
  const { rows } = await client.query(
    `
    update public.stream_inventory
    set status='retired',
        retired_at=coalesce(retired_at, now()),
        error=coalesce(error, $2)
    where assigned_event_id=$1
      and status='assigned'
    returning id, mode, status
  `,
    [eventId, reason]
  );
  return rows;
}

async function finishEventForAdmin(client: any, env: Env, eventId: string, reason: string) {
  const disable = reason === "admin_disable";
  await client.query(
    `
    update public.events
    set status='expired',
        expires_at=now(),
        disabled = case when $2 then true else disabled end
    where id=$1
  `,
    [eventId, disable]
  );

  let ev = await getEvent(client, eventId);
  const cleanup = await cleanupEventResources(client, env, ev, reason);
  const retiredInventory = cleanup.ok ? await retireAssignedInventoryForEvent(client, eventId, `retired after ${reason}`) : [];
  ev = await getEvent(client, eventId);
  const usage = await usageSummary(client, env, eventId);
  return { ev, cleanup, usage, retiredInventory };
}

async function adminEventPayload(client: any, env: Env, ev: any) {
  const usage = await usageSummary(client, env, ev.id);
  const links = eventLinks(env, ev);
  return {
    id: ev.id,
    email: ev.email,
    title: ev.title,
    tier: ev.tier,
    viewer_limit: ev.viewer_limit,
    white_label: !!ev.white_label,
    is_test: !!ev.is_test,
    disabled: !!ev.disabled,
    status: ev.status,
    starts_at: ev.starts_at || null,
    expires_at: ev.expires_at || null,
    created_at: ev.created_at,
    rtc_enabled: !!ev.rtc_enabled,
    hls_enabled: !!ev.hls_enabled,
    playback_url: ev.ivs_playback_url || null,
    watch_url: links.watchUrl,
    broadcast_url: links.broadcastUrl,
    cleanup_started_at: ev.cleanup_started_at || null,
    cleanup_completed_at: ev.cleanup_completed_at || null,
    cleanup_attempts: Number(ev.cleanup_attempts || 0),
    cleanup_error: ev.cleanup_error || null,
    warning_email_sent_at: ev.warning_email_sent_at || null,
    warning_email_error: ev.warning_email_error || null,
    ready_email_sent_at: ev.ready_email_sent_at || null,
    ready_email_error: ev.ready_email_error || null,
    viewer_recipient_emails: ev.viewer_recipient_emails || [],
    viewer_invites_sent_at: ev.viewer_invites_sent_at || null,
    viewer_invites_error: ev.viewer_invites_error || null,
    readiness: buildReadiness(ev, "broadcaster", env),
    usage,
  };
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

async function clearIvs(client: any, id: string) {
  await client.query(
    `
    update public.events
    set ivs_channel_arn=null,
        ivs_ingest_endpoint=null,
        ivs_playback_url=null,
        ivs_stream_key_encrypted=null
    where id=$1
  `,
    [id]
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

function priceCentsFor(env: Env, tier: number, mode: StreamMode) {
  return dollarsToCents(priceForTierAndMode(env, tier, mode));
}

function includedUsageFor(env: Env, tier: number, mode: StreamMode) {
  if (mode === "hls") {
    const key = tier === 1 ? "HLS_INCLUDED_VIEWER_HOURS_1H" : tier === 8 ? "HLS_INCLUDED_VIEWER_HOURS_EXTENDED" : "HLS_INCLUDED_VIEWER_HOURS_STANDARD";
    return { hls_viewer_hours: Number(env[key] ?? 0) };
  }
  if (mode === "rtc") {
    const key = tier === 1 ? "WEBRTC_INCLUDED_PARTICIPANT_HOURS_1H" : tier === 2 ? "WEBRTC_INCLUDED_PARTICIPANT_HOURS_2H" : "WEBRTC_INCLUDED_PARTICIPANT_HOURS_STANDARD";
    return { rtc_participant_hours: Number(env[key] ?? 0) };
  }
  const suffix = tier === 8 ? "EXTENDED" : "STANDARD";
  return {
    hls_viewer_hours: Number(env[`BOTH_INCLUDED_HLS_VIEWER_HOURS_${suffix}`] ?? 0),
    rtc_participant_hours: Number(env[`BOTH_INCLUDED_RTC_PARTICIPANT_HOURS_${suffix}`] ?? 0),
  };
}

function allowedTiersForMode(mode: StreamMode) {
  if (mode === "hls") return [1, 3, 8];
  if (mode === "rtc") return [1, 2];
  return [3, 8];
}

function tierIsAllowedForMode(tier: number, mode: StreamMode) {
  return allowedTiersForMode(mode).includes(tier);
}

function extensionOneHourPrice(env: Env) {
  return Number(env.EXTENSION_1H_PRICE_NZD ?? 49);
}

function streamWarningLeadMinutes(env: Env) {
  const target = streamWarningMinutes(env);
  const interval = Math.max(1, Number(env.STREAM_WARNING_CRON_INTERVAL_MINUTES ?? 5));
  return target + interval;
}

function viewerUpgradePrice(env: Env, amount: number) {
  const key = `VIEWER_UPGRADE_${amount}_NZD`;
  return Number(env[key] ?? (amount === 100 ? 39 : amount === 250 ? 79 : amount === 500 ? 149 : 0));
}

function allowedViewerUpgrade(amount: number) {
  return [100, 250, 500].includes(amount);
}

function viewerCapWarningPercent(env: Env) {
  const raw = Number(env.VIEWER_CAP_WARNING_PERCENT ?? 90);
  return Number.isFinite(raw) && raw > 0 && raw < 100 ? raw : 90;
}

function eventMode(ev: any): StreamMode {
  if (ev?.rtc_enabled && ev?.hls_enabled) return "both";
  if (ev?.hls_enabled) return "hls";
  return "rtc";
}

function broadcastSeconds(ev: any) {
  if (!ev?.starts_at) return 0;
  const start = new Date(ev.starts_at).getTime();
  const end = ev.expires_at ? Math.min(new Date(ev.expires_at).getTime(), Date.now()) : Date.now();
  return Math.max(0, Math.ceil((end - start) / 1000));
}

async function seatStats(env: Env, eventId: string, viewerLimit: number) {
  const id = env.SEATS.idFromName(`seats:${eventId}`);
  const stub = env.SEATS.get(id);
  const u = new URL("https://relay.internal/seats/stats");
  u.searchParams.set("limit", String(viewerLimit || env.DEFAULT_VIEWER_LIMIT || 150));
  const r = await stub.fetch(new Request(u.toString(), { method: "GET" }));
  return await r.json().catch(() => ({})) as any;
}

async function usageSummary(client: any, env: Env, eventId: string) {
  const ev = await getEvent(client, eventId);
  if (!ev) return null;

  const mode = eventMode(ev);
  const viewerLimit = Number(ev.viewer_limit || env.DEFAULT_VIEWER_LIMIT || 150);
  const stats = await seatStats(env, eventId, viewerLimit);
  const viewerSeconds = Number(stats.total_viewer_seconds || 0);
  const bSeconds = broadcastSeconds(ev);
  const included = includedUsageFor(env, Number(ev.tier), mode);
  const overage = {
    hls_viewer_hour_nzd: Number(env.HLS_OVERAGE_VIEWER_HOUR_NZD ?? 0),
    rtc_participant_hour_nzd: Number(env.WEBRTC_OVERAGE_PARTICIPANT_HOUR_NZD ?? 0),
    extra_stream_minute_nzd: Number(env.EXTRA_STREAM_MINUTE_NZD ?? 0),
  };

  const viewerHours = viewerSeconds / 3600;
  const participantHours = viewerSeconds / 3600;
  const includedViewerHours = Number((included as any).hls_viewer_hours || 0);
  const includedParticipantHours = Number((included as any).rtc_participant_hours || 0);
  const includedStreamSeconds = hoursForTier(env, Number(ev.tier)) * 3600;

  const extraStreamMinutes = Math.max(0, Math.ceil((bSeconds - includedStreamSeconds) / 60));
  const extraViewerHours = mode !== "rtc" ? Math.max(0, viewerHours - includedViewerHours) : 0;
  const extraParticipantHours = mode !== "hls" ? Math.max(0, participantHours - includedParticipantHours) : 0;

  const overageAmount =
    extraStreamMinutes * overage.extra_stream_minute_nzd +
    extraViewerHours * overage.hls_viewer_hour_nzd +
    extraParticipantHours * overage.rtc_participant_hour_nzd;

  const activeViewers = Number(stats.active || 0);
  const peakViewers = Number(stats.peak_active || 0);
  const capPercent = viewerLimit > 0 ? Math.round((activeViewers / viewerLimit) * 100) : 0;
  const capWarningPercent = viewerCapWarningPercent(env);

  return {
    event_id: eventId,
    mode,
    tier: Number(ev.tier),
    starts_at: ev.starts_at || null,
    expires_at: ev.expires_at || null,
    broadcast_seconds: bSeconds,
    broadcast_minutes: Math.ceil(bSeconds / 60),
    viewer_seconds: viewerSeconds,
    viewer_hours: Number(viewerHours.toFixed(3)),
    peak_viewers: peakViewers,
    active_viewers: activeViewers,
    viewer_limit: viewerLimit,
    viewer_cap_percent: capPercent,
    viewer_cap_warning_percent: capWarningPercent,
    viewer_cap_warning: viewerLimit > 0 && capPercent >= capWarningPercent,
    viewer_cap_full: viewerLimit > 0 && activeViewers >= viewerLimit,
    included,
    overage_rates: overage,
    extra_stream_minutes: extraStreamMinutes,
    extra_hls_viewer_hours: Number(extraViewerHours.toFixed(3)),
    extra_rtc_participant_hours: Number(extraParticipantHours.toFixed(3)),
    estimated_overage_nzd: Number(overageAmount.toFixed(2)),
  };
}

async function extendEventWindow(client: any, eventId: string, minutes: number) {
  const { rows } = await extendEventWindowOnce(client, eventId, minutes, null);
  return rows[0] || null;
}

async function ensureExtensionTrackingColumns(client: any) {
  await client.query(`
    alter table public.events
      add column if not exists extension_checkout_session_ids text[] not null default '{}'
  `);
}

async function extendEventWindowOnce(client: any, eventId: string, minutes: number, sessionId: string | null) {
  await ensureExtensionTrackingColumns(client);
  const { rows } = await client.query(
    `
    update public.events
    set status='paid',
        expires_at = case
          when expires_at is null then now() + ($2::int * interval '1 minute')
          when expires_at < now() then now() + ($2::int * interval '1 minute')
          else expires_at + ($2::int * interval '1 minute')
        end,
        warning_email_sent_at = null,
        warning_email_error = null,
        extension_checkout_session_ids = case
          when $3::text is null or $3::text = '' then extension_checkout_session_ids
          else array_append(extension_checkout_session_ids, $3::text)
        end
    where id=$1
      and ($3::text is null or $3::text = '' or not ($3::text = any(extension_checkout_session_ids)))
    returning expires_at
  `,
    [eventId, minutes, sessionId]
  );
  return { rows };
}

async function sendEventReadyEmail(env: Env, ev: any) {
  if (!ev?.email) return { skipped: true };
  const brand = appName(env);
  const { watchUrl, broadcastUrl } = eventLinks(env, ev);
  const extendUrl = extensionCheckoutUrl(env, ev);
  const extensionPrice = extensionOneHourPrice(env);
  const test = isTestEvent(ev);
  const minutes = testStreamMinutes(env);
  const startDays = paidUnusedExpiryDays(env);
  const subjectLabel = emailSubjectEventLabel(ev);
  const subject = test ? `${brand}: test stream ready - ${subjectLabel}` : `${brand}: stream links ready - ${subjectLabel}`;
  const text = [
    test ? `${brand} test stream ready` : `${brand} live stream ready`,
    "",
    `Event: ${ev.title || "Untitled event"}`,
    ...(test
      ? ["", `This is a free setup test. The live window is ${minutes} minutes and viewer access is limited.`]
      : ["", `Start this event within ${startDays} days. Your paid package time starts when the host goes live.`]
    ),
    "",
    `Broadcast link (private): ${broadcastUrl}`,
    `Watch link (share with viewers): ${watchUrl}`,
    "",
    "Next steps:",
    "1. Open the broadcast link on the host device.",
    "2. Allow camera and microphone, or choose OBS Token for OBS/WHIP setup.",
    "3. Share the watch link with viewers only.",
    "",
    test ? "Free tests cannot be extended. Create a paid event when you are ready to stream for real." : `Need more time later? Buy another hour (${formatNzMoney(extensionPrice)}): ${extendUrl}`,
  ].join("\n");
  const html = emailShell(brand, test ? "Your test stream is ready" : "Your live stream links are ready", `
      <p style="margin:0 0 12px"><b>Event:</b> ${htmlEscape(ev.title || "Untitled event")}</p>
      ${test ? `<p style="margin:0 0 16px">This is a free setup test. The live window is <b>${htmlEscape(minutes)} minutes</b> and viewer access is limited.</p>` : `<p style="margin:0 0 16px">Start this event within <b>${htmlEscape(startDays)} days</b>. Your paid package time starts when the host goes live.</p>`}
      <p style="margin:18px 0 8px">${emailButton("Open broadcast page", broadcastUrl)}</p>
      <p style="margin:0 0 18px;color:#6b7280;font-size:13px">Use this on the host device. It includes browser broadcasting and OBS token setup.</p>
      <p style="margin:18px 0 8px">${emailButton("Open watch page", watchUrl)}</p>
      <p style="margin:0 0 18px;color:#6b7280;font-size:13px">Share this link with viewers.</p>
      ${test ? "" : `<p style="margin:18px 0 8px">${emailButton(`Buy another hour (${formatNzMoney(extensionPrice)})`, extendUrl)}</p>
      <p style="margin:0 0 18px;color:#6b7280;font-size:13px">You can extend this stream any time before it expires. Payment adds one hour to the same broadcast and watch links.</p>`}
      <div style="background:#f3f4f6;border:1px solid #e5e7eb;border-radius:6px;padding:12px;margin-top:16px">
        <p style="margin:0 0 8px"><b>OBS:</b> open the broadcast page, choose OBS Token, then copy the WHIP server and bearer token into OBS.</p>
        <p style="margin:0">${test ? "Free tests cannot be extended. Create a paid event when you are ready to stream for real." : "If you need more time, use the extension payment link above before the event expires."}</p>
      </div>
      <p style="margin:18px 0 0;color:#6b7280;font-size:12px">Buttons above contain the private access links. Keep the broadcast button private and share only the watch button with viewers.</p>
  `);
  return sendEmail(env, { to: ev.email, subject, html, text, tag: "event_ready" });
}

async function ensureReadyEmailColumns(client: any) {
  await client.query(`
    alter table public.events
      add column if not exists ready_email_sent_at timestamptz,
      add column if not exists ready_email_error text,
      add column if not exists viewer_recipient_emails text[] not null default '{}',
      add column if not exists viewer_invites_sent_at timestamptz,
      add column if not exists viewer_invites_error text
  `);
}

async function sendViewerInviteEmailsOnce(client: any, env: Env, ev: any, reason: string) {
  await ensureReadyEmailColumns(client);
  const recipients = normalizeEmailList(ev?.viewer_recipient_emails || []);
  if (!recipients.length) return { skipped: true, reason: "no_viewer_recipients" };

  const { rows } = await client.query(
    `
    update public.events
    set viewer_invites_error=null
    where id=$1
      and viewer_invites_sent_at is null
    returning *
  `,
    [ev.id]
  );
  const claimed = rows[0] || null;
  if (!claimed) return { skipped: true, reason: "already_sent_or_missing" };

  const brand = appName(env);
  const { watchUrl } = eventLinks(env, claimed);
  const subject = `${brand}: watch link for ${claimed.title || "live stream"}`;
  const text = [
    `${brand} watch link`,
    "",
    `Event: ${claimed.title || "Untitled event"}`,
    "",
    `Watch link: ${watchUrl}`,
  ].join("\n");
  const html = emailShell(brand, "Your watch link is ready", `
      <p style="margin:0 0 12px"><b>Event:</b> ${htmlEscape(claimed.title || "Untitled event")}</p>
      <p style="margin:18px 0 8px">${emailButton("Open watch page", watchUrl)}</p>
      <p style="margin:0 0 18px;color:#6b7280;font-size:13px">This is the private viewer link for the event.</p>
      <p style="margin:18px 0 0;color:#6b7280;font-size:12px">The button above contains the private viewer access link.</p>
  `);

  try {
    const results = [];
    for (const to of recipients) {
      results.push(await sendEmail(env, { to, subject, html, text, tag: "viewer_invite" }));
    }
    await client.query(`update public.events set viewer_invites_sent_at=now(), viewer_invites_error=null where id=$1`, [ev.id]);
    console.log("viewer invite emails sent", JSON.stringify({ eventId: ev.id, reason, recipients: recipients.length }));
    return { sent: true, count: recipients.length, results };
  } catch (e: any) {
    const message = (e?.message || String(e)).slice(0, 2000);
    await client.query(`update public.events set viewer_invites_error=$2 where id=$1`, [ev.id, message]);
    console.error("viewer invite emails failed", ev.id, reason, e);
    throw e;
  }
}

async function sendEventReadyEmailOnce(client: any, env: Env, eventId: string, reason: string) {
  await ensureReadyEmailColumns(client);
  const { rows: claimedRows } = await client.query(
    `
    update public.events
    set ready_email_error=null
    where id=$1
      and ready_email_sent_at is null
    returning *
  `,
    [eventId]
  );
  const ev = claimedRows[0] || null;
  if (!ev) {
    const existing = await getEvent(client, eventId);
    if (existing) {
      await sendViewerInviteEmailsOnce(client, env, existing, reason).catch((e) => console.error("viewer invite email failed", eventId, e));
    }
    return { skipped: true, reason: "already_sent_or_missing" };
  }

  try {
    const result = await sendEventReadyEmail(env, ev);
    await client.query(`update public.events set ready_email_sent_at=now(), ready_email_error=null where id=$1`, [eventId]);
    await sendViewerInviteEmailsOnce(client, env, ev, reason).catch((e) => console.error("viewer invite email failed", eventId, e));
    console.log("event ready email sent", JSON.stringify({ eventId, reason, result }));
    return { sent: true, result };
  } catch (e: any) {
    const message = (e?.message || String(e)).slice(0, 2000);
    await client.query(`update public.events set ready_email_error=$2 where id=$1`, [eventId, message]);
    console.error("event ready email failed", eventId, reason, e);
    throw e;
  }
}

async function sendExtensionEmail(env: Env, ev: any, minutes: number) {
  if (!ev?.email) return { skipped: true };
  const brand = appName(env);
  const { watchUrl, broadcastUrl } = eventLinks(env, ev);
  const subject = `${brand}: your stream has been extended`;
  const text = [
    `Your ${brand} stream has been extended by ${minutes} minutes.`,
    "",
    `Event: ${ev.title || "Untitled event"}`,
    `New expiry: ${ev.expires_at || "updated"}`,
    "",
    "The same broadcast and watch links continue working.",
    "",
    `Broadcast link: ${broadcastUrl}`,
    `Watch link: ${watchUrl}`,
  ].join("\n");
  const html = emailShell(brand, "Your stream has been extended", `
      <p style="margin:0 0 12px">Your stream has been extended by <b>${htmlEscape(minutes)} minutes</b>.</p>
      <p style="margin:0 0 12px"><b>Event:</b> ${htmlEscape(ev.title || "Untitled event")}</p>
      <p style="margin:0 0 18px"><b>New expiry:</b> ${htmlEscape(ev.expires_at || "updated")}</p>
      <p style="margin:0 0 16px">The same broadcast and watch links continue working.</p>
      <p style="margin:18px 0 8px">${emailButton("Open broadcast page", broadcastUrl)}</p>
      <p style="margin:18px 0 8px">${emailButton("Open watch page", watchUrl)}</p>
  `);
  return sendEmail(env, { to: ev.email, subject, html, text, tag: "event_extended" });
}

async function sendViewerUpgradeEmail(env: Env, ev: any, amount: number) {
  if (!ev?.email) return { skipped: true };
  const brand = appName(env);
  const { watchUrl, broadcastUrl } = eventLinks(env, ev);
  const subject = `${brand}: viewer capacity upgraded`;
  const text = [
    `Your ${brand} stream viewer capacity has been increased by ${amount}.`,
    "",
    `Event: ${ev.title || "Untitled event"}`,
    `New viewer limit: ${ev.viewer_limit || "updated"}`,
    "",
    "The same broadcast and watch links continue working.",
    "",
    `Broadcast link: ${broadcastUrl}`,
    `Watch link: ${watchUrl}`,
  ].join("\n");
  const html = emailShell(brand, "Viewer capacity upgraded", `
      <p style="margin:0 0 12px">Your stream viewer capacity has been increased by <b>${htmlEscape(amount)}</b>.</p>
      <p style="margin:0 0 12px"><b>Event:</b> ${htmlEscape(ev.title || "Untitled event")}</p>
      <p style="margin:0 0 18px"><b>New viewer limit:</b> ${htmlEscape(ev.viewer_limit || "updated")}</p>
      <p style="margin:0 0 16px">The same broadcast and watch links continue working.</p>
      <p style="margin:18px 0 8px">${emailButton("Open broadcast page", broadcastUrl)}</p>
      <p style="margin:18px 0 8px">${emailButton("Open watch page", watchUrl)}</p>
  `);
  return sendEmail(env, { to: ev.email, subject, html, text, tag: "viewer_capacity_upgraded" });
}

async function sendStreamWarningEmail(env: Env, ev: any) {
  if (!ev?.email) return { skipped: true };
  const brand = appName(env);
  const { watchUrl, broadcastUrl } = eventLinks(env, ev);
  const minutes = streamWarningMinutes(env);
  const price = extensionOneHourPrice(env);
  const extendUrl = extensionCheckoutUrl(env, ev);
  const expires = ev.expires_at || "soon";
  const subject = `${brand}: your stream ends in about ${minutes} minutes`;
  const text = [
    `Your ${brand} stream is due to end in about ${minutes} minutes.`,
    "",
    `Event: ${ev.title || "Untitled event"}`,
    `Current expiry: ${expires}`,
    "",
    `Buy another hour (${formatNzMoney(price)}): ${extendUrl}`,
    "",
    `Broadcast link: ${broadcastUrl}`,
    `Watch link: ${watchUrl}`,
    "",
    "You can complete the renewal from another device. The same broadcast and watch links continue working if the extra hour is purchased before the event expires.",
  ].join("\n");
  const html = emailShell(brand, "Your stream is ending soon", `
      <p style="margin:0 0 12px">Your stream is due to end in about <b>${htmlEscape(minutes)} minutes</b>.</p>
      <p style="margin:0 0 12px"><b>Event:</b> ${htmlEscape(ev.title || "Untitled event")}</p>
      <p style="margin:0 0 18px"><b>Current expiry:</b> ${htmlEscape(expires)}</p>
      <p style="margin:18px 0 8px">${emailButton(`Buy another hour (${formatNzMoney(price)})`, extendUrl)}</p>
      <p style="margin:0 0 16px;color:#6b7280;font-size:13px">This can be completed from another device. If the extra hour is purchased before expiry, the same broadcast and watch links continue working.</p>
      <p style="margin:18px 0 8px">${emailButton("Open broadcast page", broadcastUrl)}</p>
      <p style="margin:18px 0 8px">${emailButton("Open watch page", watchUrl)}</p>
  `);
  return sendEmail(env, { to: ev.email, subject, html, text, tag: "stream_warning" });
}

async function sendReportAlertEmail(env: Env, report: any, ev: any) {
  const to = env.REPORT_ALERT_EMAIL || env.SUPPORT_EMAIL || "";
  if (!to) {
    console.log("report alert skipped: REPORT_ALERT_EMAIL/SUPPORT_EMAIL not configured", report?.id);
    return { skipped: true };
  }

  const brand = appName(env);
  const adminUrl = `${env.APP_ORIGIN}/admin`;
  const subject = `${brand}: ${report.urgent ? "urgent " : ""}stream report - ${report.reason}`;
  const eventTitle = ev?.title || report?.event_snapshot?.title || "Unknown event";
  const eventId = ev?.id || report?.event_id || "unknown";
  const eventEmail = ev?.email || report?.event_snapshot?.email || "-";
  const eventStatus = ev?.status || report?.event_snapshot?.status || "-";
  const eventDisabled = ev?.disabled || report?.event_snapshot?.disabled ? "yes" : "no";

  const text = [
    `A stream report was submitted.`,
    ``,
    `Report ID: ${report.id}`,
    `Urgent: ${report.urgent ? "yes" : "no"}`,
    `Reason: ${report.reason}`,
    `Event: ${eventTitle}`,
    `Event ID: ${eventId}`,
    `Event owner: ${eventEmail}`,
    `Event status: ${eventStatus}`,
    `Event disabled: ${eventDisabled}`,
    `Page: ${report.page || "-"}`,
    `Viewer session: ${report.viewer_session_id || "-"}`,
    `Reporter IP: ${report.ip_address || "-"}`,
    `Reporter email: ${report.reporter_email || "-"}`,
    ``,
    `Description:`,
    report.description || "-",
    ``,
    `Admin: ${adminUrl}`,
  ].join("\n");

  const html = emailShell(brand, `${report.urgent ? "Urgent stream report" : "Stream report received"}`, `
      <p style="margin:0 0 12px"><b>Reason:</b> ${htmlEscape(report.reason)}</p>
      <p style="margin:0 0 12px"><b>Event:</b> ${htmlEscape(eventTitle)}<br><b>Event ID:</b> ${htmlEscape(eventId)}<br><b>Event owner:</b> ${htmlEscape(eventEmail)}</p>
      <p style="margin:0 0 12px"><b>Status:</b> ${htmlEscape(eventStatus)}<br><b>Disabled:</b> ${htmlEscape(eventDisabled)}<br><b>Page:</b> ${htmlEscape(report.page || "-")}</p>
      <p style="margin:0 0 12px"><b>Reporter email:</b> ${htmlEscape(report.reporter_email || "-")}<br><b>Reporter IP:</b> ${htmlEscape(report.ip_address || "-")}<br><b>Viewer session:</b> ${htmlEscape(report.viewer_session_id || "-")}</p>
      <div style="background:#f3f4f6;border:1px solid #e5e7eb;border-radius:6px;padding:12px;margin:14px 0">
        <p style="margin:0"><b>Description:</b><br>${htmlEscape(report.description || "-")}</p>
      </div>
      <p style="margin:18px 0 8px">${emailButton("Open admin", adminUrl)}</p>
  `);

  return sendEmail(env, { to, subject, html, text, tag: "stream_report" });
}

async function testStreamRequestAllowed(client: any, env: Env, email: string, ip: string) {
  const limit = testDailyLimit(env);
  const { rows } = await client.query(
    `
    select
      count(*) filter (where lower(email)=lower($1))::int as email_count,
      count(*) filter (where ip_address=$2 and $2 <> '')::int as ip_count
    from public.test_stream_requests
    where created_at > now() - interval '24 hours'
  `,
    [email, ip || ""]
  );
  const row = rows[0] || {};
  const emailCount = Number(row.email_count || 0);
  const ipCount = Number(row.ip_count || 0);
  return {
    ok: emailCount < limit && ipCount < Math.max(limit * 2, 4),
    email_count: emailCount,
    ip_count: ipCount,
    limit,
  };
}

async function ensureEventStartedWindow(client: any, env: Env, eventId: string, ev: any) {
  let startedNow = false;
  let startsAtIso = ev.starts_at ? new Date(ev.starts_at).toISOString() : null;
  let expiresAtIso = ev.expires_at ? new Date(ev.expires_at).toISOString() : null;

  if (!ev.starts_at) {
    const now = new Date();
    const liveMinutes = isTestEvent(ev)
      ? testStreamMinutes(env)
      : hoursForTier(env, Number(ev.tier)) * 60 + Number(env.GRACE_MINUTES || "0");
    const expires = new Date(now.getTime() + liveMinutes * 60 * 1000);

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

function inventoryModeForEvent(ev: any): InventoryMode | null {
  const rtc = !!ev?.rtc_enabled;
  const hls = !!ev?.hls_enabled;
  if (rtc && hls) return "both";
  if (hls) return "hls";
  if (rtc) return "rtc";
  return null;
}

function inventoryModeFromInput(mode: any): InventoryMode {
  return normalizeMode(mode || "hls") as InventoryMode;
}

function inventorySlotSatisfies(slot: InventorySlot, mode: InventoryMode) {
  const hasRtc = !!slot?.rtc_stage_arn;
  const hasHls = !!slot?.ivs_channel_arn && !!slot?.ivs_ingest_endpoint && !!slot?.ivs_playback_url && !!slot?.ivs_stream_key_encrypted;
  if (mode === "rtc") return hasRtc;
  if (mode === "hls") return hasHls;
  return hasRtc && hasHls;
}

function eventHasInventoryResourcesForMode(ev: any, mode: InventoryMode) {
  const hasRtc = !!ev?.rtc_stage_arn;
  const hasHls = !!ev?.ivs_channel_arn && !!ev?.ivs_ingest_endpoint && !!ev?.ivs_playback_url && !!ev?.ivs_stream_key_encrypted;
  if (mode === "rtc") return hasRtc;
  if (mode === "hls") return hasHls;
  return hasRtc && hasHls;
}

async function updateEventFromInventorySlot(client: any, eventId: string, slot: InventorySlot) {
  const endpointsJson = slot.rtc_stage_endpoints ? JSON.stringify(slot.rtc_stage_endpoints) : null;
  await client.query(
    `
    update public.events
    set ivs_channel_arn=$1,
        ivs_ingest_endpoint=$2,
        ivs_playback_url=$3,
        ivs_stream_key_encrypted=$4,
        rtc_stage_arn=$5,
        rtc_stage_endpoints=$6::jsonb
    where id=$7
  `,
    [
      slot.ivs_channel_arn || null,
      slot.ivs_ingest_endpoint || null,
      slot.ivs_playback_url || null,
      slot.ivs_stream_key_encrypted || null,
      slot.rtc_stage_arn || null,
      endpointsJson,
      eventId,
    ]
  );
}

async function getAssignedInventorySlot(client: any, eventId: string): Promise<InventorySlot | null> {
  const { rows } = await client.query(
    `
    select *
    from public.stream_inventory
    where assigned_event_id = $1
      and status = 'assigned'
    limit 1
  `,
    [eventId]
  );
  return rows[0] || null;
}

async function getReservedInventorySlot(client: any, eventId: string): Promise<InventorySlot | null> {
  const { rows } = await client.query(
    `
    select *
    from public.stream_inventory
    where assigned_event_id = $1
      and status = 'reserved'
    limit 1
  `,
    [eventId]
  );
  return rows[0] || null;
}

async function claimInventorySlotForEvent(client: any, eventId: string, mode: InventoryMode) {
  const already = await getAssignedInventorySlot(client, eventId);
  if (already) {
    await updateEventFromInventorySlot(client, eventId, already);
    return { claimed: false, slot: already, source: "already_assigned" };
  }

  const reserved = await getReservedInventorySlot(client, eventId);
  if (reserved && inventorySlotSatisfies(reserved, mode)) {
    await client.query("begin");
    try {
      const { rows } = await client.query(
        `
        update public.stream_inventory
        set status='assigned',
            assigned_at=now()
        where id=$1
          and status='reserved'
        returning *
      `,
        [reserved.id]
      );
      const slot = rows[0] || reserved;
      await updateEventFromInventorySlot(client, eventId, slot);
      await client.query("commit");
      return { claimed: true, slot, source: "reserved_inventory" };
    } catch (e) {
      await client.query("rollback").catch(() => {});
      throw e;
    }
  }

  await client.query("begin");
  try {
    const { rows } = await client.query(
      `
      select *
      from public.stream_inventory
      where status = 'available'
        and mode = $1
        and (
          ($1 = 'rtc' and rtc_stage_arn is not null)
          or ($1 = 'hls' and
            ivs_channel_arn is not null
            and ivs_ingest_endpoint is not null
            and ivs_playback_url is not null
            and ivs_stream_key_encrypted is not null
          )
          or ($1 = 'both'
            and rtc_stage_arn is not null
            and ivs_channel_arn is not null
            and ivs_ingest_endpoint is not null
            and ivs_playback_url is not null
            and ivs_stream_key_encrypted is not null
          )
        )
      order by created_at asc
      for update skip locked
      limit 1
    `,
      [mode]
    );

    const slot: InventorySlot | null = rows[0] || null;
    if (!slot || !inventorySlotSatisfies(slot, mode)) {
      await client.query("commit");
      return { claimed: false, slot: null, source: "empty" };
    }

    await client.query(
      `
      update public.stream_inventory
      set status='assigned',
          assigned_event_id=$1,
          reserved_at=coalesce(reserved_at, now()),
          assigned_at=now()
      where id=$2
    `,
      [eventId, slot.id]
    );

    await updateEventFromInventorySlot(client, eventId, slot);
    await client.query("commit");
    return { claimed: true, slot, source: "inventory" };
  } catch (e) {
    await client.query("rollback").catch(() => {});
    throw e;
  }
}

async function reserveInventorySlotForCheckout(client: any, eventId: string, mode: InventoryMode) {
  const alreadyAssigned = await getAssignedInventorySlot(client, eventId);
  if (alreadyAssigned) return { reserved: false, slot: alreadyAssigned, source: "already_assigned" };

  const alreadyReserved = await getReservedInventorySlot(client, eventId);
  if (alreadyReserved) {
    await updateEventFromInventorySlot(client, eventId, alreadyReserved);
    return { reserved: false, slot: alreadyReserved, source: "already_reserved" };
  }

  await client.query("begin");
  try {
    const { rows } = await client.query(
      `
      select *
      from public.stream_inventory
      where status = 'available'
        and mode = $1
        and (
          ($1 = 'rtc' and rtc_stage_arn is not null)
          or ($1 = 'hls' and
            ivs_channel_arn is not null
            and ivs_ingest_endpoint is not null
            and ivs_playback_url is not null
            and ivs_stream_key_encrypted is not null
          )
          or ($1 = 'both'
            and rtc_stage_arn is not null
            and ivs_channel_arn is not null
            and ivs_ingest_endpoint is not null
            and ivs_playback_url is not null
            and ivs_stream_key_encrypted is not null
          )
        )
      order by created_at asc
      for update skip locked
      limit 1
    `,
      [mode]
    );

    const slot: InventorySlot | null = rows[0] || null;
    if (!slot || !inventorySlotSatisfies(slot, mode)) {
      await client.query("commit");
      return { reserved: false, slot: null, source: "empty" };
    }

    const { rows: updatedRows } = await client.query(
      `
      update public.stream_inventory
      set status='reserved',
          assigned_event_id=$1,
          reserved_at=now()
      where id=$2
        and status='available'
      returning *
    `,
      [eventId, slot.id]
    );
    const reserved = updatedRows[0] || slot;
    await updateEventFromInventorySlot(client, eventId, reserved);
    await client.query("commit");
    return { reserved: true, slot: reserved, source: "inventory" };
  } catch (e) {
    await client.query("rollback").catch(() => {});
    throw e;
  }
}

async function prepareInventoryEndpoints(env: Env, mode: InventoryMode, endpoints: any, name: string) {
  if (mode === "rtc") return endpoints;

  const sharedArn = getSharedEncoderConfigurationArn(env);
  if (sharedArn) return withEncoderConfigArn(endpoints, sharedArn);

  const storedArn = getEncoderConfigArnFromEndpoints(endpoints);
  if (storedArn) return endpoints;

  const enc = await createEncoderConfiguration(env, name);
  const createdArn = enc?.arn || (enc as any)?.encoderConfiguration?.arn || null;
  return createdArn ? withEncoderConfigArn(endpoints, createdArn) : endpoints;
}

async function createInventorySlot(client: any, env: Env, mode: InventoryMode): Promise<InventorySlot> {
  const slotId = crypto.randomUUID();
  const name = `relay-pool-${mode}-${slotId.slice(0, 8)}`;

  let stageArn: string | null = null;
  let channelArn: string | null = null;

  try {
    let endpoints: any = null;
    if (mode === "rtc" || mode === "both") {
      const st = await createStage(env, name);
      stageArn = st.stageArn;
      endpoints = await prepareInventoryEndpoints(env, mode, st.endpoints || null, name);
    }

    let ingestEndpoint: string | null = null;
    let playbackUrl: string | null = null;
    let streamKeyEncrypted: string | null = null;

    if (mode === "hls" || mode === "both") {
      const ch = await createChannel(env, name);
      channelArn = ch.channelArn;
      ingestEndpoint = ch.ingestEndpoint;
      playbackUrl = ch.playbackUrl;

      if (ch.streamKeyValue) {
        streamKeyEncrypted = await encryptString(ch.streamKeyValue, env.STREAMKEY_ENC_KEY_B64);
      }
    }

    const { rows } = await client.query(
      `
      insert into public.stream_inventory
        (id, mode, status,
         ivs_channel_arn, ivs_ingest_endpoint, ivs_playback_url, ivs_stream_key_encrypted,
         rtc_stage_arn, rtc_stage_endpoints)
      values
        ($1,$2,'available',$3,$4,$5,$6,$7,$8::jsonb)
      returning *
    `,
      [
        slotId,
        mode,
        channelArn,
        ingestEndpoint,
        playbackUrl,
        streamKeyEncrypted,
        stageArn,
        endpoints ? JSON.stringify(endpoints) : null,
      ]
    );

    return rows[0];
  } catch (e) {
    const errors: string[] = [];
    if (channelArn) {
      await bestEffortCleanupStep("deleteInventoryChannelAfterCreateFailure", slotId, () => deleteIvsChannel(env, channelArn!), errors);
    }
    if (stageArn) {
      await bestEffortCleanupStep("deleteInventoryStageAfterCreateFailure", slotId, () => deleteStage(env, stageArn!), errors);
    }
    if (errors.length) console.error("createInventorySlot: partial cleanup failed", slotId, errors.join(" | "));
    throw e;
  }
}

async function countAvailableInventory(client: any, mode: InventoryMode) {
  const { rows } = await client.query(
    `
    select count(*)::int as count
    from public.stream_inventory
    where status='available'
      and mode=$1
      and (
        ($1 = 'rtc' and rtc_stage_arn is not null)
        or ($1 = 'hls' and
          ivs_channel_arn is not null
          and ivs_ingest_endpoint is not null
          and ivs_playback_url is not null
          and ivs_stream_key_encrypted is not null
        )
        or ($1 = 'both'
          and rtc_stage_arn is not null
          and ivs_channel_arn is not null
          and ivs_ingest_endpoint is not null
          and ivs_playback_url is not null
          and ivs_stream_key_encrypted is not null
        )
      )
  `,
    [mode]
  );
  return Number(rows[0]?.count || 0);
}

function inventoryMinimumForMode(env: Env, mode: InventoryMode) {
  const key =
    mode === "hls"
      ? "INVENTORY_MIN_HLS"
      : mode === "rtc"
        ? "INVENTORY_MIN_RTC"
        : "INVENTORY_MIN_BOTH";
  return Math.max(0, Number(env[key] ?? 0));
}

function inventoryMaximumForMode(env: Env, mode: InventoryMode) {
  const key =
    mode === "hls"
      ? "INVENTORY_MAX_HLS"
      : mode === "rtc"
        ? "INVENTORY_MAX_RTC"
        : "INVENTORY_MAX_BOTH";
  const fallback = mode === "both" ? 1 : 3;
  const raw = Number(env[key] ?? fallback);
  return Math.max(0, Number.isFinite(raw) ? raw : fallback);
}

function hlsPrestartCompositionEnabled(env: Env) {
  return String(env.HLS_PRESTART_COMPOSITION ?? "false").toLowerCase() === "true";
}

function inventoryAutofillEnabled(env: Env) {
  return String(env.INVENTORY_AUTOFILL_ENABLED ?? "false").toLowerCase() === "true";
}

function inventoryFillBackoffMs(env: Env) {
  const minutes = Number(env.INVENTORY_FILL_BACKOFF_MINUTES ?? 30);
  return Math.max(5, Number.isFinite(minutes) ? minutes : 30) * 60 * 1000;
}

async function fillInventory(client: any, env: Env, mode: InventoryMode, targetAvailable: number, opts: { respectBackoff?: boolean } = {}) {
  const now = Date.now();
  const backoffUntil = inventoryFillBackoffUntil[mode] || 0;
  const cappedTargetAvailable = Math.min(Math.max(0, targetAvailable), inventoryMaximumForMode(env, mode));
  if (opts.respectBackoff && backoffUntil > now) {
    const before = await countAvailableInventory(client, mode);
    return {
      mode,
      before,
      after: before,
      targetAvailable: cappedTargetAvailable,
      requestedTargetAvailable: targetAvailable,
      created: 0,
      skipped: "backoff",
      retry_after_seconds: Math.ceil((backoffUntil - now) / 1000),
      errors: [],
    };
  }

  await client.query("begin");
  await client.query(`select pg_advisory_xact_lock(hashtext($1))`, [`stream_inventory_fill:${mode}`]);

  const before = await countAvailableInventory(client, mode);
  const maxCreate = Math.max(1, Math.min(Number(env.INVENTORY_FILL_MAX || 2), inventoryMaximumForMode(env, mode)));
  const toCreate = Math.max(0, Math.min(maxCreate, cappedTargetAvailable - before));
  const created: InventorySlot[] = [];
  const errors: string[] = [];

  try {
    for (let i = 0; i < toCreate; i++) {
      try {
        created.push(await createInventorySlot(client, env, mode));
      } catch (e: any) {
        const message = e?.message || String(e);
        errors.push(message);
        console.error("fillInventory failed", mode, message);
        inventoryFillBackoffUntil[mode] = Date.now() + inventoryFillBackoffMs(env);
        break;
      }
    }

    const after = await countAvailableInventory(client, mode);
    if (after >= cappedTargetAvailable) delete inventoryFillBackoffUntil[mode];
    await client.query("commit");
    return { mode, before, after, targetAvailable: cappedTargetAvailable, requestedTargetAvailable: targetAvailable, created: created.length, errors };
  } catch (e) {
    await client.query("rollback").catch(() => {});
    throw e;
  }
}

async function refillInventoryToMinimum(env: Env, mode: InventoryMode) {
  const client = await getClient(env);
  try {
    const target = inventoryMinimumForMode(env, mode);
    if (target <= 0) return { mode, skipped: "target_zero" };
    return await fillInventory(client, env, mode, target, { respectBackoff: true });
  } finally {
    await client.end();
  }
}

async function retireInventorySlot(client: any, env: Env, slotId: string) {
  await client.query("begin");
  try {
    const { rows: existingRows } = await client.query(
      `
      select *
      from public.stream_inventory
      where id=$1
        and status in ('available','failed')
      for update
    `,
      [slotId]
    );
    const existing = existingRows[0] || null;
    if (!existing) {
      await client.query("commit");
      return null;
    }

    const errors: string[] = [];
    if (existing.ivs_channel_arn) {
      await bestEffortCleanupStep("deleteRetiredInventoryChannel", slotId, () => deleteIvsChannel(env, existing.ivs_channel_arn), errors);
    }
    if (existing.rtc_stage_arn) {
      await bestEffortCleanupStep("deleteRetiredInventoryStage", slotId, () => deleteStage(env, existing.rtc_stage_arn), errors);
    }

    if (errors.length) {
      const error = errors.join(" | ").slice(0, 2000);
      await client.query(
        `
        update public.stream_inventory
        set status='failed',
            error=$2
        where id=$1
      `,
        [slotId, error]
      );
      await client.query("commit");
      throw new Error(error);
    }

  const { rows } = await client.query(
    `
    update public.stream_inventory
    set status='retired',
        retired_at=now(),
        ivs_channel_arn=null,
        ivs_ingest_endpoint=null,
        ivs_playback_url=null,
        ivs_stream_key_encrypted=null,
        rtc_stage_arn=null,
        rtc_stage_endpoints=null,
        error=null
    where id=$1
      and status in ('available','failed')
    returning *
  `,
    [slotId]
  );
    await client.query("commit");
    return rows[0] || null;
  } catch (e) {
    await client.query("rollback").catch(() => {});
    throw e;
  }
}

async function resetAwsResourceStateAfterManualCleanup(client: any) {
  await client.query("begin");
  try {
    const { rows: inventoryRows } = await client.query(
      `
      update public.stream_inventory
      set status='retired',
          retired_at=coalesce(retired_at, now()),
          ivs_channel_arn=null,
          ivs_ingest_endpoint=null,
          ivs_playback_url=null,
          ivs_stream_key_encrypted=null,
          rtc_stage_arn=null,
          rtc_stage_endpoints=null,
          assigned_event_id=null,
          error=coalesce(error, 'AWS resources manually cleared; DB resource refs reset')
      where ivs_channel_arn is not null
         or ivs_ingest_endpoint is not null
         or ivs_playback_url is not null
         or ivs_stream_key_encrypted is not null
         or rtc_stage_arn is not null
         or rtc_stage_endpoints is not null
         or status in ('available','reserved','assigned','failed')
      returning id, mode, status, assigned_event_id
    `
    );

    const { rows: eventRows } = await client.query(
      `
      update public.events
      set ivs_channel_arn=null,
          ivs_ingest_endpoint=null,
          ivs_playback_url=null,
          ivs_stream_key_encrypted=null,
          rtc_stage_arn=null,
          rtc_stage_endpoints=null,
          cleanup_error=null
      where ivs_channel_arn is not null
         or ivs_ingest_endpoint is not null
         or ivs_playback_url is not null
         or ivs_stream_key_encrypted is not null
         or rtc_stage_arn is not null
         or rtc_stage_endpoints is not null
      returning id, status, hls_enabled, rtc_enabled
    `
    );

    await client.query("commit");
    return {
      inventory_reset: inventoryRows.length,
      event_resource_refs_cleared: eventRows.length,
      inventory: inventoryRows,
      events: eventRows,
    };
  } catch (e) {
    await client.query("rollback").catch(() => {});
    throw e;
  }
}

async function reconcileCleanedInventoryAssignments(client: any) {
  const { rows } = await client.query(
    `
    update public.stream_inventory si
    set status='retired',
        retired_at=coalesce(si.retired_at, now()),
        error=coalesce(si.error, 'retired after assigned event cleanup')
    from public.events e
    where si.assigned_event_id=e.id
      and si.status='assigned'
      and e.status='expired'
      and e.cleanup_completed_at is not null
    returning si.id, si.mode, si.assigned_event_id
  `
  );
  return rows;
}

async function releaseAbandonedInventoryReservations(client: any) {
  const { rows: events } = await client.query(
    `
    update public.events e
    set ivs_channel_arn=null,
        ivs_ingest_endpoint=null,
        ivs_playback_url=null,
        ivs_stream_key_encrypted=null,
        rtc_stage_arn=null,
        rtc_stage_endpoints=null
    from public.stream_inventory si
    where si.assigned_event_id=e.id
      and si.status='reserved'
      and e.status='pending'
      and si.reserved_at < now() - interval '45 minutes'
    returning e.id
  `
  );

  const { rows: slots } = await client.query(
    `
    update public.stream_inventory si
    set status='available',
        assigned_event_id=null,
        reserved_at=null,
        error=null
    from public.events e
    where si.assigned_event_id=e.id
      and si.status='reserved'
      and e.status='pending'
      and si.reserved_at < now() - interval '45 minutes'
    returning si.id, si.mode
  `
  );

  return { events: events.length, slots: slots.length };
}

async function ensureRtcStage(client: any, env: Env, eventId: string, evIn: any) {
  return await withAdvisoryLock(client, `event:${eventId}:rtc-stage`, async () => {
    await client.query("begin");
    try {
      const { rows } = await client.query(
        `
        select
          id, email, title, tier, viewer_limit, white_label,
          status, starts_at, expires_at, created_at,
          ivs_channel_arn, ivs_ingest_endpoint, ivs_playback_url, ivs_stream_key_encrypted,
          secret_key, broadcast_key, success_token_hash, stripe_session_id,
          rtc_stage_arn, rtc_stage_endpoints, rtc_enabled, hls_enabled, disabled,
          cleanup_started_at, cleanup_completed_at, cleanup_attempts, cleanup_error,
          warning_email_sent_at, warning_email_error,
          ready_email_sent_at, ready_email_error,
          is_test, test_created_ip, test_expires_unused_at
        from public.events
        where id = $1
        for update
      `,
        [eventId]
      );
      let ev = rows[0] || evIn;
      let stageArn: string | null = (ev?.rtc_stage_arn as string) || null;
      let endpoints: any = ev?.rtc_stage_endpoints ?? null;

      if (stageArn) {
        await client.query("commit");
        return { ev, stageArn, endpoints, created: false };
      }

      const st = await createStage(env, `relay-${eventId}`);
      stageArn = st.stageArn;
      endpoints = st.endpoints || null;
      await updateRtc(client, eventId, stageArn, endpoints);
      ev = await getEvent(client, eventId);
      await client.query("commit");

      return {
        ev,
        stageArn: (ev?.rtc_stage_arn as string) || stageArn,
        endpoints: ev?.rtc_stage_endpoints ?? endpoints,
        created: true,
      };
    } catch (e) {
      await client.query("rollback").catch(() => {});
      throw e;
    }
  });
}

async function ensureHlsChannel(client: any, env: Env, eventId: string, evIn: any) {
  return await withAdvisoryLock(client, `event:${eventId}:hls-channel`, async () => {
    await client.query("begin");
    try {
      const { rows } = await client.query(
        `
        select
          id, email, title, tier, viewer_limit, white_label,
          status, starts_at, expires_at, created_at,
          ivs_channel_arn, ivs_ingest_endpoint, ivs_playback_url, ivs_stream_key_encrypted,
          secret_key, broadcast_key, success_token_hash, stripe_session_id,
          rtc_stage_arn, rtc_stage_endpoints, rtc_enabled, hls_enabled, disabled,
          cleanup_started_at, cleanup_completed_at, cleanup_attempts, cleanup_error,
          warning_email_sent_at, warning_email_error,
          ready_email_sent_at, ready_email_error,
          is_test, test_created_ip, test_expires_unused_at
        from public.events
        where id = $1
        for update
      `,
        [eventId]
      );
      let ev: any = rows[0] || evIn;

      if (!ev || !ev.hls_enabled) {
        await client.query("commit");
        return { ev, created: false };
      }

      let channelArn: string | null = (ev.ivs_channel_arn as string) || null;

      if (channelArn) {
        try {
          const ch = await getChannel(env, channelArn);
          await updateIvs(client, eventId, ch.channelArn, ch.ingestEndpoint, ch.playbackUrl, ev.ivs_stream_key_encrypted || null);
          ev = await getEvent(client, eventId);
          await client.query("commit");
          return { ev, created: false };
        } catch (e) {
          console.warn("ensureHlsChannel: existing channel refresh failed", eventId, e);
          await clearIvs(client, eventId);
          ev = await getEvent(client, eventId);
          channelArn = null;
        }
      }

      const ch = await createChannel(env, `relay-${eventId}`);
      let streamKeyEncrypted = ev.ivs_stream_key_encrypted || null;
      if (!streamKeyEncrypted && ch.streamKeyValue) {
        streamKeyEncrypted = await encryptString(ch.streamKeyValue, env.STREAMKEY_ENC_KEY_B64);
      }
      await updateIvs(client, eventId, ch.channelArn, ch.ingestEndpoint, ch.playbackUrl, streamKeyEncrypted);
      ev = await getEvent(client, eventId);
      await client.query("commit");
      return { ev, created: true };
    } catch (e) {
      await client.query("rollback").catch(() => {});
      throw e;
    }
  });
}

async function ensureStreamKey(client: any, env: Env, eventId: string, evIn: any) {
  let ev = evIn;
  if (!ev?.hls_enabled) return { ev, streamKeyPlaintext: null, created: false };
  if (!ev.ivs_channel_arn || !ev.ivs_ingest_endpoint || !ev.ivs_playback_url) {
    ev = (await ensureHlsChannel(client, env, eventId, ev)).ev;
  }
  if (!ev.ivs_channel_arn || !ev.ivs_ingest_endpoint || !ev.ivs_playback_url) {
    throw new Error("hls_channel_not_ready");
  }

  if (ev.ivs_stream_key_encrypted) {
    const streamKeyPlaintext = await decryptString(ev.ivs_stream_key_encrypted, env.STREAMKEY_ENC_KEY_B64);
    return { ev, streamKeyPlaintext, created: false };
  }

  const oldChannelArn = ev.ivs_channel_arn;
  await clearIvs(client, eventId);
  if (oldChannelArn) {
    await bestEffortCleanupStep("deleteHlsChannelWithoutRecoverableStreamKey", eventId, () => deleteIvsChannel(env, oldChannelArn), []);
  }
  ev = await getEvent(client, eventId);
  ev = (await ensureHlsChannel(client, env, eventId, ev)).ev;

  if (!ev.ivs_channel_arn || !ev.ivs_ingest_endpoint || !ev.ivs_playback_url) {
    throw new Error("hls_channel_not_ready");
  }
  if (!ev.ivs_stream_key_encrypted) {
    throw new Error("hls_stream_key_not_returned");
  }

  const streamKeyPlaintext = await decryptString(ev.ivs_stream_key_encrypted, env.STREAMKEY_ENC_KEY_B64);
  return { ev, streamKeyPlaintext, created: true };
}

async function ensureHlsInfrastructure(client: any, env: Env, eventId: string, evIn: any) {
  let ev = evIn;
  if (!ev || !ev.hls_enabled) return ev;

  const ch = await ensureHlsChannel(client, env, eventId, ev);
  ev = ch.ev;

  return await getEvent(client, eventId);
}

async function provisionHlsBroadcast(client: any, env: Env, eventId: string, evIn: any) {
  let ev = await ensureHlsInfrastructure(client, env, eventId, evIn);
  try {
    return await ensureStreamKey(client, env, eventId, ev);
  } catch (e: any) {
    const message = String(e?.message || e);
    if (!message.includes("hls_channel_not_ready")) throw e;
    ev = await getEvent(client, eventId);
    if (ev?.ivs_channel_arn && ev?.ivs_ingest_endpoint && ev?.ivs_playback_url && ev?.ivs_stream_key_encrypted) {
      return await ensureStreamKey(client, env, eventId, ev);
    }
    throw e;
  }
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

async function preProvisionPaidEvent(client: any, env: Env, eventId: string, opts: { prestartComposition?: boolean } = {}) {
  let ev = await getEvent(client, eventId);
  if (!ev || ev.status !== "paid" || isExpired(ev) || isDisabled(ev)) return ev;

  const inventoryMode = inventoryModeForEvent(ev);
  const canUseInventory = !isTestEvent(ev) || String(env.TEST_USE_INVENTORY || "").toLowerCase() === "true";
  if (canUseInventory && inventoryMode && !eventHasInventoryResourcesForMode(ev, inventoryMode)) {
    try {
      const claim = await claimInventorySlotForEvent(client, eventId, inventoryMode);
      if (claim.slot) {
        console.log("preProvisionPaidEvent: inventory", JSON.stringify({
          eventId,
          mode: inventoryMode,
          source: claim.source,
          claimed: claim.claimed,
          slotId: claim.slot.id,
        }));
        ev = await getEvent(client, eventId);
      }
    } catch (e) {
      console.error("preProvisionPaidEvent: inventory claim failed", eventId, e);
    }
  }

  // RTC/HLS resource creation is intentionally limited to inventory fill or the
  // explicit broadcaster provision calls. Generic paid/link/public warm-up is hit
  // by multiple pages and pollers, so it must not create IVS resources.

  if (ev?.hls_enabled && opts.prestartComposition && hlsPrestartCompositionEnabled(env)) {
    try {
      ev = (await ensureCompositionStarted(client, env, eventId, ev)).ev;
    } catch (e) {
      console.error("preProvisionPaidEvent: hls composition prestart failed", eventId, e);
    }
  }

  return await getEvent(client, eventId);
}

function displayHoursForTier(env: Env | null | undefined, tier: number) {
  try {
    const value = hoursForTier(env || {}, tier);
    if (Number.isFinite(value) && value > 0) return value;
  } catch {}
  if (tier === 1) return 1;
  if (tier === 2) return 2;
  if (tier === 8) return 8;
  return 3;
}

function buildReadiness(ev: any, role: AccessRole, env?: Env): Readiness {
  const endpoints = ev?.rtc_stage_endpoints ?? null;
  const hlsEnabled = !!ev?.hls_enabled;
  const rtcEnabled = !!ev?.rtc_enabled;
  const stageExists = rtcEnabled ? !!ev?.rtc_stage_arn : false;
  const channelExists = !!ev?.ivs_channel_arn && !!ev?.ivs_ingest_endpoint && !!ev?.ivs_playback_url;
  const streamKeyExists = !!ev?.ivs_stream_key_encrypted;
  const encoderExists = !!getEncoderConfigArnFromEndpoints(endpoints);
  const compositionStarted = !!getCompositionArnFromEndpoints(endpoints);
  const playbackUrlExists = !!ev?.ivs_playback_url;
  const whipUrlExists = !!endpoints?.whip;
  const paid = ev?.status === "paid";
  const disabled = isDisabled(ev);
  const expired = isExpired(ev);
  const streamWindowOpen = !!ev?.starts_at && !expired;
  const paidEndsAtMs = ev?.starts_at
    ? new Date(ev.starts_at).getTime() + displayHoursForTier(env, Number(ev.tier)) * 3600 * 1000
    : null;
  const paidSecondsRemaining = paidEndsAtMs && !expired
    ? Math.max(0, Math.floor((paidEndsAtMs - Date.now()) / 1000))
    : null;
  const expirySecondsRemaining = ev?.expires_at && !expired ? Math.max(0, Math.floor((new Date(ev.expires_at).getTime() - Date.now()) / 1000)) : null;
  const secondsRemaining = paidSecondsRemaining ?? expirySecondsRemaining;
  const hlsBroadcastReady = hlsEnabled && channelExists && streamKeyExists;
  const canIssueBroadcasterToken = paid && !disabled && !expired && ((rtcEnabled && stageExists) || hlsBroadcastReady);
  const canIssueViewerToken = paid && !disabled && !expired && rtcEnabled && stageExists;
  const canGoLive = role === "broadcaster" && canIssueBroadcasterToken;
  const canWatchHls = paid && !disabled && !expired && hlsEnabled && playbackUrlExists;
  const canWatchRtc = paid && !disabled && !expired && rtcEnabled && stageExists;

  let state = "preparing";
  let detail = "Preparing stream infrastructure.";

  if (disabled) {
    state = "disabled";
    detail = "This event has been disabled.";
  } else if (!paid) {
    state = "awaiting_payment";
    detail = "Waiting for payment confirmation.";
  } else if (expired) {
    state = "expired";
    detail = "This event has expired.";
  } else if (rtcEnabled && !stageExists) {
    state = "preparing_stage";
    detail = "Creating RTC stage.";
  } else if (hlsEnabled && !channelExists) {
    state = "preparing_hls_channel";
    detail = "Creating HLS channel.";
  } else if (hlsEnabled && !streamKeyExists) {
    state = "preparing_hls_channel";
    detail = "Creating HLS stream key.";
  } else if (role === "broadcaster" && canGoLive) {
    state = streamWindowOpen ? "live_window_open" : "ready_to_go_live";
    detail = streamWindowOpen ? "Broadcast window is open." : "Ready to go live.";
  } else if (role === "viewer" && (canWatchHls || canWatchRtc)) {
    state = "waiting_for_stream";
    detail = "Waiting for broadcaster to publish media.";
  }

  let warning: string | null = null;
  if (secondsRemaining !== null && secondsRemaining <= 120) warning = "This paid stream ends in less than 2 minutes.";
  else if (secondsRemaining !== null && secondsRemaining <= 600) warning = "This paid stream ends in less than 10 minutes.";

  return {
    role,
    paid,
    disabled,
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
    seconds_remaining: secondsRemaining,
    warning,
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
      const paidEvent = await preProvisionPaidEvent(client, env, ev.id);
      await sendEventReadyEmailOnce(client, env, ev.id, "stripe_poll").catch((e) => console.error("event ready email failed after stripe poll", ev.id, e));
      return paidEvent;
    }
  } catch (e) {
    console.error("maybeRefreshPaidStatus failed", ev.id, e);
  }
  return ev;
}

async function preProvisionEventIfNeeded(client: any, env: Env, eventId: string, evIn: any) {
  let ev = evIn;
  if (!ev || ev.status !== "paid" || isExpired(ev) || isDisabled(ev)) return ev;
  const needsRtc = !!ev.rtc_enabled && !ev.rtc_stage_arn;
  const needsHlsChannel = !!ev.hls_enabled && (!ev.ivs_channel_arn || !ev.ivs_ingest_endpoint || !ev.ivs_playback_url || !ev.ivs_stream_key_encrypted);
  if (!needsRtc && !needsHlsChannel) return ev;
  return await preProvisionPaidEvent(client, env, eventId);
}

async function handleScheduled(_event: ScheduledEvent, env: any, _ctx: ExecutionContext) {
  console.log("cron tick", new Date().toISOString());

  const client = await getClient(env);
  try {
    const { rowCount: expiredCount } = await client.query(
      `
      update public.events
      set status='expired'
      where status in ('paid','live')
        and expires_at is not null
        and expires_at <= now()
    `
    );
    if (expiredCount) console.log("cron expired events", expiredCount);

    const { rowCount: unusedTestExpiredCount } = await client.query(
      `
      update public.events
      set status='expired',
          expires_at=coalesce(expires_at, now())
      where status='paid'
        and is_test = true
        and starts_at is null
        and test_expires_unused_at is not null
        and test_expires_unused_at <= now()
    `
    );
    if (unusedTestExpiredCount) console.log("cron expired unused test events", unusedTestExpiredCount);

    const unusedPaidExpiryDays = paidUnusedExpiryDays(env);
    const { rowCount: unusedPaidExpiredCount } = await client.query(
      `
      update public.events
      set status='expired',
          expires_at=coalesce(expires_at, now())
      where status='paid'
        and coalesce(is_test,false) = false
        and starts_at is null
        and created_at <= now() - ($1::int * interval '1 day')
    `,
      [unusedPaidExpiryDays]
    );
    if (unusedPaidExpiredCount) console.log("cron expired unused paid events", unusedPaidExpiredCount);

    const { rows: cleanupRows } = await client.query(
      `
      select
        id, email, title, tier, viewer_limit, white_label,
        status, starts_at, expires_at, created_at,
        ivs_channel_arn, ivs_ingest_endpoint, ivs_playback_url, ivs_stream_key_encrypted,
        secret_key, broadcast_key, success_token_hash, stripe_session_id,
        rtc_stage_arn, rtc_stage_endpoints, rtc_enabled, hls_enabled,
        cleanup_started_at, cleanup_completed_at, cleanup_attempts, cleanup_error
      from public.events
      where status='expired'
        and cleanup_completed_at is null
        and (expires_at is null or expires_at <= now())
        and cleanup_attempts < 5
      order by expires_at asc nulls last
      limit 10
    `
    );
    for (const ev of cleanupRows) {
      const result = await cleanupEventResources(client, env, ev, "cron_expired");
      if (result.ok) {
        const retiredInventory = await retireAssignedInventoryForEvent(client, ev.id, "retired after cron cleanup");
        if (retiredInventory.length) console.log("cron retired assigned inventory", JSON.stringify({ eventId: ev.id, retiredInventory }));
      }
      console.log("cron cleanup", JSON.stringify(result));
    }

    const reconciledInventory = await reconcileCleanedInventoryAssignments(client);
    if (reconciledInventory.length) console.log("inventory assignment reconciliation", JSON.stringify(reconciledInventory));

    const releasedReservations = await releaseAbandonedInventoryReservations(client);
    if (releasedReservations.slots) console.log("released abandoned inventory reservations", JSON.stringify(releasedReservations));

    const warningLeadMinutes = streamWarningLeadMinutes(env);
    const { rows: warningRows } = await client.query(
      `
      select
        id, email, title, tier, viewer_limit, white_label,
        status, starts_at, expires_at, created_at,
        ivs_channel_arn, ivs_ingest_endpoint, ivs_playback_url, ivs_stream_key_encrypted,
        secret_key, broadcast_key, success_token_hash, stripe_session_id,
        rtc_stage_arn, rtc_stage_endpoints, rtc_enabled, hls_enabled,
        cleanup_started_at, cleanup_completed_at, cleanup_attempts, cleanup_error,
        warning_email_sent_at, warning_email_error
      from public.events
      where status='paid'
        and coalesce(is_test,false) = false
        and starts_at is not null
        and expires_at is not null
        and expires_at > now()
        and expires_at <= now() + ($1::int * interval '1 minute')
        and warning_email_sent_at is null
      order by expires_at asc
      limit 20
    `,
      [warningLeadMinutes]
    );
    for (const ev of warningRows) {
      try {
        const result = await sendStreamWarningEmail(env, ev);
        await client.query(
          `update public.events set warning_email_sent_at=now(), warning_email_error=null where id=$1`,
          [ev.id]
        );
        console.log("warning email sent", JSON.stringify({ eventId: ev.id, result }));
      } catch (e: any) {
        const message = (e?.message || String(e)).slice(0, 2000);
        await client.query(`update public.events set warning_email_error=$2 where id=$1`, [ev.id, message]);
        console.error("warning email failed", ev.id, e);
      }
    }

    if (inventoryAutofillEnabled(env)) {
      const modes: InventoryMode[] = ["hls", "rtc", "both"];
      for (const mode of modes) {
        const min = inventoryMinimumForMode(env, mode);
        if (min > 0) {
          const result = await fillInventory(client, env, mode, min, { respectBackoff: true });
          console.log("inventory refill", JSON.stringify(result));
        }
      }
    } else {
      console.log("inventory refill skipped: INVENTORY_AUTOFILL_ENABLED is not true");
    }
  } catch (e) {
    console.error("inventory scheduled refill failed", e);
  } finally {
    await client.end();
  }
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext) {
    const url = new URL(request.url);
    const { pathname } = url;
    const method = request.method.toUpperCase();

    if (method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders(env) });
    }

    try {
      if (method === "GET" && pathname === "/") return text(env, `${appName(env)} API OK`, 200);
      if (method === "GET" && pathname === "/healthz") return text(env, "ok", 200);

      if (method === "GET" && pathname === "/api/pricing") {
        const tiers: any = {};
        for (const mode of ["hls", "rtc", "both"] as StreamMode[]) {
          tiers[mode] = {};
          for (const tier of allowedTiersForMode(mode)) {
            tiers[mode][String(tier)] = {
              hours: hoursForTier(env, tier),
              price_nzd: priceCentsFor(env, tier, mode),
              included: includedUsageFor(env, tier, mode),
            };
          }
        }

        return json(env, {
          ok: true,
          currency: "nzd",
          pricing_model: "base_price_with_optional_extension",
          tiers,
          overage: {
            hls_viewer_hour_nzd: dollarsToCents(env.HLS_OVERAGE_VIEWER_HOUR_NZD ?? 0),
            rtc_participant_hour_nzd: dollarsToCents(env.WEBRTC_OVERAGE_PARTICIPANT_HOUR_NZD ?? 0),
            extra_stream_minute_nzd: dollarsToCents(env.EXTRA_STREAM_MINUTE_NZD ?? 0),
          },
          extensions: {
            one_hour_nzd: dollarsToCents(extensionOneHourPrice(env)),
          },
          viewer_upgrades: {
            "100": dollarsToCents(viewerUpgradePrice(env, 100)),
            "250": dollarsToCents(viewerUpgradePrice(env, 250)),
            "500": dollarsToCents(viewerUpgradePrice(env, 500)),
          },
        });
      }

      if (method === "GET" && pathname === "/api/admin/launch-checks") {
        const auth = requireAdminKey(request, env);
        if (auth) return auth;

        const checks: any[] = [];
        const add = (id: string, label: string, status: "pass" | "warn" | "fail", detail: string, data?: any) => {
          checks.push({ id, label, status, detail, data: data ?? null });
        };
        const has = (name: string) => !!String(env[name] || "").trim();
        const requestOrigin = `${url.protocol}//${url.host}`;

        add("brand", "Brand", appName(env) === "Castlink" ? "pass" : "warn", `BRAND_NAME=${appName(env)}`);
        add(
          "app_origin",
          "App origin",
          String(env.APP_ORIGIN || "") === "https://castlink.stream" ? "pass" : "warn",
          `APP_ORIGIN=${env.APP_ORIGIN || "(missing)"}`
        );
        add("api_origin", "API origin", requestOrigin.includes("api.castlink.stream") ? "pass" : "warn", `Current API origin=${requestOrigin}`);
        add("admin_key", "Admin key", has("ADMIN_KEY") ? "pass" : "fail", has("ADMIN_KEY") ? "Configured" : "ADMIN_KEY missing");
        add("stripe_secret", "Stripe secret", has("STRIPE_SECRET_KEY") ? "pass" : "fail", has("STRIPE_SECRET_KEY") ? "Configured" : "STRIPE_SECRET_KEY missing");
        add(
          "stripe_webhook",
          "Stripe webhook secret",
          has("STRIPE_WEBHOOK_SECRET") ? "pass" : "fail",
          has("STRIPE_WEBHOOK_SECRET") ? "Configured" : "STRIPE_WEBHOOK_SECRET missing"
        );
        add(
          "email",
          "Email provider",
          emailConfigured(env) ? "pass" : "fail",
          emailConfigured(env)
            ? `${emailProvider(env)} configured from ${env.EMAIL_FROM || "(missing sender)"}`
            : "EMAIL_FROM or provider token missing"
        );
        add(
          "report_email",
          "Report alert email",
          has("REPORT_ALERT_EMAIL") ? "pass" : "warn",
          env.REPORT_ALERT_EMAIL || "REPORT_ALERT_EMAIL missing; moderation alerts will not send"
        );
        add(
          "aws_region",
          "AWS IVS region",
          has("AWS_REGION") && has("IVS_API_ENDPOINT") && has("IVS_REALTIME_API_ENDPOINT") ? "pass" : "fail",
          `${env.AWS_REGION || "(missing)"} / ${env.IVS_API_ENDPOINT || "(missing)"} / ${env.IVS_REALTIME_API_ENDPOINT || "(missing)"}`
        );
        add(
          "aws_credentials",
          "AWS credentials",
          has("AWS_ACCESS_KEY_ID") && has("AWS_SECRET_ACCESS_KEY") ? "pass" : "fail",
          has("AWS_ACCESS_KEY_ID") && has("AWS_SECRET_ACCESS_KEY") ? "Configured" : "AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY missing"
        );
        add(
          "stream_key_encryption",
          "Stream key encryption",
          has("STREAMKEY_ENC_KEY_B64") ? "pass" : "fail",
          has("STREAMKEY_ENC_KEY_B64") ? "Configured" : "STREAMKEY_ENC_KEY_B64 missing"
        );
        add(
          "ivs_proxy",
          "IVS proxy",
          has("IVS_PROXY_BASE") && has("IVS_PROXY_SECRET") ? "pass" : "warn",
          has("IVS_PROXY_BASE") ? (has("IVS_PROXY_SECRET") ? "Proxy base and secret configured" : "IVS_PROXY_SECRET missing") : "IVS_PROXY_BASE missing"
        );

        const pricingWarnings: string[] = [];
        for (const mode of ["hls", "rtc", "both"] as StreamMode[]) {
          for (const tier of allowedTiersForMode(mode)) {
            if (priceCentsFor(env, tier, mode) <= 0) pricingWarnings.push(`${mode} ${tier}h has no price`);
          }
        }
        if (extensionOneHourPrice(env) <= 0) pricingWarnings.push("1 hour extension has no price");
        for (const amount of [100, 250, 500]) {
          if (viewerUpgradePrice(env, amount) <= 0) pricingWarnings.push(`+${amount} viewer upgrade has no price`);
        }
        const capWarnPct = viewerCapWarningPercent(env);
        if (capWarnPct < 50 || capWarnPct >= 100) pricingWarnings.push("VIEWER_CAP_WARNING_PERCENT should be 50-99");
        add(
          "pricing",
          "Pricing",
          pricingWarnings.length ? "fail" : "pass",
          pricingWarnings.length ? pricingWarnings.join("; ") : "Launch package and extension prices are configured"
        );
        add(
          "overage",
          "Overage charging",
          Number(env.HLS_OVERAGE_VIEWER_HOUR_NZD || 0) === 0 &&
          Number(env.WEBRTC_OVERAGE_PARTICIPANT_HOUR_NZD || 0) === 0 &&
          Number(env.EXTRA_STREAM_MINUTE_NZD || 0) === 0 ? "pass" : "warn",
          "Expected launch state: no automatic overage charges"
        );
        const testConfigWarnings: string[] = [];
        const testMinutes = Number(env.TEST_STREAM_MINUTES ?? 3);
        const testUnusedMinutes = Number(env.TEST_UNUSED_EXPIRY_MINUTES ?? 15);
        const testViewers = Number(env.TEST_VIEWER_LIMIT ?? 3);
        const testDaily = Number(env.TEST_STREAMS_PER_EMAIL_PER_DAY ?? 2);
        const testUsesInventory = String(env.TEST_USE_INVENTORY || "false").toLowerCase() === "true";
        const paidExpiryDays = Number(env.PAID_UNUSED_EXPIRY_DAYS ?? 7);
        if (!Number.isFinite(testMinutes) || testMinutes < 1 || testMinutes > 10) testConfigWarnings.push("TEST_STREAM_MINUTES should be 1-10");
        if (!Number.isFinite(testUnusedMinutes) || testUnusedMinutes < testMinutes || testUnusedMinutes > 60) testConfigWarnings.push("TEST_UNUSED_EXPIRY_MINUTES should be at least the test length and no more than 60");
        if (!Number.isFinite(testViewers) || testViewers < 1 || testViewers > 10) testConfigWarnings.push("TEST_VIEWER_LIMIT should be 1-10");
        if (!Number.isFinite(testDaily) || testDaily < 1 || testDaily > 10) testConfigWarnings.push("TEST_STREAMS_PER_EMAIL_PER_DAY should be 1-10");
        if (testUsesInventory) testConfigWarnings.push("TEST_USE_INVENTORY=true will consume paid pre-made slots");
        if (!Number.isFinite(paidExpiryDays) || paidExpiryDays < 1 || paidExpiryDays > 90) testConfigWarnings.push("PAID_UNUSED_EXPIRY_DAYS should be 1-90");
        add(
          "test_stream_config",
          "Start-window config",
          testConfigWarnings.length ? "warn" : "pass",
          testConfigWarnings.length ? testConfigWarnings.join("; ") : `Paid events must start within ${paidExpiryDays} days. Free tests: ${testMinutes} minutes, unused expiry ${testUnusedMinutes} minutes, ${testViewers} viewers, ${testDaily}/email/day`,
          {
            PAID_UNUSED_EXPIRY_DAYS: env.PAID_UNUSED_EXPIRY_DAYS ?? 7,
            TEST_STREAM_MINUTES: env.TEST_STREAM_MINUTES ?? 3,
            TEST_UNUSED_EXPIRY_MINUTES: env.TEST_UNUSED_EXPIRY_MINUTES ?? 15,
            TEST_VIEWER_LIMIT: env.TEST_VIEWER_LIMIT ?? 3,
            TEST_STREAMS_PER_EMAIL_PER_DAY: env.TEST_STREAMS_PER_EMAIL_PER_DAY ?? 2,
            TEST_USE_INVENTORY: env.TEST_USE_INVENTORY ?? "false",
          }
        );

        const client = await getClient(env);
        try {
          try {
            const { rows } = await client.query(`select now() as now`);
            add("db", "Database connection", "pass", `Connected at ${rows[0]?.now || "now"}`);
            await ensureReadyEmailColumns(client);
            readyEmailColumnsEnsured = true;
          } catch (e: any) {
            add("db", "Database connection", "fail", e?.message || String(e));
          }

          try {
            const { rows } = await client.query(`
              select
                to_regclass('public.events') as events,
                to_regclass('public.stream_inventory') as stream_inventory,
                to_regclass('public.reports') as reports,
                to_regclass('public.test_stream_requests') as test_stream_requests
            `);
            const row = rows[0] || {};
            const missing = ["events", "stream_inventory", "reports", "test_stream_requests"].filter((k) => !row[k]);
            add("tables", "Required tables", missing.length ? "fail" : "pass", missing.length ? `Missing: ${missing.join(", ")}` : "events, stream_inventory, reports, and test_stream_requests exist");
          } catch (e: any) {
            add("tables", "Required tables", "fail", e?.message || String(e));
          }

          try {
            const { rows } = await client.query(`
              select column_name
              from information_schema.columns
              where table_schema='public'
                and table_name='events'
                and column_name in ('disabled','cleanup_started_at','cleanup_completed_at','cleanup_error','warning_email_sent_at','warning_email_error','ready_email_sent_at','ready_email_error','viewer_recipient_emails','viewer_invites_sent_at','viewer_invites_error','is_test','test_created_ip','test_expires_unused_at')
            `);
            const have = new Set(rows.map((r: any) => r.column_name));
            const required = ["disabled", "cleanup_started_at", "cleanup_completed_at", "cleanup_error", "warning_email_sent_at", "warning_email_error", "ready_email_sent_at", "ready_email_error", "viewer_recipient_emails", "viewer_invites_sent_at", "viewer_invites_error", "is_test", "test_created_ip", "test_expires_unused_at"];
            const missing = required.filter((c) => !have.has(c));
            add("event_columns", "Event migration columns", missing.length ? "fail" : "pass", missing.length ? `Missing: ${missing.join(", ")}` : "Cleanup, warning, invite, disabled, and test columns exist");
          } catch (e: any) {
            add("event_columns", "Event migration columns", "fail", e?.message || String(e));
          }

          try {
            const { rows } = await client.query(`
              select mode, status, count(*)::int as count
              from public.stream_inventory
              group by mode, status
              order by mode, status
            `);
            const available = rows.filter((r: any) => r.status === "available").reduce((sum: number, r: any) => sum + Number(r.count || 0), 0);
            add("inventory", "Pre-made inventory", available > 0 ? "pass" : "warn", `${available} available slots`, rows);
          } catch (e: any) {
            add("inventory", "Pre-made inventory", "fail", e?.message || String(e));
          }

          try {
            const { rows } = await client.query(`
              select
                count(*) filter (where cleanup_error is not null)::int as cleanup_errors,
                count(*) filter (where warning_email_error is not null)::int as warning_email_errors,
                count(*) filter (where ready_email_error is not null)::int as ready_email_errors,
                count(*) filter (where viewer_invites_error is not null)::int as viewer_invite_errors
              from public.events
            `);
            const row = rows[0] || {};
            const problems = Number(row.cleanup_errors || 0) + Number(row.warning_email_errors || 0) + Number(row.ready_email_errors || 0) + Number(row.viewer_invite_errors || 0);
            add("event_errors", "Event operational errors", problems ? "warn" : "pass", `${row.cleanup_errors || 0} cleanup errors, ${row.warning_email_errors || 0} warning email errors, ${row.ready_email_errors || 0} ready email errors, ${row.viewer_invite_errors || 0} viewer invite errors`, row);
          } catch (e: any) {
            add("event_errors", "Event operational errors", "warn", e?.message || String(e));
          }

          try {
            const { rows } = await client.query(
              `
              select count(*)::int as stale_unused_paid
              from public.events
              where status='paid'
                and coalesce(is_test,false) = false
                and starts_at is null
                and created_at <= now() - ($1::int * interval '1 day')
            `,
              [paidUnusedExpiryDays(env)]
            );
            const stale = Number(rows[0]?.stale_unused_paid || 0);
            add("stale_unused_paid", "Stale unused paid events", stale ? "warn" : "pass", `${stale} paid events older than the start window`);
          } catch (e: any) {
            add("stale_unused_paid", "Stale unused paid events", "warn", e?.message || String(e));
          }

          try {
            const { rows } = await client.query(`select count(*)::int as open_reports from public.reports where status <> 'closed'`);
            const openReports = Number(rows[0]?.open_reports || 0);
            add("open_reports", "Open reports", openReports ? "warn" : "pass", `${openReports} open reports`);
          } catch (e: any) {
            add("open_reports", "Open reports", "warn", e?.message || String(e));
          }
        } finally {
          await client.end();
        }

        const failed = checks.filter((c) => c.status === "fail").length;
        const warnings = checks.filter((c) => c.status === "warn").length;
        return json(env, { ok: failed === 0, failed, warnings, checked_at: new Date().toISOString(), checks });
      }

      if (method === "GET" && pathname === "/api/admin/events") {
        const auth = requireAdminKey(request, env);
        if (auth) return auth;

        const client = await getClient(env);
        try {
          const { rows } = await client.query(
            `
            select id, email, title, tier, viewer_limit, white_label,
                   status, disabled, is_test, starts_at, expires_at, created_at,
                   rtc_enabled, hls_enabled,
                   cleanup_started_at, cleanup_completed_at, cleanup_attempts, cleanup_error
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

      if (method === "GET" && pathname === "/api/admin/reports") {
        const auth = requireAdminKey(request, env);
        if (auth) return auth;

        const client = await getClient(env);
        try {
          const { rows } = await client.query(
            `
            select r.id, r.event_id, r.page, r.reason, r.description,
                   r.reporter_email, r.urgent, r.status, r.viewer_session_id,
                   r.ip_address, r.user_agent, r.event_snapshot, r.created_at,
                   e.title as event_title, e.email as event_email, e.status as event_status, e.disabled as event_disabled, e.is_test as event_is_test,
                   e.starts_at, e.expires_at
            from public.reports r
            left join public.events e on e.id = r.event_id
            order by r.created_at desc
            limit 100
          `
          );
          return json(env, { ok: true, reports: rows });
        } finally {
          await client.end();
        }
      }

      {
        const m = match(pathname, /^\/api\/admin\/reports\/([^\/]+)\/status$/);
        if (method === "POST" && m) {
          const auth = requireAdminKey(request, env);
          if (auth) return auth;

          const reportId = m[0];
          const body: any = await request.json().catch(() => ({}));
          const status = String(body.status || "").trim().toLowerCase();
          if (!["open", "reviewing", "closed"].includes(status)) return json(env, { error: "invalid_status" }, 400);

          const client = await getClient(env);
          try {
            const { rows } = await client.query(
              `update public.reports set status=$2 where id=$1 returning *`,
              [reportId, status]
            );
            if (!rows[0]) return json(env, { error: "not_found" }, 404);
            return json(env, { ok: true, report: rows[0] });
          } finally {
            await client.end();
          }
        }
      }

      {
        const m = match(pathname, /^\/api\/admin\/events\/([^\/]+)$/);
        if (method === "GET" && m) {
          const auth = requireAdminKey(request, env);
          if (auth) return auth;

          const eventId = m[0];
          const client = await getClient(env);
          try {
            const ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);
            return json(env, { ok: true, event: await adminEventPayload(client, env, ev) });
          } finally {
            await client.end();
          }
        }
      }

      {
        const m = match(pathname, /^\/api\/admin\/events\/([^\/]+)\/resend-email$/);
        if (method === "POST" && m) {
          const auth = requireAdminKey(request, env);
          if (auth) return auth;

          const eventId = m[0];
          const client = await getClient(env);
          try {
            const ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);
            const result = await sendEventReadyEmail(env, ev);
            return json(env, { ok: true, result });
          } finally {
            await client.end();
          }
        }
      }

      {
        const m = match(pathname, /^\/api\/admin\/events\/([^\/]+)\/(finish|disable)$/);
        if (method === "POST" && m) {
          const auth = requireAdminKey(request, env);
          if (auth) return auth;

          const eventId = m[0];
          const action = m[1];
          const client = await getClient(env);
          try {
            const ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);
            const result = await finishEventForAdmin(client, env, eventId, action === "disable" ? "admin_disable" : "admin_finish");
            return json(env, {
              ok: true,
              finished: true,
              disabled: action === "disable",
              event: { id: result.ev.id, status: result.ev.status, expires_at: result.ev.expires_at },
              usage: result.usage,
              cleanup: result.cleanup,
            });
          } finally {
            await client.end();
          }
        }
      }

      if (method === "GET" && pathname === "/api/admin/inventory") {
        const auth = requireAdminKey(request, env);
        if (auth) return auth;

        const client = await getClient(env);
        try {
          const { rows: summary } = await client.query(
            `
            select mode, status, count(*)::int as count
            from public.stream_inventory
            group by mode, status
            order by mode, status
          `
          );
          const { rows: slots } = await client.query(
            `
            select id, mode, status, assigned_event_id,
                   ivs_channel_arn, ivs_playback_url, rtc_stage_arn,
                   created_at, assigned_at, retired_at, error
            from public.stream_inventory
            order by created_at desc
            limit 100
          `
          );
          return json(env, { ok: true, summary, slots });
        } finally {
          await client.end();
        }
      }

      if (method === "POST" && pathname === "/api/admin/inventory/fill") {
        const auth = requireAdminKey(request, env);
        if (auth) return auth;

        const body: any = await request.json().catch(() => ({}));
        const mode = inventoryModeFromInput(body.mode || url.searchParams.get("mode") || "hls");
        const targetAvailable = Math.max(0, Number(body.target_available ?? body.targetAvailable ?? url.searchParams.get("target") ?? 1));

        const client = await getClient(env);
        try {
          const result = await fillInventory(client, env, mode, targetAvailable);
          return json(env, { ok: true, result });
        } finally {
          await client.end();
        }
      }

      if (method === "POST" && pathname === "/api/admin/aws-state/reset-after-manual-cleanup") {
        const auth = requireAdminKey(request, env);
        if (auth) return auth;

        const body: any = await request.json().catch(() => ({}));
        if (body.confirm !== "AWS_IVS_IS_EMPTY") {
          return json(env, { error: "confirmation_required", confirm: "AWS_IVS_IS_EMPTY" }, 400);
        }

        const client = await getClient(env);
        try {
          const result = await resetAwsResourceStateAfterManualCleanup(client);
          return json(env, { ok: true, result });
        } finally {
          await client.end();
        }
      }

      {
        const m = match(pathname, /^\/api\/admin\/inventory\/([^\/]+)\/retire$/);
        if (method === "POST" && m) {
          const auth = requireAdminKey(request, env);
          if (auth) return auth;

          const slotId = m[0];
          const client = await getClient(env);
          try {
            const slot = await retireInventorySlot(client, env, slotId);
            if (!slot) return json(env, { error: "not_found_or_not_retirable" }, 404);
            return json(env, { ok: true, slot });
          } finally {
            await client.end();
          }
        }
      }

      if (method === "POST" && pathname === "/api/test-events") {
        const body: any = await request.json().catch(() => ({}));
        const email = String(body.email || "").trim();
        const title = String(body.title || "Castlink test stream").trim().slice(0, 160);
        const mode: StreamMode = normalizeMode(body.mode || "rtc");
        const ip = requestIp(request);

        if (!email) return json(env, { error: "missing_email" }, 400);
        if (!["rtc", "hls", "both"].includes(mode)) return json(env, { error: "invalid_mode" }, 400);

        const rtcEnabled = mode === "rtc" || mode === "both";
        const hlsEnabled = mode === "hls" || mode === "both";
        const secretKey = randomSecretUrlSafe(24);
        const broadcastKey = randomSecretUrlSafe(24);
        const successToken = randomSecretUrlSafe(24);
        const successTokenHash = await sha256Hex(successToken);
        const viewerLimit = testViewerLimit(env);
        const unusedExpires = new Date(Date.now() + testUnusedExpiryMinutes(env) * 60 * 1000).toISOString();

        const client = await getClient(env);
        try {
          const allowed = await testStreamRequestAllowed(client, env, email, ip);
          if (!allowed.ok) return json(env, { error: "test_limit_reached", ...allowed }, 429);

          const { rows } = await client.query(
            `
            insert into public.events
              (email, title, tier, viewer_limit, white_label,
               status, secret_key, broadcast_key, success_token_hash,
               rtc_enabled, hls_enabled, is_test, test_created_ip, test_expires_unused_at)
            values
              ($1,$2,1,$3,false,'paid',$4,$5,$6,$7,$8,true,$9,$10)
            returning id
          `,
            [email, title || "Castlink test stream", viewerLimit, secretKey, broadcastKey, successTokenHash, rtcEnabled, hlsEnabled, ip || null, unusedExpires]
          );

          const eventId = rows[0].id;
          await client.query(
            `insert into public.test_stream_requests (email, ip_address, event_id) values ($1,$2,$3)`,
            [email, ip || null, eventId]
          );

          await sendEventReadyEmailOnce(client, env, eventId, "test_event").catch((e) => console.error("test event email failed", eventId, e));
          const links = eventLinks(env, { id: eventId, title, secret_key: secretKey, broadcast_key: broadcastKey });

          return json(env, {
            ok: true,
            eventId,
            success_url: `${env.APP_ORIGIN}/success?event=${encodeURIComponent(eventId)}&st=${encodeURIComponent(successToken)}&test=1`,
            broadcast_url: links.broadcastUrl,
            watch_url: links.watchUrl,
            minutes: testStreamMinutes(env),
            viewer_limit: viewerLimit,
          }, 200);
        } finally {
          await client.end();
        }
      }

      if (method === "POST" && pathname === "/api/checkout") {
        const body: any = await request.json().catch(() => ({}));
        const email = String(body.email || "").trim();
        const title = String(body.title || "").trim();
        const tier = Number(body.tier || 3);
        const viewerLimitRaw = body.viewer_limit;
        const viewerLimit = Number.isFinite(Number(viewerLimitRaw))
          ? Number(viewerLimitRaw)
          : Number(env.DEFAULT_VIEWER_LIMIT || 0);
        const whiteLabel = !!body.white_label;
        const viewerEmails = normalizeEmailList(body.viewer_emails || body.viewerEmails || body.viewer_recipient_emails);

        const mode: StreamMode = normalizeMode(body.mode || "rtc");

        if (!email) return json(env, { error: "missing_email" }, 400);
        if (!title) return json(env, { error: "missing_title" }, 400);
        if (![1, 2, 3, 8].includes(tier)) return json(env, { error: "invalid_tier" }, 400);
        if (!tierIsAllowedForMode(tier, mode)) return json(env, { error: "tier_not_available_for_mode" }, 400);

        const rtcEnabled = mode === "rtc" || mode === "both";
        const hlsEnabled = mode === "hls" || mode === "both";

        const secretKey = randomSecretUrlSafe(24);
        const broadcastKey = randomSecretUrlSafe(24);
        const successToken = randomSecretUrlSafe(24);
        const successTokenHash = await sha256Hex(successToken);

        const client = await getClient(env);
        let eventId: string;
        try {
          await ensureReadyEmailColumns(client);
          const { rows } = await client.query(
            `
            insert into public.events
              (email, title, tier, viewer_limit, white_label,
               status, secret_key, broadcast_key, success_token_hash,
               rtc_enabled, hls_enabled, viewer_recipient_emails)
            values
              ($1,$2,$3,$4,$5,'pending',$6,$7,$8,$9,$10,$11::text[])
            returning id
          `,
            [email, title, tier, viewerLimit, whiteLabel, secretKey, broadcastKey, successTokenHash, rtcEnabled, hlsEnabled, viewerEmails]
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
                product_data: { name: `${appName(env)} stream (${tier}h, ${mode})` },
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
          const inventoryMode = inventoryModeForEvent({ rtc_enabled: rtcEnabled, hls_enabled: hlsEnabled });
          if (inventoryMode) {
            const reservation = await reserveInventorySlotForCheckout(client2, eventId, inventoryMode);
            console.log("checkout inventory reservation", JSON.stringify({
              eventId,
              mode: inventoryMode,
              source: reservation.source,
              reserved: reservation.reserved,
              slotId: reservation.slot?.id || null,
            }));
            ctx.waitUntil(
              refillInventoryToMinimum(env, inventoryMode).then((result) => {
                console.log("checkout inventory refill", JSON.stringify({ eventId, mode: inventoryMode, result }));
              }).catch((e) => {
                console.error("checkout inventory refill failed", eventId, inventoryMode, e);
              })
            );
          }
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
          evt = stripe.webhooks.constructEvent(raw as any, sig, env.STRIPE_WEBHOOK_SECRET);
        } catch (e: any) {
          return json(env, { error: "invalid_signature", message: e?.message }, 400);
        }

        if (evt.type === "checkout.session.completed") {
          const session = evt.data.object;
          const eventId = session?.metadata?.relay_event_id;
          const action = session?.metadata?.relay_action || "";

          if (eventId) {
            const client = await getClient(env);
            try {
              if (action === "extend") {
                const minutes = Number(session?.metadata?.relay_extend_minutes || 60);
                const result = await extendEventWindowOnce(client, eventId, minutes, session?.id || null);
                const ev = await getEvent(client, eventId);
                if (result.rows.length) {
                  await sendExtensionEmail(env, ev, minutes).catch((e) => console.error("extension email failed", eventId, e));
                }
              } else if (action === "viewer_upgrade") {
                const amount = Number(session?.metadata?.relay_viewer_upgrade || 0);
                if (allowedViewerUpgrade(amount)) {
                  await client.query(`update public.events set viewer_limit = coalesce(viewer_limit, $2) + $3 where id=$1`, [
                    eventId,
                    Number(env.DEFAULT_VIEWER_LIMIT || 150),
                    amount,
                  ]);
                  const ev = await getEvent(client, eventId);
                  await sendViewerUpgradeEmail(env, ev, amount).catch((e) => console.error("viewer upgrade email failed", eventId, e));
                }
              } else {
                await markPaid(client, eventId);
                await preProvisionPaidEvent(client, env, eventId);
                await sendEventReadyEmailOnce(client, env, eventId, "stripe_webhook").catch((e) => console.error("event ready email failed", eventId, e));
              }
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
            ev = await preProvisionEventIfNeeded(client, env, eventId, ev);

            const readiness = buildReadiness(ev, role, env);
            return json(env, {
              ok: true,
              id: ev.id,
              title: ev.title,
              status: ev.status,
              is_test: !!ev.is_test,
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
          const body: any = await request.json().catch(() => ({}));
          const openWindow = body?.open_window !== false;

          const client = await getClient(env);
          try {
            let ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);

            const role = accessRoleForKey(ev, key);
            if (!role) return json(env, { error: "unauthorized" }, 401);
            if (role !== "broadcaster") return json(env, { error: "broadcaster_key_required" }, 403);
            if (ev.status !== "paid") return json(env, { error: "not_paid" }, 403);
            if (isDisabled(ev)) return json(env, { error: "disabled" }, 403);
            if (isExpired(ev)) return json(env, { error: "expired" }, 410);

            ev = await preProvisionPaidEvent(client, env, eventId);

            let startedWindow = { ev, startedNow: false, startsAtIso: ev.starts_at || null, expiresAtIso: ev.expires_at || null };
            if (openWindow) {
              startedWindow = await ensureEventStartedWindow(client, env, eventId, ev);
              ev = startedWindow.ev;
            }

            const hlsResult = ev.hls_enabled && ev.rtc_enabled
              ? await ensureCompositionStarted(client, env, eventId, ev)
              : { ev, compositionStarted: !!ev.hls_enabled, compositionArn: null };
            ev = hlsResult.ev;

            const readiness = buildReadiness(ev, role, env);

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
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/finish$/);
        if (method === "POST" && m) {
          const eventId = m[0];
          const key = url.searchParams.get("key") || "";

          const client = await getClient(env);
          try {
            let ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);

            const authRes = requireExact(key, ev.broadcast_key, env);
            if (authRes) return authRes;

            const result = await finishEventForAdmin(client, env, eventId, "manual_finish");
            return json(env, {
              ok: true,
              finished: true,
              event: { id: result.ev.id, status: result.ev.status, expires_at: result.ev.expires_at },
              usage: result.usage,
              cleanup: result.cleanup,
            });
          } finally {
            await client.end();
          }
        }
      }

      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/extend\/checkout$/);
        if ((method === "POST" || method === "GET") && m) {
          const eventId = m[0];
          const key = url.searchParams.get("key") || "";

          const client = await getClient(env);
          try {
            const ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);

            const authRes = requireExact(key, ev.broadcast_key, env);
            if (authRes) return authRes;
            if (ev.status !== "paid") return json(env, { error: "not_extendable" }, 400);
            if (isTestEvent(ev)) return json(env, { error: "test_streams_cannot_be_extended" }, 400);
            if (isDisabled(ev)) return json(env, { error: "disabled" }, 403);

            const price = extensionOneHourPrice(env);
            const successUrl = `${env.APP_ORIGIN}/success/?event=${encodeURIComponent(eventId)}&key=${encodeURIComponent(key)}&extension=1&session_id={CHECKOUT_SESSION_ID}`;
            const cancelUrl = `${env.APP_ORIGIN}/broadcast/?event=${encodeURIComponent(eventId)}&key=${encodeURIComponent(key)}&extend_cancelled=1`;
            const stripe = stripeClient(env);
            const session = await stripe.checkout.sessions.create({
              mode: "payment",
              customer_email: ev.email,
              line_items: [
                {
                  price_data: {
                    currency: "nzd",
                    unit_amount: Math.round(price * 100),
                    product_data: { name: `${appName(env)} extension (+1 hour) - ${ev.title || eventId}` },
                  },
                  quantity: 1,
                },
              ],
              success_url: successUrl,
              cancel_url: cancelUrl,
              metadata: {
                relay_event_id: eventId,
                relay_action: "extend",
                relay_extend_minutes: "60",
              },
            });

            if (method === "GET") return Response.redirect(session.url || cancelUrl, 303);
            return json(env, { ok: true, url: session.url, eventId, minutes: 60 }, 200);
          } finally {
            await client.end();
          }
        }
      }

      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/extend\/confirm$/);
        if ((method === "POST" || method === "GET") && m) {
          const eventId = m[0];
          const key = url.searchParams.get("key") || "";
          const sessionId = url.searchParams.get("session_id") || url.searchParams.get("session") || "";
          if (!sessionId) return json(env, { error: "missing_session_id" }, 400);

          const client = await getClient(env);
          try {
            const ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);
            const authRes = requireExact(key, ev.broadcast_key, env);
            if (authRes) return authRes;

            const stripe = stripeClient(env);
            const session = await stripe.checkout.sessions.retrieve(sessionId);
            if (session?.metadata?.relay_event_id !== eventId || session?.metadata?.relay_action !== "extend") {
              return json(env, { error: "invalid_extension_session" }, 400);
            }
            if (session.payment_status !== "paid" && session.status !== "complete") {
              return json(env, { error: "extension_payment_not_complete" }, 402);
            }

            const minutes = Number(session?.metadata?.relay_extend_minutes || 60);
            const result = await extendEventWindowOnce(client, eventId, minutes, session.id || sessionId);
            const updated = await getEvent(client, eventId);
            if (result.rows.length) {
              await sendExtensionEmail(env, updated, minutes).catch((e) => console.error("extension email failed", eventId, e));
            }
            return json(env, {
              ok: true,
              extended: true,
              applied: result.rows.length > 0,
              minutes,
              expires_at: updated?.expires_at || null,
              broadcast_url: eventLinks(env, updated).broadcastUrl,
            });
          } finally {
            await client.end();
          }
        }
      }

      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/viewer-upgrade\/checkout$/);
        if (method === "POST" && m) {
          const eventId = m[0];
          const key = url.searchParams.get("key") || "";
          const body: any = await request.json().catch(() => ({}));
          const amount = Number(body.amount || url.searchParams.get("amount") || 100);
          if (!allowedViewerUpgrade(amount)) return json(env, { error: "invalid_viewer_upgrade" }, 400);

          const client = await getClient(env);
          try {
            const ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);
            const authRes = requireExact(key, ev.broadcast_key, env);
            if (authRes) return authRes;
            if (ev.status !== "paid") return json(env, { error: "not_paid" }, 403);
            if (isTestEvent(ev)) return json(env, { error: "test_streams_cannot_be_upgraded" }, 400);
            if (isDisabled(ev)) return json(env, { error: "disabled" }, 403);
            if (isExpired(ev)) return json(env, { error: "expired" }, 410);

            const price = viewerUpgradePrice(env, amount);
            if (price <= 0) return json(env, { error: "viewer_upgrade_not_configured" }, 500);

            const stripe = stripeClient(env);
            const successUrl = `${env.APP_ORIGIN}/broadcast/?event=${encodeURIComponent(eventId)}&key=${encodeURIComponent(key)}&viewer_upgraded=1`;
            const cancelUrl = `${env.APP_ORIGIN}/broadcast/?event=${encodeURIComponent(eventId)}&key=${encodeURIComponent(key)}&viewer_upgrade_cancelled=1`;
            const session = await stripe.checkout.sessions.create({
              mode: "payment",
              customer_email: ev.email,
              line_items: [
                {
                  price_data: {
                    currency: "nzd",
                    unit_amount: Math.round(price * 100),
                    product_data: { name: `${appName(env)} viewer capacity (+${amount}) - ${ev.title || eventId}` },
                  },
                  quantity: 1,
                },
              ],
              success_url: successUrl,
              cancel_url: cancelUrl,
              metadata: {
                relay_event_id: eventId,
                relay_action: "viewer_upgrade",
                relay_viewer_upgrade: String(amount),
              },
            });

            return json(env, { ok: true, url: session.url, amount, price_nzd: dollarsToCents(price) });
          } finally {
            await client.end();
          }
        }
      }

      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/usage$/);
        if (method === "GET" && m) {
          const eventId = m[0];
          const key = url.searchParams.get("key") || "";

          const client = await getClient(env);
          try {
            const ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);
            const role = accessRoleForKey(ev, key);
            if (!role) return json(env, { error: "unauthorized" }, 401);
            const usage = await usageSummary(client, env, eventId);
            return json(env, { ok: true, usage });
          } finally {
            await client.end();
          }
        }
      }

      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/report$/);
        if (method === "POST" && m) {
          const eventId = m[0];
          const key = url.searchParams.get("key") || "";
          const body: any = await request.json().catch(() => ({}));

          const allowedReasons = new Set(["inappropriate", "illegal", "privacy", "harassment", "technical", "other"]);
          const reason = String(body.reason || "").trim().toLowerCase();
          const description = String(body.description || "").trim().slice(0, 4000);
          const reporterEmail = String(body.reporter_email || body.reporterEmail || "").trim().slice(0, 254);
          const urgent = !!body.urgent;
          const viewerSessionId = String(body.viewer_session_id || body.viewerSessionId || "").trim().slice(0, 200);

          if (!allowedReasons.has(reason)) return json(env, { error: "invalid_reason" }, 400);
          if (!description && reason === "other") return json(env, { error: "missing_description" }, 400);

          const client = await getClient(env);
          try {
            const ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);

            const role = accessRoleForKey(ev, key);
            if (!role) return json(env, { error: "unauthorized" }, 401);

            const usage = await usageSummary(client, env, eventId).catch(() => null);
            const snapshot = {
              id: ev.id,
              title: ev.title,
              status: ev.status,
              starts_at: ev.starts_at || null,
              expires_at: ev.expires_at || null,
              rtc_enabled: !!ev.rtc_enabled,
              hls_enabled: !!ev.hls_enabled,
              role,
              usage,
            };

            const ip =
              request.headers.get("cf-connecting-ip") ||
              request.headers.get("x-forwarded-for") ||
              "";
            const userAgent = request.headers.get("user-agent") || "";

            const { rows } = await client.query(
              `
              insert into public.reports
                (event_id, page, reason, description, reporter_email, urgent,
                 ip_address, user_agent, event_snapshot, viewer_session_id)
              values ($1,'watch',$2,$3,$4,$5,$6,$7,$8::jsonb,$9)
              returning *
            `,
              [
                eventId,
                reason,
                description || null,
                reporterEmail || null,
                urgent,
                ip || null,
                userAgent || null,
                JSON.stringify(snapshot),
                viewerSessionId || null,
              ]
            );

            const report = rows[0];
            await sendReportAlertEmail(env, report, ev).catch((e) => console.error("report alert email failed", report.id, e));
            return json(env, { ok: true, report_id: report.id });
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
            ev = await preProvisionEventIfNeeded(client, env, eventId, ev);
            const links = eventLinks(env, ev);

            return json(env, {
              ok: true,
              watch_url: links.watchUrl,
              broadcast_url: links.broadcastUrl,
              rtc_enabled: !!ev.rtc_enabled,
              hls_enabled: !!ev.hls_enabled,
              is_test: !!ev.is_test,
              playback_url: ev.ivs_playback_url || null,
              readiness: buildReadiness(ev, "broadcaster", env),
            });
          } finally {
            await client.end();
          }
        }
      }

      {
        const m = match(pathname, /^\/api\/events\/([^\/]+)\/email\/resend$/);
        if (method === "POST" && m) {
          const eventId = m[0];
          const st = url.searchParams.get("st");
          const adminKey = request.headers.get("x-relay-admin-key");

          const client = await getClient(env);
          try {
            const ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);

            let authorized = false;
            if (adminKey && env.ADMIN_KEY && adminKey === env.ADMIN_KEY) authorized = true;
            if (st) {
              const stHash = await sha256Hex(st);
              if (stHash === ev.success_token_hash) authorized = true;
            }
            if (!authorized) return json(env, { error: "unauthorized" }, 401);

            const result = await sendEventReadyEmail(env, ev);
            if (!result?.skipped) {
              await ensureReadyEmailColumns(client);
              await client.query(`update public.events set ready_email_sent_at=now(), ready_email_error=null where id=$1`, [eventId]);
            }
            return json(env, { ok: true, result });
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
            ev = await preProvisionEventIfNeeded(client, env, eventId, ev);

            const readiness = buildReadiness(ev, role, env);

            return json(env, {
              ok: true,
              id: ev.id,
              title: ev.title,
              status: ev.status,
              is_test: !!ev.is_test,
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
          const key = url.searchParams.get("key") || "";

          const id = env.SEATS.idFromName(`seats:${eventId}`);
          const stub = env.SEATS.get(id);

          const doUrl = new URL(request.url);
          doUrl.pathname = `/seats/${action}`;
          const client = await getClient(env);
          try {
            const ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);
            if (action === "view-session") {
              const authRes = requireExact(key, ev.secret_key, env);
              if (authRes) return authRes;
              if (ev.status !== "paid") return json(env, { error: "not_paid" }, 403);
              if (isDisabled(ev)) return json(env, { error: "disabled" }, 403);
              if (isExpired(ev)) return json(env, { error: "expired" }, 410);
            }
            doUrl.searchParams.set("limit", String(Number(ev.viewer_limit || env.DEFAULT_VIEWER_LIMIT || 150)));

            const response = await stub.fetch(new Request(doUrl.toString(), request));
            return withCors(env, response);
          } finally {
            await client.end();
          }
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

            const response = await stub.fetch(new Request(doUrl.toString(), request));
            return withCors(env, response);
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
            ev = await maybeRefreshPaidStatus(client, env, ev);
            if (ev.status !== "paid") return json(env, { error: "not_paid" }, 400);
            if (isDisabled(ev)) return json(env, { error: "disabled" }, 403);
            if (isExpired(ev)) return json(env, { error: "expired" }, 410);
            if (!ev.hls_enabled) return json(env, { error: "hls_disabled" }, 400);

            const authRes = requireExact(key, ev.broadcast_key, env);
            if (authRes) return authRes;

            let keyResult = await provisionHlsBroadcast(client, env, eventId, ev);
            ev = keyResult.ev;

            const host = ingestHostFromDb(ev.ivs_ingest_endpoint);
            return json(env, {
              ok: true,
              alreadyProvisioned: !keyResult.created,
              ingest: {
                endpoint: host,
                ingestEndpoint: host,
                rtmpsUrl: rtmpsUrlFromHost(host),
                streamKey: keyResult.streamKeyPlaintext,
              },
              playback: { url: ev.ivs_playback_url },
              readiness: buildReadiness(ev, "broadcaster", env),
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
            if (isDisabled(ev)) return json(env, { error: "disabled" }, 403);
            if (isExpired(ev)) return json(env, { error: "expired" }, 410);
            if (!ev.rtc_enabled) return json(env, { error: "rtc_ingest_disabled" }, 400);

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
              readiness: buildReadiness(ev, "broadcaster", env),
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
            if (isDisabled(ev)) return json(env, { error: "disabled" }, 403);
            if (isExpired(ev)) return json(env, { error: "expired" }, 410);
            if (!ev.rtc_enabled) return json(env, { error: "rtc_ingest_disabled" }, 400);

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
              readiness: buildReadiness(ev, "broadcaster", env),
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
            if (isDisabled(ev)) return json(env, { error: "disabled" }, 403);
            if (isExpired(ev)) return json(env, { error: "expired" }, 410);
            if (!ev.rtc_enabled) return json(env, { error: "rtc_disabled" }, 400);

            const authRes = requireExact(key, ev.secret_key, env);
            if (authRes) return authRes;

            if (!ev.rtc_stage_arn) {
              return json(env, { error: "stage_not_ready", readiness: buildReadiness(ev, "viewer", env) }, 409);
            }
            const rtc = {
              stageArn: ev.rtc_stage_arn as string,
              endpoints: ev.rtc_stage_endpoints ?? null,
            };

            const token = await createParticipantToken(
              env,
              rtc.stageArn,
              `viewer-${randomSecretUrlSafe(8)}`,
              ["SUBSCRIBE"],
              3600
            );

            return json(env, {
              ok: true,
              stageArn: rtc.stageArn,
              participantToken: token.token,
              readiness: buildReadiness(ev, "viewer", env),
            });
          } finally {
            await client.end();
          }
        }
      }

      {
        const m = match(pathname, /^\/api\/admin\/events\/([^\/]+)\/cleanup$/);
        if (method === "POST" && m) {
          const auth = requireAdminKey(request, env);
          if (auth) return auth;

          const eventId = m[0];

          const client = await getClient(env);
          try {
            const ev = await getEvent(client, eventId);
            if (!ev) return json(env, { error: "not_found" }, 404);
            if (ev.status !== "expired" && !isExpired(ev)) {
              return json(env, { error: "not_expired" }, 400);
            }

            if (ev.status !== "expired") {
              await client.query(`update public.events set status='expired', expires_at=coalesce(expires_at, now()) where id=$1`, [eventId]);
            }

            const updated = await getEvent(client, eventId);
            const cleanup = await cleanupEventResources(client, env, updated, "admin_cleanup");
            return json(env, { ok: cleanup.ok, cleanup }, cleanup.ok ? 200 : 502);
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

            await cleanupEventResources(client, { ...env, DELETE_IVS_ON_EXPIRE: "true" }, ev, "admin_delete");

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

            if (!stageArn) return json(env, { error: "stage_not_ready" }, 409);

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
  async scheduled(event: ScheduledEvent, env: any, ctx: ExecutionContext) {
    await handleScheduled(event, env, ctx);
  },
};

export async function scheduled(_event: ScheduledEvent, env: any, _ctx: ExecutionContext) {
  await handleScheduled(_event, env, _ctx);
}
