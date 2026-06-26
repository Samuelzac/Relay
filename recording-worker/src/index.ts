import { Client } from "pg";
import { signAwsJsonRequest, presignS3GetUrl, listS3Objects, deleteS3RecordingObjects, getS3Text, putS3Text } from "./awsSigV4";

type Env = any;

function json(env: Env, body: any, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
      "access-control-allow-origin": env.APP_ORIGIN || "https://castlink.stream",
      "access-control-allow-methods": "GET,POST,OPTIONS",
      "access-control-allow-headers": "content-type,x-relay-recording-secret",
      "access-control-allow-credentials": "true",
      vary: "Origin",
    },
  });
}

async function getClient(env: Env) {
  const client = new Client({ connectionString: env.HYPERDRIVE.connectionString });
  await client.connect();
  return client;
}

function htmlEscape(value: any) {
  return String(value ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" } as any)[c]);
}

function emailButton(label: string, href: string) {
  return `<a href="${htmlEscape(href)}" style="display:inline-block;background-color:#ffffff;color:#111827;text-decoration:none;padding:11px 16px;border-radius:6px;border:1px solid #111827;font-weight:700;mso-padding-alt:0">${htmlEscape(label)}</a>`;
}

function emailShell(brand: string, heading: string, body: string) {
  return `<!doctype html>
    <html>
      <head>
        <meta name="color-scheme" content="light">
        <meta name="supported-color-schemes" content="light">
      </head>
      <body style="margin:0;background-color:#f6f7f9;padding:24px;font-family:Arial,sans-serif;color:#111827">
        <div style="max-width:620px;margin:0 auto;background-color:#ffffff;color:#111827;border:1px solid #e5e7eb;border-radius:10px;padding:24px">
          <div style="font-size:14px;font-weight:700;color:#111827;margin-bottom:18px">${htmlEscape(brand)}</div>
          <h2 style="margin:0 0 14px;font-size:22px;color:#111827">${htmlEscape(heading)}</h2>${body}
        </div>
      </body>
    </html>`;
}

function brand(env: Env) {
  return String(env.BRAND_NAME || "Castlink");
}

async function sendEmail(env: Env, opts: { to: string; subject: string; html: string; text: string; tag?: string }) {
  if (!env.POSTMARK_SERVER_TOKEN) return { skipped: true, reason: "postmark_not_configured" };
  const res = await fetch("https://api.postmarkapp.com/email", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      accept: "application/json",
      "X-Postmark-Server-Token": env.POSTMARK_SERVER_TOKEN,
    },
    body: JSON.stringify({
      From: env.EMAIL_FROM || "Castlink <support@castlink.stream>",
      To: opts.to,
      Subject: opts.subject,
      HtmlBody: opts.html,
      TextBody: opts.text,
      MessageStream: env.POSTMARK_MESSAGE_STREAM || "outbound",
      Tag: opts.tag,
      TrackLinks: "None",
    }),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`postmark_failed_${res.status}: ${text.slice(0, 1000)}`);
  return text ? JSON.parse(text) : {};
}

function slug(value: any) {
  return String(value || "").trim().toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 80);
}

function recordingLink(env: Env, ev: any) {
  return `${env.APP_ORIGIN || "https://castlink.stream"}/success/?event=${encodeURIComponent(ev.id)}&key=${encodeURIComponent(ev.broadcast_key)}&recording=1`;
}

function setupLink(env: Env, ev: any) {
  return `${env.APP_ORIGIN || "https://castlink.stream"}/success/?event=${encodeURIComponent(ev.id)}&key=${encodeURIComponent(ev.broadcast_key)}&setup=1`;
}

function retentionHours(env: Env) {
  const raw = Number(env.RECORDING_RETENTION_HOURS ?? 48);
  return Number.isFinite(raw) && raw > 0 ? Math.min(raw, 168) : 48;
}

function expiryIso(env: Env, endedAt?: string | null) {
  const start = endedAt ? new Date(endedAt).getTime() : Date.now();
  return new Date(start + retentionHours(env) * 3600 * 1000).toISOString();
}

function conversionGraceMinutes(env: Env) {
  const raw = Number(env.RECORDING_CONVERSION_GRACE_MINUTES ?? 5);
  return Number.isFinite(raw) && raw > 0 ? Math.min(Math.floor(raw), 30) : 5;
}

function unusedPaidExpiryHours(env: Env) {
  const raw = Number(env.UNUSED_PAID_EXPIRY_HOURS ?? 4);
  return Number.isFinite(raw) && raw > 0 ? Math.min(Math.floor(raw), 168) : 4;
}

async function ensureColumns(client: Client) {
  await client.query(`
    alter table public.events
      add column if not exists recording_enabled boolean not null default false,
      add column if not exists recording_status text not null default 'not_started',
      add column if not exists recording_s3_bucket text,
      add column if not exists recording_s3_prefix text,
      add column if not exists recording_hls_manifest_key text,
      add column if not exists recording_mp4_s3_key text,
      add column if not exists recording_mp4_job_id text,
      add column if not exists recording_mp4_job_status text,
      add column if not exists recording_started_at timestamptz,
      add column if not exists recording_ended_at timestamptz,
      add column if not exists recording_expires_at timestamptz,
      add column if not exists recording_ivs_channel_arn text,
      add column if not exists recording_webhook_received_at timestamptz,
      add column if not exists recording_webhook_payload jsonb,
      add column if not exists recording_error text,
      add column if not exists recording_email_sent_at timestamptz,
      add column if not exists recording_email_error text,
      add column if not exists recording_download_claimed_at timestamptz,
      add column if not exists recording_download_claimed_ip text,
      add column if not exists recording_cleanup_started_at timestamptz,
      add column if not exists recording_cleanup_completed_at timestamptz,
      add column if not exists recording_cleanup_error text
  `);
  await client.query(`
    create table if not exists public.recording_segments (
      id uuid primary key default gen_random_uuid(),
      event_id uuid not null references public.events(id) on delete cascade,
      channel_arn text not null,
      bucket text not null,
      prefix text,
      manifest_key text not null unique,
      started_at timestamptz,
      ended_at timestamptz,
      last_modified_at timestamptz,
      payload jsonb,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    )
  `);
  await client.query(`create index if not exists recording_segments_event_order_idx on public.recording_segments(event_id, coalesce(started_at, ended_at, last_modified_at, created_at))`);
}

async function getEvent(client: Client, id: string) {
  await ensureColumns(client);
  const { rows } = await client.query(`select * from public.events where id=$1`, [id]);
  return rows[0] || null;
}

function state(ev: any) {
  const expiresAt = ev?.recording_expires_at || null;
  const expired = expiresAt ? new Date(expiresAt).getTime() <= Date.now() : false;
  const claimed = !!ev?.recording_download_claimed_at;
  const ready = String(ev?.recording_status || "") === "ready" && !!ev?.recording_mp4_s3_key && !!ev?.recording_s3_bucket && !expired;
  return {
    enabled: !!ev?.hls_enabled,
    status: expired ? "expired" : String(ev?.recording_status || "not_started"),
    mp4_ready: ready,
    download_available: ready && !claimed,
    download_claimed: claimed,
    download_claimed_at: ev?.recording_download_claimed_at || null,
    expires_at: expiresAt,
    error: ev?.recording_error || null,
    email_sent_at: ev?.recording_email_sent_at || null,
    email_error: ev?.recording_email_error || null,
  };
}

function adminAuthorized(request: Request, env: Env) {
  const got = request.headers.get("x-relay-recording-secret") || "";
  return !!env.RECORDING_WEBHOOK_SECRET && got === env.RECORDING_WEBHOOK_SECRET;
}

async function listStaleEvents(client: Client, env: Env, limit = 50) {
  const safeLimit = Math.max(1, Math.min(Number(limit) || 50, 100));
  const { rows } = await client.query(
    `
    select
      id, title, email, status, tier, created_at, starts_at, expires_at,
      hls_enabled, rtc_enabled, cleanup_completed_at
    from public.events
    where status in ('paid','live')
      and (
        (expires_at is not null and expires_at <= now())
        or (
          starts_at is not null
          and expires_at is null
          and starts_at + ((case when tier in (1,2,3,8) then tier else 1 end) * interval '1 hour') + interval '15 minutes' <= now()
        )
        or (
          starts_at is null
          and created_at <= now() - ($2::int * interval '1 hour')
        )
      )
    order by coalesce(expires_at, starts_at, created_at) asc
    limit $1
  `,
    [safeLimit, unusedPaidExpiryHours(env)]
  );
  return rows;
}

async function expireStaleEvents(client: Client, env: Env, limit = 50) {
  const safeLimit = Math.max(1, Math.min(Number(limit) || 50, 100));
  const { rows } = await client.query(
    `
    with stale as (
      select id
      from public.events
      where status in ('paid','live')
        and (
          (expires_at is not null and expires_at <= now())
          or (
            starts_at is not null
            and expires_at is null
            and starts_at + ((case when tier in (1,2,3,8) then tier else 1 end) * interval '1 hour') + interval '15 minutes' <= now()
          )
          or (
            starts_at is null
            and created_at <= now() - ($2::int * interval '1 hour')
          )
        )
      order by coalesce(expires_at, starts_at, created_at) asc
      limit $1
    )
    update public.events e
    set status='expired',
        expires_at=coalesce(
          e.expires_at,
          case
            when e.starts_at is not null then e.starts_at + ((case when e.tier in (1,2,3,8) then e.tier else 1 end) * interval '1 hour')
            else now()
          end
        )
    from stale
    where e.id=stale.id
    returning
      e.id, e.title, e.email, e.status, e.tier, e.created_at, e.starts_at, e.expires_at,
      e.hls_enabled, e.rtc_enabled, e.cleanup_completed_at
  `,
    [safeLimit, unusedPaidExpiryHours(env)]
  );
  return rows;
}

async function expireCleanedOpenEvents(client: Client, limit = 50) {
  const safeLimit = Math.max(1, Math.min(Number(limit) || 50, 100));
  const { rows } = await client.query(
    `
    with cleaned_open as (
      select id
      from public.events
      where status in ('paid','live')
        and cleanup_completed_at is not null
      order by cleanup_completed_at desc
      limit $1
    )
    update public.events e
    set status='expired',
        expires_at=coalesce(e.expires_at, e.cleanup_completed_at, now()),
        cleanup_error=null
    from cleaned_open
    where e.id=cleaned_open.id
    returning
      e.id, e.title, e.email, e.status, e.tier, e.created_at, e.starts_at, e.expires_at,
      e.hls_enabled, e.rtc_enabled, e.cleanup_completed_at
  `,
    [safeLimit]
  );
  return rows;
}

async function stopExpiredIvsStreams(client: Client, env: Env, limit = 10) {
  const safeLimit = Math.max(1, Math.min(Number(limit) || 10, 25));
  const { rows } = await client.query(
    `
    select id, ivs_channel_arn
    from public.events
    where status='expired'
      and ivs_channel_arn is not null
      and cleanup_completed_at is null
    order by coalesce(expires_at, created_at) asc
    limit $1
  `,
    [safeLimit]
  );
  const results: any[] = [];
  for (const row of rows) {
    try {
      const result = await stopIvsStream(env, row.ivs_channel_arn);
      results.push({ id: row.id, channelArn: row.ivs_channel_arn, ...result });
      if (result.stopped || result.alreadyStopped) {
        await client.query(
          `
          update public.events
          set cleanup_error=case
            when cleanup_error like '%Unable to perform: ivs:DeleteChannel while resource:%is live%' then null
            else cleanup_error
          end
          where id=$1
        `,
          [row.id]
        );
      }
    } catch (e: any) {
      const message = String(e?.message || e).slice(0, 2000);
      results.push({ id: row.id, channelArn: row.ivs_channel_arn, stopped: false, error: message });
      await client.query(`update public.events set cleanup_error=$2 where id=$1`, [row.id, `stopIvsStream: ${message}`.slice(0, 2000)]);
    }
  }
  return results;
}

async function listOpenEvents(client: Client, limit = 50) {
  const safeLimit = Math.max(1, Math.min(Number(limit) || 50, 200));
  const { rows } = await client.query(
    `
    select
      id, title, email, status, tier, created_at, starts_at, expires_at,
      hls_enabled, rtc_enabled, ivs_channel_arn, ivs_playback_url,
      cleanup_completed_at
    from public.events
    where status in ('paid','live')
    order by created_at desc
    limit $1
  `,
    [safeLimit]
  );
  return rows;
}

async function listAssignedInventory(client: Client, limit = 50) {
  const safeLimit = Math.max(1, Math.min(Number(limit) || 50, 200));
  const { rows } = await client.query(
    `
    select
      si.id, si.mode, si.status, si.assigned_event_id, si.created_at, si.assigned_at, si.retired_at,
      e.title as event_title, e.email as event_email, e.status as event_status,
      e.created_at as event_created_at, e.starts_at as event_starts_at, e.expires_at as event_expires_at,
      e.cleanup_completed_at as event_cleanup_completed_at
    from public.stream_inventory si
    left join public.events e on e.id=si.assigned_event_id
    where si.status='assigned'
    order by si.assigned_at desc nulls last, si.created_at desc
    limit $1
  `,
    [safeLimit]
  );
  return rows;
}

async function listRecentRecordings(client: Client, limit = 25) {
  await ensureColumns(client);
  const safeLimit = Math.max(1, Math.min(Number(limit) || 25, 100));
  const { rows } = await client.query(
    `
    select
      id, title, email, status, tier, created_at, starts_at, expires_at,
      hls_enabled, rtc_enabled, ivs_channel_arn, ivs_playback_url,
      cleanup_started_at, cleanup_completed_at, cleanup_error,
      recording_enabled, recording_status, recording_s3_bucket, recording_s3_prefix,
      recording_hls_manifest_key, recording_mp4_s3_key, recording_mp4_job_id,
      recording_mp4_job_status, recording_started_at, recording_ended_at,
      recording_expires_at, recording_ivs_channel_arn, recording_error,
      recording_webhook_received_at, recording_webhook_payload,
      recording_email_sent_at, recording_email_error,
      (
        select si.ivs_channel_arn
        from public.stream_inventory si
        where si.assigned_event_id=events.id
          and si.ivs_channel_arn is not null
        order by si.assigned_at desc nulls last, si.created_at desc
        limit 1
      ) as inventory_ivs_channel_arn
    from public.events
    where hls_enabled=true
    order by created_at desc
    limit $1
  `,
    [safeLimit]
  );
  return rows;
}

async function listDuplicateRecordings(client: Client) {
  const { rows } = await client.query(
    `
    select
      recording_hls_manifest_key,
      count(*)::int as count,
      json_agg(json_build_object(
        'id', id,
        'title', title,
        'email', email,
        'status', status,
        'recording_status', recording_status,
        'ivs_channel_arn', ivs_channel_arn,
        'recording_mp4_s3_key', recording_mp4_s3_key,
        'recording_email_sent_at', recording_email_sent_at,
        'recording_download_claimed_at', recording_download_claimed_at,
        'created_at', created_at,
        'starts_at', starts_at,
        'expires_at', expires_at
      ) order by created_at desc) as events
    from public.events
    where recording_hls_manifest_key is not null
    group by recording_hls_manifest_key
    having count(*) > 1
    order by count(*) desc
  `
  );
  return rows;
}

async function repairMismatchedRecordingClaims(client: Client) {
  const { rows } = await client.query(
    `
    update public.events
    set recording_status='failed',
        recording_s3_bucket=null,
        recording_s3_prefix=null,
        recording_hls_manifest_key=null,
        recording_mp4_s3_key=null,
        recording_mp4_job_id=null,
        recording_mp4_job_status=null,
        recording_error='recording_manifest_channel_mismatch_repaired',
        recording_email_error=coalesce(recording_email_error, 'recording_manifest_channel_mismatch_repaired')
    where recording_hls_manifest_key is not null
      and ivs_channel_arn is not null
      and position('/' || regexp_replace(ivs_channel_arn, '^.*/', '') || '/' in recording_hls_manifest_key) = 0
    returning id, title, email, status, recording_status, ivs_channel_arn, recording_error
  `
  );
  return rows;
}

async function repairDuplicateRecordingClaims(client: Client) {
  const { rows } = await client.query(
    `
    with duplicate_keys as (
      select recording_hls_manifest_key
      from public.events
      where recording_hls_manifest_key is not null
      group by recording_hls_manifest_key
      having count(*) > 1
    )
    update public.events e
    set recording_status='failed',
        recording_s3_bucket=null,
        recording_s3_prefix=null,
        recording_hls_manifest_key=null,
        recording_mp4_s3_key=null,
        recording_mp4_job_id=null,
        recording_mp4_job_status=null,
        recording_error='duplicate_recording_manifest_repaired',
        recording_email_error=coalesce(recording_email_error, 'duplicate_recording_manifest_repaired')
    from duplicate_keys d
    where e.recording_hls_manifest_key=d.recording_hls_manifest_key
    returning e.id, e.title, e.email, e.status, e.recording_status, e.recording_error
  `
  );
  return rows;
}

function deepString(input: any, keys: string[]) {
  const wanted = new Set(keys.map((k) => k.toLowerCase()));
  const stack = [input];
  const seen = new Set<any>();
  while (stack.length) {
    const item = stack.pop();
    if (!item || typeof item !== "object" || seen.has(item)) continue;
    seen.add(item);
    for (const [k, v] of Object.entries(item)) {
      if (wanted.has(k.toLowerCase()) && typeof v === "string" && v.trim()) return v.trim();
      if (v && typeof v === "object") stack.push(v);
    }
  }
  return "";
}

function extractRecordingEvent(input: any, env: Env) {
  const channelArn = deepString(input, ["channelArn", "channel_arn", "ChannelArn"]) || (JSON.stringify(input).match(/arn:aws:ivs:[^"]+:channel\/[^",\]}]+/) || [])[0] || "";
  const bucket = deepString(input, ["recordingS3BucketName", "recording_s3_bucket_name", "s3BucketName", "s3_bucket_name", "bucketName", "bucket_name", "bucket"]) || String(env.RECORDINGS_S3_BUCKET || "");
  let manifestKey = deepString(input, ["recordingHlsManifestKey", "recording_hls_manifest_key", "hlsManifestKey", "hls_manifest_key", "manifestKey", "manifest_key", "s3ObjectKey", "s3_object_key", "objectKey", "object_key", "key"]).replace(/^s3:\/\/[^/]+\//, "");
  const prefix = (deepString(input, ["recordingS3KeyPrefix", "recording_s3_key_prefix", "s3KeyPrefix", "s3_key_prefix", "prefix"]).replace(/^s3:\/\/[^/]+\//, "") || (manifestKey ? manifestKey.replace(/\/media\/hls\/master\.m3u8$/, "") : ""));
  if (!manifestKey && prefix) manifestKey = `${prefix.replace(/\/+$/, "")}/media/hls/master.m3u8`;
  const rawStatus = deepString(input, ["recordingStatus", "recording_status", "recordingState", "recording_state", "status", "state"]).toUpperCase();
  const endedAt = deepString(input, ["recordingEndTime", "recording_end_time", "endTime", "end_time", "endedAt", "ended_at"]) || input?.time || null;
  const startedAt = deepString(input, ["recordingStartTime", "recording_start_time", "startTime", "start_time", "startedAt", "started_at"]) || null;
  return { channelArn, bucket, prefix, manifestKey, startedAt, endedAt, ended: !!endedAt || ["ENDED", "STOPPED", "COMPLETE", "COMPLETED"].includes(rawStatus) };
}

function mcEndpoint(env: Env) {
  return String(env.MEDIACONVERT_ENDPOINT || `https://mediaconvert.${env.AWS_REGION || "ap-northeast-1"}.amazonaws.com`).replace(/\/+$/, "");
}

function mediaConvertJob(data: any) {
  return data?.Job || data?.job || data;
}

function ivsEndpoint(env: Env) {
  const region = String(env.AWS_REGION || "ap-northeast-1");
  return String(env.IVS_API_ENDPOINT || `https://ivs.${region}.amazonaws.com`).replace(/\/+$/, "");
}

async function ivsCall(env: Env, path: string, payload: any) {
  const body = JSON.stringify(payload || {});
  const url = `${ivsEndpoint(env)}${path}`;
  const headers = await signAwsJsonRequest({
    method: "POST",
    url,
    body,
    accessKeyId: String(env.AWS_ACCESS_KEY_ID).trim(),
    secretAccessKey: String(env.AWS_SECRET_ACCESS_KEY).trim(),
    region: String(env.AWS_REGION || "ap-northeast-1"),
    service: "ivs",
    headers: { accept: "application/json", "content-type": "application/json" },
  });
  const res = await fetch(url, { method: "POST", headers, body });
  const text = await res.text();
  const data = text ? JSON.parse(text) : {};
  if (!res.ok) throw new Error(`ivs_${path}_${res.status}: ${text.slice(0, 1000)}`);
  return data;
}

function isIgnorableStopStreamError(error: any) {
  const msg = String(error?.message || error || "");
  return (
    msg.includes("ChannelNotBroadcasting") ||
    msg.includes("ResourceNotFoundException") ||
    msg.includes("NotFoundException") ||
    msg.includes("404")
  );
}

async function stopIvsStream(env: Env, channelArn: string) {
  try {
    await ivsCall(env, "/StopStream", { channelArn });
    return { stopped: true };
  } catch (e: any) {
    if (isIgnorableStopStreamError(e)) return { stopped: false, alreadyStopped: true, message: String(e?.message || e) };
    throw e;
  }
}

async function prepareRecordingChannel(client: Client, env: Env, eventId: string) {
  await ensureColumns(client);
  const ev = await getEvent(client, eventId);
  if (!ev) throw new Error("event_not_found");
  if (!ev.hls_enabled) return { skipped: true, reason: "hls_not_enabled" };
  if (!ev.ivs_channel_arn) return { skipped: true, reason: "ivs_channel_missing" };
  const recordingArn = String(env.RECORDING_CONFIGURATION_ARN || "").trim();
  if (!recordingArn) throw new Error("recording_configuration_arn_missing");
  const got = await ivsCall(env, "/GetChannel", { arn: ev.ivs_channel_arn });
  const current = got?.channel?.recordingConfigurationArn || "";
  if (current === recordingArn) {
    await client.query(`update public.events set recording_enabled=true, recording_ivs_channel_arn=coalesce(recording_ivs_channel_arn, ivs_channel_arn) where id=$1`, [eventId]);
    return { prepared: true, already: true, channelArn: ev.ivs_channel_arn, recordingConfigurationArn: current };
  }
  await ivsCall(env, "/UpdateChannel", {
    arn: ev.ivs_channel_arn,
    recordingConfigurationArn: recordingArn,
  });
  await client.query(`update public.events set recording_enabled=true, recording_ivs_channel_arn=coalesce(recording_ivs_channel_arn, ivs_channel_arn) where id=$1`, [eventId]);
  return { prepared: true, updated: true, channelArn: ev.ivs_channel_arn, recordingConfigurationArn: recordingArn };
}

async function mediaConvert(env: Env, method: "GET" | "POST", path: string, payload?: any) {
  const body = payload === undefined ? "" : JSON.stringify(payload);
  const url = `${mcEndpoint(env)}${path}`;
  const headers = await signAwsJsonRequest({
    method, url, body,
    accessKeyId: String(env.AWS_ACCESS_KEY_ID).trim(),
    secretAccessKey: String(env.AWS_SECRET_ACCESS_KEY).trim(),
    region: String(env.AWS_REGION || "ap-northeast-1"),
    service: "mediaconvert",
    headers: { accept: "application/json", "content-type": "application/json" },
  });
  const res = await fetch(url, { method, headers, body: body || undefined });
  const text = await res.text();
  const data = text ? JSON.parse(text) : {};
  if (!res.ok) throw new Error(`mediaconvert_${method}_${res.status}: ${text.slice(0, 1000)}`);
  return data?.Job || data?.job || data;
}

function mp4OutputKey(env: Env, ev: any) {
  const base = String(ev.recording_s3_prefix || "castlink-recordings").replace(/^\/+|\/+$/g, "");
  const modifier = String(env.RECORDING_MP4_NAME_MODIFIER || "-castlink");
  return `${base}/mp4/${ev.id}/master${modifier}.mp4`;
}

function ivsResourceId(arn: any) {
  const match = String(arn || "").match(/\/([^/]+)$/);
  return match?.[1] || "";
}

async function exactRecordingChannelArn(client: Client, ev: any) {
  if (ev?.ivs_channel_arn) return ev.ivs_channel_arn;
  if (ev?.recording_ivs_channel_arn) return ev.recording_ivs_channel_arn;
  const { rows } = await client.query(
    `
    select ivs_channel_arn
    from public.stream_inventory
    where assigned_event_id=$1
      and ivs_channel_arn is not null
    order by assigned_at desc nulls last, created_at desc
    limit 1
  `,
    [ev.id]
  );
  return rows[0]?.ivs_channel_arn || "";
}

async function upsertRecordingSegment(client: Client, opts: {
  eventId: string;
  channelArn: string;
  bucket: string;
  prefix?: string | null;
  manifestKey: string;
  startedAt?: string | null;
  endedAt?: string | null;
  lastModifiedAt?: string | null;
  payload?: any;
}) {
  const manifestKey = String(opts.manifestKey || "").replace(/^s3:\/\/[^/]+\//, "").trim();
  if (!manifestKey) return { skipped: true, reason: "manifest_key_missing" };
  const prefix = String(opts.prefix || manifestKey.replace(/\/media\/hls\/master\.m3u8$/, "")).replace(/^s3:\/\/[^/]+\//, "").replace(/\/+$/, "");
  const { rows } = await client.query(
    `
    insert into public.recording_segments (
      event_id, channel_arn, bucket, prefix, manifest_key,
      started_at, ended_at, last_modified_at, payload
    )
    values ($1,$2,$3,$4,$5,$6::timestamptz,$7::timestamptz,$8::timestamptz,$9::jsonb)
    on conflict (manifest_key) do update
    set event_id=case
          when recording_segments.event_id=excluded.event_id then excluded.event_id
          else recording_segments.event_id
        end,
        channel_arn=case
          when recording_segments.event_id=excluded.event_id then excluded.channel_arn
          else recording_segments.channel_arn
        end,
        bucket=case
          when recording_segments.event_id=excluded.event_id then excluded.bucket
          else recording_segments.bucket
        end,
        prefix=case
          when recording_segments.event_id=excluded.event_id then coalesce(excluded.prefix, recording_segments.prefix)
          else recording_segments.prefix
        end,
        started_at=case
          when recording_segments.event_id=excluded.event_id then coalesce(recording_segments.started_at, excluded.started_at)
          else recording_segments.started_at
        end,
        ended_at=case
          when recording_segments.event_id=excluded.event_id then coalesce(excluded.ended_at, recording_segments.ended_at)
          else recording_segments.ended_at
        end,
        last_modified_at=case
          when recording_segments.event_id=excluded.event_id then coalesce(excluded.last_modified_at, recording_segments.last_modified_at)
          else recording_segments.last_modified_at
        end,
        payload=case
          when recording_segments.event_id=excluded.event_id then coalesce(excluded.payload, recording_segments.payload)
          else recording_segments.payload
        end,
        updated_at=now()
    returning id, event_id, manifest_key, event_id=$1::uuid as belongs_to_event
  `,
    [
      opts.eventId,
      opts.channelArn,
      opts.bucket,
      prefix || null,
      manifestKey,
      opts.startedAt || null,
      opts.endedAt || null,
      opts.lastModifiedAt || null,
      opts.payload ? JSON.stringify(opts.payload) : null,
    ]
  );
  const row = rows[0];
  if (row && !row.belongs_to_event) throw new Error(`recording_segment_manifest_already_claimed ${manifestKey}`);
  return { upserted: true, segment: row };
}

async function recordingSegmentsForEvent(client: Client, eventId: string) {
  const { rows } = await client.query(
    `
    select *
    from public.recording_segments
    where event_id=$1
    order by coalesce(started_at, ended_at, last_modified_at, created_at) asc, created_at asc
  `,
    [eventId]
  );
  return rows;
}

function dirname(key: string) {
  const i = String(key || "").lastIndexOf("/");
  return i >= 0 ? key.slice(0, i) : "";
}

function normalizeKeyPath(key: string) {
  const parts: string[] = [];
  for (const part of String(key || "").split("/")) {
    if (!part || part === ".") continue;
    if (part === "..") parts.pop();
    else parts.push(part);
  }
  return parts.join("/");
}

function resolvePlaylistUri(baseKey: string, uri: string) {
  const clean = String(uri || "").trim();
  if (/^[a-z][a-z0-9+.-]*:/i.test(clean)) return clean;
  return normalizeKeyPath(`${dirname(baseKey)}/${clean}`);
}

function relativeUri(fromKey: string, toKey: string) {
  const fromParts = dirname(fromKey).split("/").filter(Boolean);
  const toParts = String(toKey || "").split("/").filter(Boolean);
  let i = 0;
  while (i < fromParts.length && i < toParts.length && fromParts[i] === toParts[i]) i += 1;
  return [...Array(fromParts.length - i).fill(".."), ...toParts.slice(i)].join("/") || toParts[toParts.length - 1] || "";
}

function commonPrefixPath(keys: string[]) {
  if (!keys.length) return "";
  const split = keys.map((key) => String(key || "").split("/").filter(Boolean));
  const out: string[] = [];
  for (let i = 0; i < split[0].length; i++) {
    const part = split[0][i];
    if (split.every((parts) => parts[i] === part)) out.push(part);
    else break;
  }
  return out.join("/");
}

function firstVariantUri(master: string) {
  const lines = master.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  for (let i = 0; i < lines.length; i++) {
    if (lines[i].startsWith("#EXT-X-STREAM-INF")) {
      const next = lines.slice(i + 1).find((line) => line && !line.startsWith("#"));
      if (next) return next;
    }
  }
  return lines.find((line) => line && !line.startsWith("#")) || "";
}

function rewriteUriAttributes(line: string, sourcePlaylistKey: string, combinedKey: string) {
  return line.replace(/URI="([^"]+)"/g, (_m, uri) => {
    const absoluteKey = resolvePlaylistUri(sourcePlaylistKey, uri);
    if (/^[a-z][a-z0-9+.-]*:/i.test(absoluteKey)) return `URI="${absoluteKey}"`;
    return `URI="${relativeUri(combinedKey, absoluteKey)}"`;
  });
}

function mediaPlaylistBodyForCombine(media: string, sourcePlaylistKey: string, combinedKey: string, includeHeader: boolean) {
  const out: string[] = [];
  for (const rawLine of media.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line) continue;
    if (line === "#EXTM3U") {
      if (includeHeader) out.push(line);
      continue;
    }
    if (/^#EXT-X-(VERSION|MEDIA-SEQUENCE|PLAYLIST-TYPE|INDEPENDENT-SEGMENTS)/.test(line)) {
      if (includeHeader) out.push(line);
      continue;
    }
    if (line.startsWith("#EXT-X-TARGETDURATION")) continue;
    if (line.startsWith("#EXT-X-ENDLIST")) continue;
    if (line.startsWith("#EXT-X-MAP")) {
      out.push(rewriteUriAttributes(line, sourcePlaylistKey, combinedKey));
      continue;
    }
    if (line.startsWith("#")) {
      out.push(line);
      continue;
    }
    const absoluteKey = resolvePlaylistUri(sourcePlaylistKey, line);
    out.push(/^[a-z][a-z0-9+.-]*:/i.test(absoluteKey) ? absoluteKey : relativeUri(combinedKey, absoluteKey));
  }
  return out;
}

function targetDurationForManifest(lines: string[]) {
  let maxDuration = 1;
  for (const line of lines) {
    const match = String(line || "").match(/^#EXTINF:([0-9.]+)/);
    if (!match) continue;
    const duration = Number(match[1]);
    if (Number.isFinite(duration)) maxDuration = Math.max(maxDuration, Math.ceil(duration));
  }
  return maxDuration;
}

function mp4OutputKeyForManifest(outputPrefix: string, inputManifestKey: string, env: Env) {
  const modifier = String(env.RECORDING_MP4_NAME_MODIFIER || "-castlink");
  const file = String(inputManifestKey || "").split("/").pop() || "master.m3u8";
  const base = file.replace(/\.[^.]+$/, "") || "master";
  return `${outputPrefix}/${base}${modifier}.mp4`;
}

async function buildCombinedHlsManifest(env: Env, bucket: string, segments: any[], outputPrefix: string, eventId: string) {
  if (segments.length <= 1) return { inputManifestKey: segments[0]?.manifest_key || "", combined: false, segmentCount: segments.length };
  const commonPrefix = commonPrefixPath(segments.map((segment: any) => String(segment.prefix || dirname(segment.manifest_key))));
  const combinedKey = `${commonPrefix || outputPrefix}/castlink-combined-${eventId}.m3u8`;
  const combinedLines: string[] = [];

  for (let i = 0; i < segments.length; i++) {
    const segment = segments[i];
    const masterKey = segment.manifest_key;
    const master = await getS3Text(env, bucket, masterKey);
    const variant = firstVariantUri(master);
    const mediaKey = variant ? resolvePlaylistUri(masterKey, variant) : masterKey;
    if (/^[a-z][a-z0-9+.-]*:/i.test(mediaKey)) throw new Error(`unsupported_external_hls_variant ${mediaKey}`);
    const media = await getS3Text(env, bucket, mediaKey);
    combinedLines.push(...mediaPlaylistBodyForCombine(media, mediaKey, combinedKey, i === 0));
  }

  const targetDuration = targetDurationForManifest(combinedLines);
  const headerIndex = combinedLines.findIndex((line) => line === "#EXTM3U");
  combinedLines.splice(headerIndex >= 0 ? headerIndex + 1 : 0, 0, `#EXT-X-TARGETDURATION:${targetDuration}`);
  combinedLines.push("#EXT-X-ENDLIST");
  const body = `${combinedLines.join("\n")}\n`;
  await putS3Text(env, bucket, combinedKey, body);
  return { inputManifestKey: combinedKey, combined: true, segmentCount: segments.length };
}

async function recordingDebugForEvent(client: Client, eventId: string) {
  await ensureColumns(client);
  const ev = await getEvent(client, eventId);
  if (!ev) return null;
  const segments = await recordingSegmentsForEvent(client, eventId);
  return {
    event: {
      id: ev.id,
      title: ev.title,
      status: ev.status,
      hls_enabled: !!ev.hls_enabled,
      rtc_enabled: !!ev.rtc_enabled,
      expires_at: ev.expires_at || null,
      cleanup_completed_at: ev.cleanup_completed_at || null,
      recording_status: ev.recording_status || null,
      recording_mp4_job_id: ev.recording_mp4_job_id || null,
      recording_mp4_job_status: ev.recording_mp4_job_status || null,
      recording_mp4_s3_key: ev.recording_mp4_s3_key || null,
      recording_email_sent_at: ev.recording_email_sent_at || null,
      recording_error: ev.recording_error || null,
      recording_webhook_received_at: ev.recording_webhook_received_at || null,
    },
    segment_count: segments.length,
    segments: segments.map((segment: any) => ({
      manifest_key: segment.manifest_key,
      prefix: segment.prefix,
      bucket: segment.bucket,
      started_at: segment.started_at || null,
      ended_at: segment.ended_at || null,
      last_modified_at: segment.last_modified_at || null,
      created_at: segment.created_at || null,
      updated_at: segment.updated_at || null,
      recording_session_id: segment.payload?.detail?.recording_session_id || null,
      stream_ids: segment.payload?.detail?.recording_session_stream_ids || null,
      duration_ms: segment.payload?.detail?.recording_duration_ms || null,
      payload_time: segment.payload?.time || null,
    })),
  };
}

async function ingestStoredWebhookPayload(client: Client, env: Env, eventId: string) {
  const ev = await getEvent(client, eventId);
  if (!ev?.recording_webhook_payload) return { skipped: true, reason: "webhook_payload_missing" };
  const rec = extractRecordingEvent(ev.recording_webhook_payload, env);
  if (!rec.channelArn || !rec.bucket || !rec.manifestKey) return { skipped: true, reason: "webhook_recording_fields_missing", extracted: rec };
  await upsertRecordingSegment(client, {
    eventId,
    channelArn: rec.channelArn,
    bucket: rec.bucket,
    prefix: rec.prefix || null,
    manifestKey: rec.manifestKey,
    startedAt: rec.startedAt || null,
    endedAt: rec.endedAt || null,
    payload: ev.recording_webhook_payload,
  });
  await client.query(
    `
    update public.events
    set recording_enabled=true,
        recording_status='processing',
        recording_s3_bucket=$2,
        recording_s3_prefix=coalesce(recording_s3_prefix, $3),
        recording_hls_manifest_key=coalesce(recording_hls_manifest_key, $4),
        recording_ivs_channel_arn=coalesce(recording_ivs_channel_arn, $5),
        recording_ended_at=coalesce(recording_ended_at, $6::timestamptz, now()),
        recording_expires_at=coalesce(recording_expires_at, $7::timestamptz),
        recording_error=null
    where id=$1
  `,
    [eventId, rec.bucket, rec.prefix || null, rec.manifestKey, rec.channelArn, rec.endedAt || null, expiryIso(env, rec.endedAt || null)]
  );
  return { ingested: true, extracted: rec };
}

async function startConversion(client: Client, env: Env, eventId: string) {
  const ev = await getEvent(client, eventId);
  const existingJobStatus = String(ev?.recording_mp4_job_status || "").toUpperCase();
  if (ev?.recording_mp4_job_id && !["ERROR", "CANCELED"].includes(existingJobStatus)) {
    return { skipped: true, reason: "conversion_already_submitted", jobId: ev.recording_mp4_job_id, status: ev.recording_mp4_job_status || null };
  }
  const segments = await recordingSegmentsForEvent(client, eventId);
  const firstSegment = segments[0] || null;
  const bucket = firstSegment?.bucket || ev.recording_s3_bucket;
  if (!bucket) return { skipped: true, reason: "recording_bucket_missing" };
  const basePrefix = String(firstSegment?.prefix || ev.recording_s3_prefix || "castlink-recordings").replace(/^\/+|\/+$/g, "");
  const outputPrefix = `${basePrefix}/mp4/${ev.id}`;
  const manifest = segments.length
    ? await buildCombinedHlsManifest(env, bucket, segments, outputPrefix, eventId)
    : ev?.recording_hls_manifest_key
      ? { inputManifestKey: ev.recording_hls_manifest_key, combined: false, segmentCount: 1 }
      : { inputManifestKey: "", combined: false, segmentCount: 0 };
  if (!manifest.inputManifestKey) return { skipped: true, reason: "recording_segments_missing" };
  const inputs = [{
    FileInput: `s3://${bucket}/${manifest.inputManifestKey}`,
    AudioSelectors: { "Audio Selector 1": { DefaultSelection: "DEFAULT" } },
    VideoSelector: {},
    TimecodeSource: "ZEROBASED",
  }];
  const job = await mediaConvert(env, "POST", "/2017-08-29/jobs", {
    Role: env.MEDIACONVERT_ROLE_ARN,
    UserMetadata: { app: "castlink", event_id: eventId, segment_count: String(manifest.segmentCount), combined_manifest: String(manifest.combined) },
    Settings: {
      TimecodeConfig: { Source: "ZEROBASED" },
      Inputs: inputs,
      OutputGroups: [{
        Name: "MP4",
        OutputGroupSettings: { Type: "FILE_GROUP_SETTINGS", FileGroupSettings: { Destination: `s3://${bucket}/${outputPrefix}/` } },
        Outputs: [{
          NameModifier: String(env.RECORDING_MP4_NAME_MODIFIER || "-castlink"),
          ContainerSettings: { Container: "MP4", Mp4Settings: {} },
          VideoDescription: { CodecSettings: { Codec: "H_264", H264Settings: { RateControlMode: "QVBR", QvbrSettings: { QvbrQualityLevel: 7 }, MaxBitrate: 5000000, QualityTuningLevel: "SINGLE_PASS", GopSize: 2, GopSizeUnits: "SECONDS", NumberBFramesBetweenReferenceFrames: 2 } } },
          AudioDescriptions: [{ AudioSourceName: "Audio Selector 1", CodecSettings: { Codec: "AAC", AacSettings: { Bitrate: 128000, CodingMode: "CODING_MODE_2_0", SampleRate: 48000 } } }],
        }],
      }],
    },
  });
  const jobId = job?.Id || job?.id;
  const outputKey = mp4OutputKeyForManifest(outputPrefix, manifest.inputManifestKey, env);
  const updated = await client.query(
    `
    update public.events
    set recording_status='processing',
        recording_s3_bucket=coalesce(recording_s3_bucket, $5),
        recording_s3_prefix=coalesce(recording_s3_prefix, $6),
        recording_hls_manifest_key=coalesce(recording_hls_manifest_key, $7),
        recording_mp4_job_id=$2,
        recording_mp4_job_status=$3,
        recording_mp4_s3_key=$4,
        recording_error=null
    where id=$1
    returning id, recording_status, recording_mp4_job_id, recording_mp4_job_status, recording_mp4_s3_key
  `,
    [eventId, jobId, job?.Status || "SUBMITTED", outputKey, bucket, basePrefix, firstSegment?.manifest_key || ev.recording_hls_manifest_key || null]
  );
  const persisted = updated.rows[0] || null;
  if (!persisted || persisted.recording_mp4_job_id !== jobId) {
    throw new Error(`recording_job_update_failed ${eventId}`);
  }
  return { submitted: true, jobId, outputKey, segmentCount: manifest.segmentCount, combinedManifest: manifest.combined, inputManifestKey: manifest.inputManifestKey, persisted };
}

async function discoverManifestForEvent(client: Client, env: Env, eventId: string) {
  await ensureColumns(client);
  const ev = await getEvent(client, eventId);
  if (!ev) throw new Error("event_not_found");
  if (!ev.hls_enabled) throw new Error("hls_not_enabled");
  const bucket = String(env.RECORDINGS_S3_BUCKET || "").trim();
  if (!bucket) throw new Error("recording_bucket_required");
  const channelArn = await exactRecordingChannelArn(client, ev);
  const channelId = ivsResourceId(channelArn);
  if (!channelId) throw new Error("recording_channel_missing_for_discovery");
  const basePrefix = String(env.RECORDINGS_S3_SEARCH_PREFIX || "ivs/v1/557904961613/").replace(/^\/+|\/+$/g, "");
  const channelPrefix = `${basePrefix}/${channelId}/`;
  let manifests = (await listS3Objects(env, bucket, channelPrefix, 10000))
    .filter((obj) => /\/media\/hls\/master\.m3u8$/.test(obj.key))
    .map((obj) => ({ ...obj, t: obj.lastModified ? new Date(obj.lastModified).getTime() : 0 }))
    .sort((a, b) => b.t - a.t);
  if (!manifests.length) throw new Error(`recording_manifest_not_found_for_channel ${channelId} in ${channelPrefix}`);
  const candidateKeys = manifests.map((obj) => obj.key);
  const { rows: alreadyUsed } = candidateKeys.length
    ? await client.query(
        `
        select recording_hls_manifest_key
        from public.events
        where id <> $1
          and recording_hls_manifest_key = any($2::text[])
      `,
        [eventId, candidateKeys]
      )
    : { rows: [] as any[] };
  const alreadyUsedKeys = new Set(alreadyUsed.map((row: any) => row.recording_hls_manifest_key));
  manifests = manifests.filter((obj) => !alreadyUsedKeys.has(obj.key));
  if (!manifests.length) {
    throw new Error(`recording_manifest_for_channel_already_claimed ${channelId}`);
  }
  if (manifests.length > 1) {
    throw new Error(`recording_manifest_ambiguous_for_channel ${channelId}: ${manifests.map((obj) => obj.key).join(", ")}`);
  }
  const found = manifests[0];
  const prefix = found.key.replace(/\/media\/hls\/master\.m3u8$/, "");
  await upsertRecordingSegment(client, {
    eventId,
    channelArn,
    bucket,
    prefix,
    manifestKey: found.key,
    endedAt: found.lastModified || null,
    lastModifiedAt: found.lastModified || null,
  });
  await client.query(
    `
    update public.events
    set recording_enabled=true,
        recording_status='processing',
        recording_s3_bucket=$2,
        recording_s3_prefix=$3,
        recording_hls_manifest_key=$4,
        recording_ivs_channel_arn=coalesce(recording_ivs_channel_arn, $7),
        recording_ended_at=coalesce(recording_ended_at, $5::timestamptz, now()),
        recording_expires_at=coalesce(recording_expires_at, $6::timestamptz),
        recording_error=null,
        recording_mp4_job_id=null,
        recording_mp4_job_status=null,
        recording_mp4_s3_key=null,
        recording_email_sent_at=null,
        recording_email_error=null
    where id=$1
  `,
    [eventId, bucket, prefix, found.key, found.lastModified || null, expiryIso(env, found.lastModified || null), channelArn || null]
  );
  const conversion = await startConversion(client, env, eventId);
  return { bucket, prefix, manifestKey: found.key, lastModified: found.lastModified, candidates: manifests.length, conversion };
}

async function sendRecordingReady(client: Client, env: Env, eventId: string) {
  const { rows } = await client.query(`update public.events set recording_email_error=null where id=$1 and recording_email_sent_at is null and recording_status='ready' and recording_mp4_s3_key is not null returning *`, [eventId]);
  const ev = rows[0];
  if (!ev) return { skipped: true };
  const { rows: duplicateRows } = await client.query(`select id from public.events where id <> $1 and recording_hls_manifest_key=$2 limit 1`, [eventId, ev.recording_hls_manifest_key]);
  if (duplicateRows[0]) {
    await client.query(`update public.events set recording_email_error=$2 where id=$1`, [eventId, `duplicate_recording_manifest_claimed_by_${duplicateRows[0].id}`]);
    return { skipped: true, reason: "duplicate_recording_manifest", duplicate_event_id: duplicateRows[0].id };
  }
  const b = brand(env);
  const url = recordingLink(env, ev);
  const subject = `${b}: recording ready - ${ev.title || "live stream"}`;
  const text = [`${b} recording ready`, "", `Event: ${ev.title || "Untitled event"}`, "", `Download recording: ${url}`].join("\n");
  const html = emailShell(b, "Your recording is ready", `<p style="margin:0 0 12px"><b>Event:</b> ${htmlEscape(ev.title || "Untitled event")}</p><p style="margin:18px 0 8px">${emailButton("Download recording", url)}</p>`);
  const result = await sendEmail(env, { to: ev.email, subject, text, html, tag: "recording_ready" });
  await client.query(`update public.events set recording_email_sent_at=now(), recording_email_error=null where id=$1`, [eventId]);
  return { sent: true, result };
}

async function handleRecordingEvent(request: Request, env: Env) {
  const got = request.headers.get("x-relay-recording-secret") || "";
  if (!env.RECORDING_WEBHOOK_SECRET || got !== env.RECORDING_WEBHOOK_SECRET) return json(env, { error: "unauthorized" }, 401);
  const payload = await request.json().catch(() => ({}));
  const rec = extractRecordingEvent(payload, env);
  const client = await getClient(env);
  try {
    await ensureColumns(client);
    const { rows } = await client.query(`
      update public.events
      set recording_enabled=true,
          recording_status=case when $8::boolean then 'processing' else 'recording' end,
          recording_s3_bucket=$2,
          recording_s3_prefix=coalesce(nullif($3,''), recording_s3_prefix),
          recording_hls_manifest_key=coalesce(nullif($4,''), recording_hls_manifest_key),
          recording_ivs_channel_arn=coalesce(recording_ivs_channel_arn, nullif($1,'')),
          recording_started_at=coalesce(recording_started_at, $5::timestamptz),
          recording_ended_at=case when $8::boolean then coalesce(recording_ended_at, $6::timestamptz, now()) else recording_ended_at end,
          recording_expires_at=case when $8::boolean then coalesce(recording_expires_at, $7::timestamptz) else recording_expires_at end,
          recording_webhook_received_at=now(),
          recording_webhook_payload=$9::jsonb,
          recording_error=null
      where ivs_channel_arn=$1 and hls_enabled=true
      returning id, status
    `, [rec.channelArn, rec.bucket, rec.prefix || null, rec.manifestKey || null, rec.startedAt || null, rec.endedAt || null, expiryIso(env, rec.endedAt || null), rec.ended, JSON.stringify(payload)]);
    const eventId = rows[0]?.id;
    const eventStatus = String(rows[0]?.status || "");
    if (eventId && rec.manifestKey) {
      await upsertRecordingSegment(client, {
        eventId,
        channelArn: rec.channelArn,
        bucket: rec.bucket,
        prefix: rec.prefix || null,
        manifestKey: rec.manifestKey,
        startedAt: rec.startedAt || null,
        endedAt: rec.endedAt || null,
        payload,
      }).catch((e) => client.query(`update public.events set recording_error=$2 where id=$1`, [eventId, String(e?.message || e).slice(0, 2000)]));
    }
    return json(env, { ok: !!eventId, event_id: eventId || null, extracted: rec });
  } finally {
    await client.end();
  }
}

async function poll(client: Client, env: Env) {
  await ensureColumns(client);
  const { rows: prepareRows } = await client.query(
    `
    select id
    from public.events
    where hls_enabled=true
      and ivs_channel_arn is not null
      and status in ('paid','live')
      and coalesce(recording_enabled,false)=false
    order by created_at desc
    limit 10
  `
  );
  for (const row of prepareRows) {
    await prepareRecordingChannel(client, env, row.id).catch((e) =>
      client.query(`update public.events set recording_error=$2 where id=$1`, [row.id, String(e?.message || e).slice(0, 2000)])
    );
  }

  const { rows: storedWebhookRows } = await client.query(
    `
    select id
    from public.events
    where hls_enabled=true
      and recording_webhook_payload is not null
      and recording_mp4_job_id is null
      and not exists (select 1 from public.recording_segments rs where rs.event_id=events.id)
    order by recording_webhook_received_at desc nulls last, created_at desc
    limit 5
  `
  );
  for (const row of storedWebhookRows) {
    await ingestStoredWebhookPayload(client, env, row.id).catch((e) =>
      client.query(`update public.events set recording_error=$2 where id=$1`, [row.id, String(e?.message || e).slice(0, 2000)])
    );
  }

  const { rows: readyForConversion } = await client.query(
    `
    select e.id
    from public.events e
    where e.status='expired'
      and e.hls_enabled=true
      and e.recording_mp4_job_id is null
      and exists (select 1 from public.recording_segments rs where rs.event_id=e.id)
      and coalesce(e.expires_at, e.cleanup_completed_at, now()) <= now() - ($1::int * interval '1 minute')
      and not exists (
        select 1
        from public.recording_segments rs
        where rs.event_id=e.id
          and rs.updated_at > now() - ($1::int * interval '1 minute')
      )
    order by coalesce(e.expires_at, e.created_at) desc
    limit 5
  `,
    [conversionGraceMinutes(env)]
  );
  for (const row of readyForConversion) {
    await startConversion(client, env, row.id).catch((e) =>
      client.query(`update public.events set recording_error=$2 where id=$1`, [row.id, String(e?.message || e).slice(0, 2000)])
    );
  }

  const { rows } = await client.query(`select id, recording_mp4_job_id from public.events where recording_status='processing' and recording_mp4_job_id is not null and coalesce(recording_mp4_job_status,'') not in ('COMPLETE','ERROR','CANCELED') limit 5`);
  for (const row of rows) {
    const job = mediaConvertJob(await mediaConvert(env, "GET", `/2017-08-29/jobs/${encodeURIComponent(row.recording_mp4_job_id)}`));
    const status = String(job?.Status || job?.status || "").toUpperCase();
    if (status === "COMPLETE") {
      await client.query(`update public.events set recording_status='ready', recording_mp4_job_status=$2, recording_ended_at=coalesce(recording_ended_at, now()), recording_expires_at=coalesce(recording_expires_at, $3::timestamptz), recording_error=null where id=$1`, [row.id, status, expiryIso(env)]);
      await sendRecordingReady(client, env, row.id).catch((e) => client.query(`update public.events set recording_email_error=$2 where id=$1`, [row.id, String(e?.message || e).slice(0, 2000)]));
    } else if (status) {
      await client.query(`update public.events set recording_mp4_job_status=$2 where id=$1`, [row.id, status]);
    }
  }
}

async function cleanupExpiredRecordings(client: Client, env: Env) {
  await ensureColumns(client);
  const { rows } = await client.query(`
    select id, recording_s3_bucket, recording_s3_prefix, recording_hls_manifest_key, recording_mp4_s3_key
    from public.events
    where recording_enabled = true
      and recording_expires_at is not null
      and recording_expires_at <= now()
      and recording_cleanup_completed_at is null
    order by recording_expires_at asc
    limit 5
  `);

  for (const ev of rows) {
    const bucket = ev.recording_s3_bucket || String(env.RECORDINGS_S3_BUCKET || "").trim();
    if (!bucket) continue;

    try {
      await client.query(
        `update public.events
         set recording_cleanup_started_at=coalesce(recording_cleanup_started_at, now()),
             recording_cleanup_error=null
         where id=$1`,
        [ev.id]
      );
      const result = await deleteS3RecordingObjects(
        env,
        bucket,
        ev.recording_s3_prefix || null,
        [ev.recording_hls_manifest_key, ev.recording_mp4_s3_key].filter(Boolean)
      );
      await client.query(
        `update public.events
         set recording_status='expired',
             recording_cleanup_completed_at=now(),
             recording_cleanup_error=null
         where id=$1`,
        [ev.id]
      );
      console.log("recording cleanup", JSON.stringify({ eventId: ev.id, bucket, ...result }));
    } catch (e: any) {
      await client.query(`update public.events set recording_cleanup_error=$2 where id=$1`, [ev.id, (e?.message || String(e)).slice(0, 2000)]);
      console.error("recording cleanup failed", ev.id, e);
    }
  }
}

export default {
  async fetch(request: Request, env: Env) {
    const url = new URL(request.url);
    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: {
          "access-control-allow-origin": env.APP_ORIGIN || "https://castlink.stream",
          "access-control-allow-methods": "GET,POST,OPTIONS",
          "access-control-allow-headers": "content-type,x-relay-recording-secret",
          "access-control-allow-credentials": "true",
          vary: "Origin",
        },
      });
    }
    if (request.method === "GET" && url.pathname === "/healthz") return new Response("ok");
    if (request.method === "POST" && url.pathname === "/ivs/recording-event") return handleRecordingEvent(request, env);

    if (url.pathname === "/admin/stale-events") {
      if (!adminAuthorized(request, env)) return json(env, { error: "unauthorized" }, 401);
      const client = await getClient(env);
      try {
        const limit = Number(url.searchParams.get("limit") || 50);
        if (request.method === "GET") {
          const events = await listStaleEvents(client, env, limit);
          return json(env, { ok: true, count: events.length, events });
        }
        if (request.method === "POST") {
          const cleanedOpen = await expireCleanedOpenEvents(client, limit);
          const expired = await expireStaleEvents(client, env, limit);
          return json(env, { ok: true, count: cleanedOpen.length + expired.length, cleaned_open: cleanedOpen, expired });
        }
      } finally {
        await client.end();
      }
    }

    if (url.pathname === "/admin/open-events" || url.pathname === "/admin/assigned-inventory") {
      if (!adminAuthorized(request, env)) return json(env, { error: "unauthorized" }, 401);
      const client = await getClient(env);
      try {
        const limit = Number(url.searchParams.get("limit") || 50);
        if (request.method === "GET" && url.pathname === "/admin/open-events") {
          const events = await listOpenEvents(client, limit);
          return json(env, { ok: true, count: events.length, events });
        }
        if (request.method === "GET" && url.pathname === "/admin/assigned-inventory") {
          const slots = await listAssignedInventory(client, limit);
          return json(env, { ok: true, count: slots.length, slots });
        }
      } finally {
        await client.end();
      }
    }

    if (url.pathname === "/admin/recent-recordings") {
      if (!adminAuthorized(request, env)) return json(env, { error: "unauthorized" }, 401);
      const client = await getClient(env);
      try {
        if (request.method === "GET") {
          const limit = Number(url.searchParams.get("limit") || 25);
          const recordings = await listRecentRecordings(client, limit);
          return json(env, { ok: true, count: recordings.length, recordings });
        }
      } finally {
        await client.end();
      }
    }

    const recordingDebugMatch = url.pathname.match(/^\/admin\/events\/([^/]+)\/recording-debug$/);
    if (request.method === "GET" && recordingDebugMatch) {
      if (!adminAuthorized(request, env)) return json(env, { error: "unauthorized" }, 401);
      const client = await getClient(env);
      try {
        const debug = await recordingDebugForEvent(client, recordingDebugMatch[1]);
        if (!debug) return json(env, { error: "not_found" }, 404);
        return json(env, { ok: true, ...debug });
      } finally {
        await client.end();
      }
    }

    const ingestStoredMatch = url.pathname.match(/^\/admin\/events\/([^/]+)\/recording\/ingest-stored-webhook$/);
    if (request.method === "POST" && ingestStoredMatch) {
      if (!adminAuthorized(request, env)) return json(env, { error: "unauthorized" }, 401);
      const client = await getClient(env);
      try {
        const ingested = await ingestStoredWebhookPayload(client, env, ingestStoredMatch[1]);
        const conversion = await startConversion(client, env, ingestStoredMatch[1]);
        return json(env, { ok: true, ingested, conversion });
      } catch (e: any) {
        return json(env, { ok: false, error: String(e?.message || e) }, 500);
      } finally {
        await client.end();
      }
    }

    const retryConversionMatch = url.pathname.match(/^\/admin\/events\/([^/]+)\/recording\/retry-conversion$/);
    if (request.method === "POST" && retryConversionMatch) {
      if (!adminAuthorized(request, env)) return json(env, { error: "unauthorized" }, 401);
      const client = await getClient(env);
      try {
        await ensureColumns(client);
        const { rows: resetRows } = await client.query(
          `
          update public.events
          set recording_status='processing',
              recording_mp4_job_id=null,
              recording_mp4_job_status=null,
              recording_mp4_s3_key=null,
              recording_email_sent_at=null,
              recording_email_error=null,
              recording_download_claimed_at=null,
              recording_download_claimed_ip=null,
              recording_error=null
          where id=$1
          returning id, recording_status, recording_mp4_job_id, recording_mp4_job_status
        `,
          [retryConversionMatch[1]]
        );
        if (!resetRows[0]) return json(env, { ok: false, error: "event_not_found" }, 404);
        const conversion = await startConversion(client, env, retryConversionMatch[1]);
        const debug = await recordingDebugForEvent(client, retryConversionMatch[1]);
        return json(env, { ok: true, reset: resetRows[0], conversion, debug });
      } catch (e: any) {
        const message = String(e?.message || e).slice(0, 2000);
        await client.query(`update public.events set recording_status='failed', recording_error=$2 where id=$1`, [retryConversionMatch[1], message]).catch(() => null);
        return json(env, { ok: false, error: message }, 500);
      } finally {
        await client.end();
      }
    }

    const resetRecordingMatch = url.pathname.match(/^\/admin\/events\/([^/]+)\/recording\/reset-premature$/);
    if (request.method === "POST" && resetRecordingMatch) {
      if (!adminAuthorized(request, env)) return json(env, { error: "unauthorized" }, 401);
      const client = await getClient(env);
      try {
        await ensureColumns(client);
        const { rows } = await client.query(
          `
          update public.events
          set recording_status='processing',
              recording_mp4_job_id=null,
              recording_mp4_job_status=null,
              recording_mp4_s3_key=null,
              recording_email_sent_at=null,
              recording_email_error=null,
              recording_download_claimed_at=null,
              recording_download_claimed_ip=null,
              recording_error=null
          where id=$1
          returning id, status, recording_status, recording_mp4_job_id, recording_mp4_job_status, recording_mp4_s3_key, recording_email_sent_at
        `,
          [resetRecordingMatch[1]]
        );
        const event = rows[0] || null;
        if (!event) return json(env, { error: "event_not_found" }, 404);
        return json(env, { ok: true, event });
      } finally {
        await client.end();
      }
    }

    const completeConversionMatch = url.pathname.match(/^\/admin\/events\/([^/]+)\/recording\/complete-conversion$/);
    if (request.method === "POST" && completeConversionMatch) {
      if (!adminAuthorized(request, env)) return json(env, { error: "unauthorized" }, 401);
      const body: any = await request.json().catch(() => ({}));
      const jobId = String(body.jobId || body.job_id || "").trim();
      const outputKey = String(body.outputKey || body.output_key || "").trim();
      if (!jobId) return json(env, { error: "job_id_required" }, 400);
      if (!outputKey) return json(env, { error: "output_key_required" }, 400);
      const client = await getClient(env);
      try {
        const job = mediaConvertJob(await mediaConvert(env, "GET", `/2017-08-29/jobs/${encodeURIComponent(jobId)}`));
        const status = String(job?.Status || job?.status || "").toUpperCase();
        if (status !== "COMPLETE") return json(env, {
          ok: false,
          status,
          error: job?.ErrorMessage || job?.errorMessage || null,
          job_keys: job && typeof job === "object" ? Object.keys(job).slice(0, 30) : [],
          job_status: job?.Status ?? null,
          job_status_lower: job?.status ?? null,
        }, 409);
        const { rows } = await client.query(
          `
          update public.events
          set recording_status='ready',
              recording_mp4_job_id=$2,
              recording_mp4_job_status=$3,
              recording_mp4_s3_key=$4,
              recording_ended_at=coalesce(recording_ended_at, now()),
              recording_expires_at=coalesce(recording_expires_at, $5::timestamptz),
              recording_error=null,
              recording_email_sent_at=null,
              recording_email_error=null
          where id=$1
          returning id, recording_status, recording_mp4_job_id, recording_mp4_job_status, recording_mp4_s3_key
        `,
          [completeConversionMatch[1], jobId, status, outputKey, expiryIso(env)]
        );
        const persisted = rows[0] || null;
        if (!persisted) return json(env, { error: "event_not_found" }, 404);
        const email = await sendRecordingReady(client, env, completeConversionMatch[1]);
        return json(env, { ok: true, status, persisted, email });
      } catch (e: any) {
        return json(env, { ok: false, error: String(e?.message || e).slice(0, 2000) }, 500);
      } finally {
        await client.end();
      }
    }

    if (url.pathname === "/admin/expired-ivs-streams/stop") {
      if (!adminAuthorized(request, env)) return json(env, { error: "unauthorized" }, 401);
      const client = await getClient(env);
      try {
        if (request.method === "POST") {
          const limit = Number(url.searchParams.get("limit") || 10);
          const stopped = await stopExpiredIvsStreams(client, env, limit);
          return json(env, { ok: true, count: stopped.length, stopped });
        }
      } finally {
        await client.end();
      }
    }

    if (url.pathname === "/admin/recording-duplicates") {
      if (!adminAuthorized(request, env)) return json(env, { error: "unauthorized" }, 401);
      const client = await getClient(env);
      try {
        if (request.method === "GET") {
          const duplicates = await listDuplicateRecordings(client);
          return json(env, { ok: true, count: duplicates.length, duplicates });
        }
        if (request.method === "POST") {
          const mismatched = await repairMismatchedRecordingClaims(client);
          const duplicatesRepaired = await repairDuplicateRecordingClaims(client);
          const repaired = [...mismatched, ...duplicatesRepaired];
          const duplicates = await listDuplicateRecordings(client);
          return json(env, { ok: true, repaired_count: repaired.length, repaired, remaining_duplicate_count: duplicates.length, remaining_duplicates: duplicates });
        }
      } finally {
        await client.end();
      }
    }

    const prepareMatch = url.pathname.match(/^\/events\/([^/]+)\/recording\/prepare$/);
    if (request.method === "POST" && prepareMatch) {
      const client = await getClient(env);
      try {
        const ev = await getEvent(client, prepareMatch[1]);
        if (!ev) return json(env, { error: "not_found" }, 404);
        if (url.searchParams.get("key") !== ev.broadcast_key) return json(env, { error: "unauthorized" }, 401);
        const result = await prepareRecordingChannel(client, env, ev.id);
        return json(env, { ok: true, result });
      } catch (e: any) {
        return json(env, { ok: false, error: String(e?.message || e) }, 500);
      } finally {
        await client.end();
      }
    }

    const discoverMatch = url.pathname.match(/^\/admin\/events\/([^/]+)\/recording\/discover$/);
    if (request.method === "POST" && discoverMatch) {
      const got = request.headers.get("x-relay-recording-secret") || "";
      if (!env.RECORDING_WEBHOOK_SECRET || got !== env.RECORDING_WEBHOOK_SECRET) return json(env, { error: "unauthorized" }, 401);
      const client = await getClient(env);
      try {
        const discovered = await discoverManifestForEvent(client, env, discoverMatch[1]);
        return json(env, { ok: true, discovered });
      } catch (e: any) {
        return json(env, { ok: false, error: String(e?.message || e) }, 500);
      } finally {
        await client.end();
      }
    }

    const statusMatch = url.pathname.match(/^\/events\/([^/]+)\/recording$/);
    if (request.method === "GET" && statusMatch) {
      const client = await getClient(env);
      try {
        const ev = await getEvent(client, statusMatch[1]);
        if (!ev) return json(env, { error: "not_found" }, 404);
        if (url.searchParams.get("key") !== ev.broadcast_key) return json(env, { error: "unauthorized" }, 401);
        return json(env, { ok: true, recording: state(ev) });
      } finally { await client.end(); }
    }

    const downloadMatch = url.pathname.match(/^\/events\/([^/]+)\/recording\/download$/);
    if (request.method === "POST" && downloadMatch) {
      const client = await getClient(env);
      try {
        const ev = await getEvent(client, downloadMatch[1]);
        if (!ev) return json(env, { error: "not_found" }, 404);
        if (url.searchParams.get("key") !== ev.broadcast_key) return json(env, { error: "unauthorized" }, 401);
        const s = state(ev);
        if (!s.download_available) return json(env, { error: s.download_claimed ? "recording_download_already_claimed" : "recording_not_ready", recording: s }, s.download_claimed ? 410 : 409);
        const { rows } = await client.query(`update public.events set recording_download_claimed_at=now(), recording_download_claimed_ip=$2 where id=$1 and recording_download_claimed_at is null returning *`, [ev.id, request.headers.get("cf-connecting-ip") || ""]);
        const claimed = rows[0];
        if (!claimed) return json(env, { error: "recording_download_already_claimed" }, 410);
        const filename = `${slug(claimed.title) || String(claimed.id).slice(0, 8)}-castlink-recording.mp4`;
        const signed = await presignS3GetUrl(env, claimed.recording_s3_bucket, claimed.recording_mp4_s3_key, 900, filename);
        return json(env, { ok: true, url: signed, expires_in_seconds: 900, recording: state(claimed) });
      } finally { await client.end(); }
    }

    return json(env, { error: "not_found" }, 404);
  },
  async scheduled(_event: ScheduledEvent, env: Env) {
    const client = await getClient(env);
    try {
      await expireCleanedOpenEvents(client, 50);
      await expireStaleEvents(client, env, 50);
      await stopExpiredIvsStreams(client, env, 10);
      await poll(client, env);
      await cleanupExpiredRecordings(client, env);
    } finally { await client.end(); }
  },
};
