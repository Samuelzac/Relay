-- 014_ops_automation.sql
-- Operational self-healing metadata, alert de-duping, and customer link recovery rate limits.

alter table public.events
  add column if not exists ready_email_attempts int not null default 0,
  add column if not exists ready_email_last_attempt_at timestamptz,
  add column if not exists viewer_invites_attempts int not null default 0,
  add column if not exists viewer_invites_last_attempt_at timestamptz,
  add column if not exists warning_email_attempts int not null default 0,
  add column if not exists warning_email_last_attempt_at timestamptz,
  add column if not exists recording_email_attempts int not null default 0,
  add column if not exists recording_email_last_attempt_at timestamptz;

create index if not exists idx_events_ready_email_retry
  on public.events(status, ready_email_last_attempt_at)
  where ready_email_sent_at is null and ready_email_error is not null;

create index if not exists idx_events_warning_email_retry
  on public.events(status, warning_email_last_attempt_at)
  where warning_email_sent_at is null and warning_email_error is not null;

create index if not exists idx_events_recording_email_retry
  on public.events(recording_status, recording_email_last_attempt_at)
  where recording_email_sent_at is null and recording_email_error is not null;

create table if not exists public.ops_alerts (
  alert_key text primary key,
  severity text not null check (severity in ('info','warn','critical')),
  subject text not null,
  detail text not null,
  data jsonb,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  last_sent_at timestamptz,
  send_count int not null default 0,
  resolved_at timestamptz
);

create index if not exists idx_ops_alerts_last_seen
  on public.ops_alerts(last_seen_at desc);

create table if not exists public.link_recovery_requests (
  id uuid primary key default gen_random_uuid(),
  email text not null,
  ip_address text,
  event_id uuid,
  matched_count int not null default 0,
  created_at timestamptz not null default now()
);

create index if not exists idx_link_recovery_email_created
  on public.link_recovery_requests(lower(email), created_at desc);

create index if not exists idx_link_recovery_ip_created
  on public.link_recovery_requests(ip_address, created_at desc);
