-- 008_stream_warning_emails.sql
-- Tracks one-time warning emails before an active stream expires.

alter table public.events
  add column if not exists warning_email_sent_at timestamptz,
  add column if not exists warning_email_error text;

create index if not exists idx_events_warning_email_due
  on public.events(status, expires_at, warning_email_sent_at)
  where status = 'paid' and starts_at is not null and warning_email_sent_at is null;
