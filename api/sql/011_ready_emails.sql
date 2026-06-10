-- 011_ready_emails.sql
-- Tracks one-time event-ready/link emails.

alter table public.events
  add column if not exists ready_email_sent_at timestamptz,
  add column if not exists ready_email_error text;

create index if not exists idx_events_ready_email_errors
  on public.events(ready_email_error)
  where ready_email_error is not null;
