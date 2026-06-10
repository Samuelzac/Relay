-- 007_event_cleanup_tracking.sql
-- Tracks best-effort cleanup of expired IVS resources.

alter table public.events
  add column if not exists cleanup_started_at timestamptz,
  add column if not exists cleanup_completed_at timestamptz,
  add column if not exists cleanup_attempts int not null default 0,
  add column if not exists cleanup_error text;

create index if not exists idx_events_cleanup_due
  on public.events(status, cleanup_completed_at, expires_at)
  where status = 'expired' and cleanup_completed_at is null;
