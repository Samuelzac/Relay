-- 009_report_moderation.sql
-- Adds moderation details for viewer reports.

alter table public.reports
  add column if not exists reporter_email text,
  add column if not exists urgent boolean not null default false,
  add column if not exists status text not null default 'open',
  add column if not exists event_snapshot jsonb,
  add column if not exists viewer_session_id text;

create index if not exists idx_reports_status_created_at
  on public.reports(status, created_at desc);
