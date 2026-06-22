-- 013_recordings.sql
-- Host recording metadata and short-lived MP4 download tracking.

alter table public.events
  add column if not exists recording_enabled boolean not null default false,
  add column if not exists recording_status text not null default 'not_started',
  add column if not exists recording_s3_bucket text,
  add column if not exists recording_s3_prefix text,
  add column if not exists recording_hls_manifest_key text,
  add column if not exists recording_mp4_s3_key text,
  add column if not exists recording_mp4_job_id text,
  add column if not exists recording_mp4_job_arn text,
  add column if not exists recording_mp4_job_status text,
  add column if not exists recording_mp4_job_submitted_at timestamptz,
  add column if not exists recording_mp4_job_completed_at timestamptz,
  add column if not exists recording_mp4_job_error text,
  add column if not exists recording_started_at timestamptz,
  add column if not exists recording_ended_at timestamptz,
  add column if not exists recording_expires_at timestamptz,
  add column if not exists recording_error text,
  add column if not exists recording_email_sent_at timestamptz,
  add column if not exists recording_email_error text,
  add column if not exists recording_download_claimed_at timestamptz,
  add column if not exists recording_download_claimed_ip text,
  add column if not exists recording_cleanup_started_at timestamptz,
  add column if not exists recording_cleanup_completed_at timestamptz,
  add column if not exists recording_cleanup_error text;

create index if not exists idx_events_recording_status
  on public.events(recording_status, recording_expires_at);
