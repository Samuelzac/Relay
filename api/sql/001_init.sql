create extension if not exists pgcrypto;

create table if not exists events (
  id uuid primary key default gen_random_uuid(),
  email text not null,
  title text not null,
  tier int not null check (tier in (3,8)),
  viewer_limit int not null default 150,
  white_label boolean not null default false,
  status text not null default 'pending' check (status in ('pending','paid','live','expired')),
  starts_at timestamptz,
  expires_at timestamptz,
  created_at timestamptz not null default now(),

  ivs_channel_arn text,
  ivs_ingest_endpoint text,
  ivs_stream_key_encrypted text,
  ivs_playback_url text,

  secret_key text not null,
  success_token_hash text,
  stripe_session_id text unique,

  stripe_upgrade_session_id text,
  pending_upgrade_to int
);

create index if not exists idx_events_status on events(status);
create index if not exists idx_events_expires on events(expires_at);

create table if not exists view_sessions (
  id uuid primary key default gen_random_uuid(),
  event_id uuid not null references events(id) on delete cascade,
  last_seen_at timestamptz not null default now(),
  created_at timestamptz not null default now()
);

create index if not exists idx_view_sessions_event on view_sessions(event_id);
