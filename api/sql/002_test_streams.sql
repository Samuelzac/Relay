create table if not exists test_streams (
  id uuid primary key default gen_random_uuid(),
  event_id uuid not null references events(id) on delete cascade,

  status text not null default 'active' check (status in ('active','expired')),
  created_at timestamptz not null default now(),
  expires_at timestamptz not null,

  ivs_channel_arn text,
  ivs_ingest_endpoint text,
  ivs_stream_key_encrypted text,
  ivs_playback_url text,

  secret_key text not null
);

create index if not exists idx_test_streams_event on test_streams(event_id);
create index if not exists idx_test_streams_expires on test_streams(expires_at);
