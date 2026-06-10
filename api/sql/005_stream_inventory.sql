-- 005_stream_inventory.sql
-- Pre-provisioned IVS resource pool for fast post-checkout assignment.
-- Safe to run multiple times.

create extension if not exists pgcrypto;

do $$
begin
  if exists (
    select 1
    from information_schema.constraint_column_usage
    where table_schema = 'public'
      and table_name = 'events'
      and column_name = 'status'
      and constraint_name = 'events_status_check'
  ) then
    alter table public.events drop constraint events_status_check;
  end if;
end $$;

update public.events
set status = 'pending'
where status = 'created';

alter table public.events
  add constraint events_status_check
  check (status in ('pending','paid','live','expired'));

create table if not exists public.stream_inventory (
  id uuid primary key default gen_random_uuid(),
  mode text not null check (mode in ('rtc','hls','both')),
  status text not null default 'available' check (status in ('available','reserved','assigned','retired','failed')),
  assigned_event_id uuid references public.events(id) on delete set null,

  ivs_channel_arn text,
  ivs_ingest_endpoint text,
  ivs_playback_url text,
  ivs_stream_key_encrypted text,

  rtc_stage_arn text,
  rtc_stage_endpoints jsonb,

  error text,
  created_at timestamptz not null default now(),
  reserved_at timestamptz,
  assigned_at timestamptz,
  retired_at timestamptz
);

create index if not exists idx_stream_inventory_available
  on public.stream_inventory(mode, created_at)
  where status = 'available';

create index if not exists idx_stream_inventory_status
  on public.stream_inventory(status, created_at);

create unique index if not exists idx_stream_inventory_assigned_event
  on public.stream_inventory(assigned_event_id)
  where assigned_event_id is not null;
