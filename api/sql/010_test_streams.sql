-- 010_test_streams.sql
-- Adds checkoutless short test streams that use the normal Castlink event path.
-- Safe to run multiple times.

alter table public.events
  add column if not exists is_test boolean not null default false,
  add column if not exists test_created_ip text,
  add column if not exists test_expires_unused_at timestamptz;

create table if not exists public.test_stream_requests (
  id uuid primary key default gen_random_uuid(),
  email text not null,
  ip_address text,
  event_id uuid references public.events(id) on delete set null,
  created_at timestamptz not null default now()
);

create index if not exists idx_test_stream_requests_email_created
  on public.test_stream_requests(lower(email), created_at desc);

create index if not exists idx_test_stream_requests_ip_created
  on public.test_stream_requests(ip_address, created_at desc);

create index if not exists idx_events_is_test
  on public.events(is_test);
