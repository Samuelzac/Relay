-- 004_realtime.sql
-- Adds fields needed for WebRTC (IVS Real-Time Stages) + per-event broadcast secret.
-- Safe to run multiple times.

create extension if not exists pgcrypto;

alter table public.events
  add column if not exists broadcast_key text,
  add column if not exists rtc_stage_arn text,
  add column if not exists rtc_stage_endpoints jsonb,
  add column if not exists rtc_enabled boolean not null default true,
  add column if not exists hls_enabled boolean not null default true;

update public.events
set broadcast_key = coalesce(broadcast_key, encode(gen_random_bytes(24), 'base64'))
where broadcast_key is null;

create index if not exists idx_events_rtc_stage_arn on public.events(rtc_stage_arn);
