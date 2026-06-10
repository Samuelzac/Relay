-- 006_launch_pricing_tiers.sql
-- Allows launch pricing durations beyond the original 3h/8h packages.
-- Safe to run multiple times.

do $$
begin
  if exists (
    select 1
    from information_schema.constraint_column_usage
    where table_schema = 'public'
      and table_name = 'events'
      and column_name = 'tier'
      and constraint_name = 'events_tier_check'
  ) then
    alter table public.events drop constraint events_tier_check;
  end if;
end $$;

alter table public.events
  add constraint events_tier_check
  check (tier in (1,2,3,8));
