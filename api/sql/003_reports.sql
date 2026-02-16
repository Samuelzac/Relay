-- Reports + manual disable flag

alter table if exists events
  add column if not exists disabled boolean not null default false;

create table if not exists reports (
  id uuid primary key default gen_random_uuid(),
  event_id uuid references events(id) on delete set null,
  page text not null default 'watch',
  reason text not null,
  description text,
  ip_address text,
  user_agent text,
  created_at timestamptz not null default now()
);

create index if not exists idx_reports_created_at on reports(created_at desc);
create index if not exists idx_reports_event on reports(event_id);
