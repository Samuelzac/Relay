alter table public.events
  add column if not exists viewer_recipient_emails text[] not null default '{}',
  add column if not exists viewer_invites_sent_at timestamptz,
  add column if not exists viewer_invites_error text;
