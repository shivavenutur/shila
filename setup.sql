-- Run once in Supabase: SQL Editor -> New query -> paste -> Run
create table if not exists messages (
  id uuid primary key default gen_random_uuid(),
  sender text not null,
  text text not null default '',
  media_path text,
  media_name text,
  media_type text,
  created_at timestamptz not null default now()
);
create index if not exists messages_created_idx on messages (created_at);

-- Lock the table: only the server's secret key can read or write it
alter table messages enable row level security;

-- Private bucket for photos, videos and files
insert into storage.buckets (id, name, public)
values ('chat-media', 'chat-media', false)
on conflict (id) do nothing;
