-- Storage policies for the `ticket-photos` bucket.
--
-- Symptom this fixes: field-response replies with a photo were failing
-- with
--
--   StorageApiError: {'statusCode': 403, 'message': 'new row violates
--                     row-level security policy'}
--
-- when the Telegram bot tried to upload the image to Supabase Storage.
-- `storage.objects` has RLS enabled by default and no policies existed
-- for the anon role, so every upload was denied.
--
-- We also flip the bucket to public so the `photo_url` we save on each
-- ticket row (built via `get_public_url`) actually resolves in browsers
-- without needing signed URLs. The bucket only ever holds field photos,
-- which the ops team already views in the dashboard -- treating them as
-- public is acceptable.
--
-- Idempotent: re-running drops + recreates the policies.

-- --------------------------------------------------------------------
-- 1) Ensure the bucket exists and is public
-- --------------------------------------------------------------------
insert into storage.buckets (id, name, public)
  values ('ticket-photos', 'ticket-photos', true)
  on conflict (id) do update set public = excluded.public;

-- --------------------------------------------------------------------
-- 2) anon policies on storage.objects scoped to this bucket only
-- --------------------------------------------------------------------
drop policy if exists "ticket_photos_anon_insert" on storage.objects;
drop policy if exists "ticket_photos_anon_select" on storage.objects;
drop policy if exists "ticket_photos_anon_update" on storage.objects;

create policy "ticket_photos_anon_insert"
  on storage.objects
  for insert
  to anon
  with check (bucket_id = 'ticket-photos');

create policy "ticket_photos_anon_select"
  on storage.objects
  for select
  to anon
  using (bucket_id = 'ticket-photos');

-- UPDATE is required because the bot uploads with `upsert: true` so the
-- same object_path can be re-uploaded if a user retries quickly.
create policy "ticket_photos_anon_update"
  on storage.objects
  for update
  to anon
  using (bucket_id = 'ticket-photos')
  with check (bucket_id = 'ticket-photos');
-- DELETE is intentionally NOT granted -- photos are kept for audit.
