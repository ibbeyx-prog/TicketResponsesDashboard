-- Additional assignment task categories (assign dropdown + bot regex).

insert into public.dashboard_task_categories (name, sort_order) values
  ('Voice / Data issue', 70),
  ('Femto Swap', 80),
  ('Pico Installation', 90),
  ('Pole / EM installation', 100),
  ('IBS', 110),
  ('Sector / Nodeb Installation', 120),
  ('Lamp Site / Fault', 130),
  ('Mobile Fault / Swap', 140),
  ('Optimization / Logfile', 150),
  ('Customer moved out', 160),
  ('MEGA Survey', 170),
  ('OFF Male', 180),
  ('Continue Installation', 190),
  ('Follow-Up Installation', 200)
on conflict (name) do nothing;

notify pgrst, 'reload schema';
