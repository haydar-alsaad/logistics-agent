-- =============================================================================
-- Barq Express — Supabase migration (data layer)            SPEC.md §1, §2, §4
-- =============================================================================
--
-- RUN ORDER (fresh self-owned Supabase project, or the local harness):
--   1. (local harness only) harness/supabase_stubs.sql — stubs auth/storage/
--      realtime/roles. On hosted Supabase these already exist; skip.
--   2. THIS FILE — paste into the Supabase SQL editor and run once (as postgres).
--      Re-running is safe: tables use IF NOT EXISTS, functions CREATE OR
--      REPLACE, policies/triggers are dropped and recreated, the realtime
--      publication and storage bucket are guarded.
--      NOTE: re-running does NOT alter columns of tables that already exist.
--      For a column change on a live project, write an ALTER, or drop the
--      schema objects and re-run (then re-seed).
--   3. python scripts/seed_baseline.py — loads data/*.json into the SHARED
--      tables and the *_backup tables (service role key, PostgREST upsert).
--   4. python scripts/upload_assets.py — uploads assets/demo-assets/** to the
--      `documents` bucket under demo-assets/.
--   5. Create users (Supabase Auth → Add user, with user metadata
--      {"whatsapp_number":"+9665…","full_name":"…"}). The AFTER INSERT trigger
--      on auth.users registers demo_users and clones the baseline for them.
--      Users created BEFORE step 3 get an empty clone — fix with
--      `python scripts/seed_baseline.py --reclone-owner <uuid>` or
--      `select public.clone_baseline_for_user('<uuid>');`.
--
-- Tenancy model (§1.3):
--   shared tables    : districts, centres, pickup_points, stores, drivers,
--                      business_rules, delivery_windows, demo_meta, demo_assets
--   per-tenant tables: customers, shipments, shipment_events,
--                      authorized_receivers, cases, returns, payment_requests,
--                      tax_invoices, store_alerts, agent_actions
--                      (owner_id uuid, composite PK (owner_id, business_id))
--   baseline         : <per-tenant>_backup (no owner_id), except agent_actions
--   clone            : clone_baseline_for_user(uuid) — generic date shift §1.4
--   reset            : reset_demo_data() — caller's rows only (auth.uid())
-- =============================================================================

-- ---------------------------------------------------------------- extensions
create schema if not exists extensions;
create extension if not exists pgcrypto with schema extensions;

set search_path = public, extensions;

-- =============================================================================
-- SHARED REFERENCE TABLES (§2.2) — no owner_id, read-only for authenticated
-- =============================================================================

create table if not exists public.centres (
  centre_id        text primary key check (centre_id ~ '^CTR-[0-9]{3}$'),
  type             text not null check (type in ('Sorting Centre','Branch')),
  name_en          text not null,
  name_ar          text not null,
  city_en          text,
  city_ar          text,
  address_en       text,
  address_ar       text,
  lat              numeric(9,6),
  lng              numeric(9,6),
  phone            text,
  manager_name_en  text,
  manager_name_ar  text,
  hours_en         text,
  hours_ar         text
);

create table if not exists public.districts (
  district_id        text primary key check (district_id ~ '^DST-[0-9]{3}$'),
  city_en            text not null,
  city_ar            text not null,
  name_en            text not null,
  name_ar            text not null,
  centroid_lat       numeric(9,6) not null,
  centroid_lng       numeric(9,6) not null,
  short_code_prefix  text not null check (short_code_prefix ~ '^[A-Z]{4}$'),
  zone               text,
  served             boolean not null default true,
  centre_id          text references public.centres(centre_id)
);

create table if not exists public.pickup_points (
  pickup_point_id  text primary key check (pickup_point_id ~ '^PUP-[0-9]{3}$'),
  type             text not null check (type in ('Locker','Partner Point','Branch Counter')),
  name_en          text not null,
  name_ar          text not null,
  address_en       text,
  address_ar       text,
  city_en          text,
  lat              numeric(9,6) not null,
  lng              numeric(9,6) not null,
  capacity         int not null default 0 check (capacity >= 0),
  hours_en         text,
  hours_ar         text,
  hold_hours       int not null default 72 check (hold_hours > 0)
);

create table if not exists public.stores (
  store_id            text primary key check (store_id ~ '^STR-[0-9]{3}$'),
  name_en             text not null,
  name_ar             text not null,
  category_en         text,
  category_ar         text,
  return_window_days  int check (return_window_days >= 0),
  contact_phone       text,
  contact_email       text,
  website             text
);

create table if not exists public.drivers (
  driver_id      text primary key check (driver_id ~ '^DRV-[0-9]{3}$'),
  name_en        text not null,
  name_ar        text not null,
  phone          text,
  vehicle_plate  text,
  centre_id      text references public.centres(centre_id)
);

create table if not exists public.business_rules (
  rule_key        text primary key,
  value_num       numeric,
  value_text      text,
  unit            text,
  description_en  text,
  description_ar  text,
  check (value_num is not null or value_text is not null)
);

create table if not exists public.delivery_windows (
  window_code  text primary key check (window_code ~ '^W[0-9]$'),
  start_time   time not null,
  end_time     time not null,
  label_en     text not null,
  label_ar     text not null,
  check (end_time > start_time)
);

create table if not exists public.demo_meta (
  key    text primary key,
  value  text
);

create table if not exists public.demo_assets (
  asset_id        text primary key,
  persona_id      text,           -- customer_id of the persona (per-tenant id, no FK)
  use_case        text,           -- e.g. 'L-15'
  title_en        text not null,
  title_ar        text,
  storage_path    text not null,  -- path inside bucket `documents`, e.g. demo-assets/pod/door-unknown.jpg
  content_type    text not null,
  description_en  text
);

-- =============================================================================
-- DEMO USERS (§1.3)
-- =============================================================================

create table if not exists public.demo_users (
  owner_id         uuid primary key,
  email            text,
  whatsapp_number  text unique not null check (whatsapp_number ~ '^\+[1-9][0-9]{7,14}$'),
  full_name        text,
  created_at       timestamptz default now()
);

-- =============================================================================
-- PER-TENANT TABLES (§2.3) — owner_id + composite PK / composite FKs
-- =============================================================================

create table if not exists public.customers (
  owner_id            uuid not null,
  customer_id         text not null check (customer_id ~ '^BRQ-C-[0-9]{4}$'),
  full_name_en        text not null,
  full_name_ar        text,
  phone               text,
  email               text,
  preferred_language  text,
  city_en             text,
  city_ar             text,
  district_id         text references public.districts(district_id),
  address_en          text,
  address_ar          text,
  short_address       text,
  home_lat            numeric(9,6),
  home_lng            numeric(9,6),
  customer_type       text default 'Individual' check (customer_type in ('Individual','Business')),
  preferences         jsonb not null default '{"preferred_window":null,"earliest_time":null,"call_before_arrival":false,"avoid_prayer_times":false,"leave_with_security":false}'::jsonb,
  company_name_en     text,
  company_name_ar     text,
  vat_number          text,
  status              text default 'Active' check (status in ('Active','Inactive','Blocked')),
  registered_since    date,
  demo_notes          text,
  created_at          timestamptz default now(),
  updated_at          timestamptz default now(),
  primary key (owner_id, customer_id)
);

create table if not exists public.shipments (
  owner_id                 uuid not null,
  tracking_number          text not null check (tracking_number ~ '^BRQ[0-9]{8}$'),
  customer_id              text not null,
  store_id                 text references public.stores(store_id),
  order_ref                text,
  description_en           text,
  description_ar           text,
  declared_value_sar       numeric(12,2),
  weight_kg                numeric(8,2),
  origin_country           text default 'SA',
  is_international         boolean not null default false,
  service_type             text not null default 'Home Delivery'
                             check (service_type in ('Home Delivery','Pickup Point','Branch Pickup')),
  status                   text not null check (status in (
                             'Created','Picked Up','In Transit','At Sorting Centre','Out for Delivery',
                             'Delivery Attempted','On Hold','Customs Hold','At Branch','In Locker',
                             'Held — Customer Request','Delivered','Return Requested','Returned to Sender')),
  hold_reason_en           text,
  hold_reason_ar           text,
  current_centre_id        text references public.centres(centre_id),
  delivery_district_id     text references public.districts(district_id),
  delivery_address_en      text,
  delivery_address_ar      text,
  delivery_short_address   text,
  delivery_lat             numeric(9,6),
  delivery_lng             numeric(9,6),
  pickup_point_id          text references public.pickup_points(pickup_point_id),
  locker_code              text,
  scheduled_date           date,
  scheduled_window         text references public.delivery_windows(window_code),
  eta_date                 date,
  time_rule                jsonb not null default '{}'::jsonb,
  driver_id                text references public.drivers(driver_id),
  route_stop               int,
  route_total              int,
  eta_start                text check (eta_start is null or eta_start ~ '^[0-2][0-9]:[0-5][0-9]$'),
  eta_end                  text check (eta_end   is null or eta_end   ~ '^[0-2][0-9]:[0-5][0-9]$'),
  driver_notes             jsonb not null default '[]'::jsonb,
  cod_amount_sar           numeric(12,2) not null default 0,
  cod_status               text not null default 'None'
                             check (cod_status in ('None','Pending','Paid Online','Collected')),
  requires_delivery_code   boolean not null default false,
  delivery_code            text,
  delivery_code_issued_at  timestamptz,
  attempt_count            int not null default 0,
  last_scan_at             timestamptz,
  last_scan_location_en    text,
  last_scan_location_ar    text,
  return_to_sender_date    date,
  hold_until               date,
  consolidation_group      text,
  delivered_at             timestamptz,
  pod                      jsonb,
  pod_locked               boolean not null default false,
  tracking_token           text unique,
  shipping_fee_sar         numeric(10,2),
  shipping_paid_by         text check (shipping_paid_by in ('Store','Customer')),
  customs                  jsonb,
  created_at               timestamptz default now(),
  updated_at               timestamptz default now(),
  primary key (owner_id, tracking_number),
  foreign key (owner_id, customer_id) references public.customers(owner_id, customer_id) on delete cascade
);

create table if not exists public.shipment_events (
  owner_id         uuid not null,
  event_id         text not null check (event_id ~ '^EVT-[0-9]{6}$'),
  tracking_number  text not null,
  event_at         timestamptz not null,
  status           text not null,
  location_en      text,
  location_ar      text,
  note_en          text,
  note_ar          text,
  source           text default 'System' check (source in ('System','Driver','Agent','Staff')),
  created_at       timestamptz default now(),
  primary key (owner_id, event_id),
  foreign key (owner_id, tracking_number) references public.shipments(owner_id, tracking_number) on delete cascade
);

create table if not exists public.authorized_receivers (
  owner_id         uuid not null,
  receiver_id      text not null check (receiver_id ~ '^RCV-[0-9]{5}$'),
  tracking_number  text not null,
  receiver_type    text not null check (receiver_type in ('Building Security','Named Person')),
  full_name        text,
  id_last4         text check (id_last4 is null or id_last4 ~ '^[0-9]{4}$'),
  relationship_en  text,
  relationship_ar  text,
  consent_at       timestamptz default now(),
  status           text not null default 'Active' check (status in ('Active','Revoked')),
  created_at       timestamptz default now(),
  primary key (owner_id, receiver_id),
  foreign key (owner_id, tracking_number) references public.shipments(owner_id, tracking_number) on delete cascade
);

create table if not exists public.cases (
  owner_id         uuid not null,
  case_id          text not null check (case_id ~ '^(CLM|INV|CMP)-[0-9]{5}$'),
  case_type        text not null check (case_type in ('Damage Claim','Non-Delivery Investigation','Complaint')),
  tracking_number  text,
  customer_id      text not null,
  status           text not null default 'Open'
                     check (status in ('Open','Under Review','Awaiting Customer','Resolved','Rejected','Escalated')),
  description_en   text,
  description_ar   text,
  opened_at        timestamptz default now(),
  due_at           timestamptz,
  assigned_to_en   text,
  assigned_to_ar   text,
  attachments      jsonb not null default '[]'::jsonb,
  resolution_en    text,
  resolution_ar    text,
  store_notified   boolean not null default false,
  regulator_phone  text,
  created_at       timestamptz default now(),
  updated_at       timestamptz default now(),
  primary key (owner_id, case_id),
  foreign key (owner_id, customer_id)     references public.customers(owner_id, customer_id) on delete cascade,
  foreign key (owner_id, tracking_number) references public.shipments(owner_id, tracking_number) on delete cascade
);

create table if not exists public.returns (
  owner_id         uuid not null,
  return_id        text not null check (return_id ~ '^RET-[0-9]{5}$'),
  tracking_number  text not null,
  customer_id      text not null,
  reason_code      text not null check (reason_code in ('Wrong Item','Wrong Size','Damaged','Changed Mind','Other')),
  reason_detail    text,
  status           text not null default 'Requested'
                     check (status in ('Requested','Pickup Booked','Picked Up','Returned')),
  pickup_date      date,
  pickup_window    text references public.delivery_windows(window_code),
  label_ref        text,
  created_at       timestamptz default now(),
  updated_at       timestamptz default now(),
  primary key (owner_id, return_id),
  foreign key (owner_id, customer_id)     references public.customers(owner_id, customer_id) on delete cascade,
  foreign key (owner_id, tracking_number) references public.shipments(owner_id, tracking_number) on delete cascade
);

create table if not exists public.payment_requests (
  owner_id         uuid not null,
  payment_id       text not null check (payment_id ~ '^PAY-[0-9]{5}$'),
  customer_id      text not null,
  tracking_number  text,
  purpose          text not null check (purpose in ('COD','Customs')),
  amount_sar       numeric(12,2) not null check (amount_sar >= 0),
  status           text not null default 'Pending' check (status in ('Pending','Paid','Cancelled')),
  method           text check (method is null or method in ('mada','Apple Pay','Credit Card','STC Pay')),
  pay_token        text unique,
  created_at       timestamptz default now(),
  paid_at          timestamptz,
  line_items       jsonb not null default '[]'::jsonb,
  updated_at       timestamptz default now(),
  primary key (owner_id, payment_id),
  foreign key (owner_id, customer_id)     references public.customers(owner_id, customer_id) on delete cascade,
  foreign key (owner_id, tracking_number) references public.shipments(owner_id, tracking_number) on delete cascade
);

create table if not exists public.tax_invoices (
  owner_id         uuid not null,
  tax_invoice_id   text not null check (tax_invoice_id ~ '^TAX-[0-9]{5}$'),
  customer_id      text not null,
  tracking_number  text not null,
  company_name_en  text,
  company_name_ar  text,
  vat_number       text check (vat_number is null or vat_number ~ '^3[0-9]{13}3$'),
  subtotal_sar     numeric(12,2),
  vat_sar          numeric(12,2),
  total_sar        numeric(12,2),
  issued_at        timestamptz default now(),
  created_at       timestamptz default now(),
  primary key (owner_id, tax_invoice_id),
  foreign key (owner_id, customer_id)     references public.customers(owner_id, customer_id) on delete cascade,
  foreign key (owner_id, tracking_number) references public.shipments(owner_id, tracking_number) on delete cascade
);

create table if not exists public.store_alerts (
  owner_id     uuid not null,
  alert_id     text not null check (alert_id ~ '^ALR-[0-9]{5}$'),
  customer_id  text not null,
  store_id     text references public.stores(store_id),
  order_ref    text,
  status       text not null default 'Active' check (status in ('Active','Triggered','Cancelled')),
  created_at   timestamptz default now(),
  note_en      text,
  primary key (owner_id, alert_id),
  foreign key (owner_id, customer_id) references public.customers(owner_id, customer_id) on delete cascade
);

-- Audit log — never cloned, no FKs to entity tables (the audit trail must
-- survive whatever the entity rows go through).
create table if not exists public.agent_actions (
  id               bigserial primary key,
  owner_id         uuid not null,
  customer_id      text,
  tracking_number  text,
  action_type      text not null,
  description      text,
  metadata         jsonb not null default '{}'::jsonb,
  status           text not null default 'Success' check (status in ('Success','Failed')),
  source           text not null default 'Agent' check (source in ('Agent','Portal','Pay Page')),
  created_at       timestamptz not null default now()
);

-- ---------------------------------------------------------------- indexes
create index if not exists customers_owner_phone_idx          on public.customers (owner_id, phone);
create index if not exists shipments_owner_customer_idx       on public.shipments (owner_id, customer_id);
create index if not exists shipments_owner_status_idx         on public.shipments (owner_id, status);
create index if not exists shipment_events_owner_tracking_idx on public.shipment_events (owner_id, tracking_number, event_at desc);
create index if not exists receivers_owner_tracking_idx       on public.authorized_receivers (owner_id, tracking_number);
create index if not exists cases_owner_customer_idx           on public.cases (owner_id, customer_id);
create index if not exists cases_owner_tracking_idx           on public.cases (owner_id, tracking_number);
create index if not exists returns_owner_customer_idx         on public.returns (owner_id, customer_id);
create index if not exists returns_owner_tracking_idx         on public.returns (owner_id, tracking_number);
create index if not exists payreq_owner_customer_idx          on public.payment_requests (owner_id, customer_id);
create index if not exists payreq_owner_tracking_idx          on public.payment_requests (owner_id, tracking_number);
create index if not exists taxinv_owner_customer_idx          on public.tax_invoices (owner_id, customer_id);
create index if not exists taxinv_owner_tracking_idx          on public.tax_invoices (owner_id, tracking_number);
create index if not exists alerts_owner_customer_idx          on public.store_alerts (owner_id, customer_id);
create index if not exists agent_actions_owner_created_idx    on public.agent_actions (owner_id, created_at desc);
create index if not exists agent_actions_owner_customer_idx   on public.agent_actions (owner_id, customer_id);
create index if not exists agent_actions_owner_tracking_idx   on public.agent_actions (owner_id, tracking_number);
-- tracking_token / pay_token: UNIQUE constraints above (global, across tenants).

-- ---------------------------------------------------------------- owner_id → auth.users (cascade cleanup)
-- Guarded so the migration also applies where auth.users is absent.
do $$
declare t text;
begin
  if to_regclass('auth.users') is null then
    raise notice 'auth.users not found — skipping owner_id FKs to auth.users';
    return;
  end if;
  foreach t in array array['demo_users','customers','shipments','shipment_events','authorized_receivers',
                           'cases','returns','payment_requests','tax_invoices','store_alerts','agent_actions']
  loop
    if not exists (select 1 from pg_constraint
                   where conname = t || '_owner_fk' and conrelid = format('public.%I', t)::regclass) then
      execute format('alter table public.%I add constraint %I foreign key (owner_id) references auth.users(id) on delete cascade',
                     t, t || '_owner_fk');
    end if;
  end loop;
end $$;

-- =============================================================================
-- BASELINE (*_backup) TABLES — same columns minus owner_id, PK = business id.
-- Seeded by scripts/seed_baseline.py. `updated_at` exists on live tables only.
-- =============================================================================

create table if not exists public.customers_backup (
  customer_id         text primary key check (customer_id ~ '^BRQ-C-[0-9]{4}$'),
  full_name_en        text not null,
  full_name_ar        text,
  phone               text,
  email               text,
  preferred_language  text,
  city_en             text,
  city_ar             text,
  district_id         text references public.districts(district_id),
  address_en          text,
  address_ar          text,
  short_address       text,
  home_lat            numeric(9,6),
  home_lng            numeric(9,6),
  customer_type       text default 'Individual' check (customer_type in ('Individual','Business')),
  preferences         jsonb not null default '{"preferred_window":null,"earliest_time":null,"call_before_arrival":false,"avoid_prayer_times":false,"leave_with_security":false}'::jsonb,
  company_name_en     text,
  company_name_ar     text,
  vat_number          text,
  status              text default 'Active' check (status in ('Active','Inactive','Blocked')),
  registered_since    date,
  demo_notes          text,
  created_at          timestamptz default now()
);

create table if not exists public.shipments_backup (
  tracking_number          text primary key check (tracking_number ~ '^BRQ[0-9]{8}$'),
  customer_id              text not null references public.customers_backup(customer_id) on delete cascade,
  store_id                 text references public.stores(store_id),
  order_ref                text,
  description_en           text,
  description_ar           text,
  declared_value_sar       numeric(12,2),
  weight_kg                numeric(8,2),
  origin_country           text default 'SA',
  is_international         boolean not null default false,
  service_type             text not null default 'Home Delivery'
                             check (service_type in ('Home Delivery','Pickup Point','Branch Pickup')),
  status                   text not null check (status in (
                             'Created','Picked Up','In Transit','At Sorting Centre','Out for Delivery',
                             'Delivery Attempted','On Hold','Customs Hold','At Branch','In Locker',
                             'Held — Customer Request','Delivered','Return Requested','Returned to Sender')),
  hold_reason_en           text,
  hold_reason_ar           text,
  current_centre_id        text references public.centres(centre_id),
  delivery_district_id     text references public.districts(district_id),
  delivery_address_en      text,
  delivery_address_ar      text,
  delivery_short_address   text,
  delivery_lat             numeric(9,6),
  delivery_lng             numeric(9,6),
  pickup_point_id          text references public.pickup_points(pickup_point_id),
  locker_code              text,
  scheduled_date           date,
  scheduled_window         text references public.delivery_windows(window_code),
  eta_date                 date,
  time_rule                jsonb not null default '{}'::jsonb,
  driver_id                text references public.drivers(driver_id),
  route_stop               int,
  route_total              int,
  eta_start                text,
  eta_end                  text,
  driver_notes             jsonb not null default '[]'::jsonb,
  cod_amount_sar           numeric(12,2) not null default 0,
  cod_status               text not null default 'None'
                             check (cod_status in ('None','Pending','Paid Online','Collected')),
  requires_delivery_code   boolean not null default false,
  delivery_code            text,
  delivery_code_issued_at  timestamptz,
  attempt_count            int not null default 0,
  last_scan_at             timestamptz,
  last_scan_location_en    text,
  last_scan_location_ar    text,
  return_to_sender_date    date,
  hold_until               date,
  consolidation_group      text,
  delivered_at             timestamptz,
  pod                      jsonb,
  pod_locked               boolean not null default false,
  tracking_token           text unique,
  shipping_fee_sar         numeric(10,2),
  shipping_paid_by         text check (shipping_paid_by in ('Store','Customer')),
  customs                  jsonb,
  created_at               timestamptz default now()
);

create table if not exists public.shipment_events_backup (
  event_id         text primary key check (event_id ~ '^EVT-[0-9]{6}$'),
  tracking_number  text not null references public.shipments_backup(tracking_number) on delete cascade,
  event_at         timestamptz not null,
  status           text not null,
  location_en      text,
  location_ar      text,
  note_en          text,
  note_ar          text,
  source           text default 'System' check (source in ('System','Driver','Agent','Staff')),
  created_at       timestamptz default now()
);

create table if not exists public.authorized_receivers_backup (
  receiver_id      text primary key,
  tracking_number  text not null references public.shipments_backup(tracking_number) on delete cascade,
  receiver_type    text not null check (receiver_type in ('Building Security','Named Person')),
  full_name        text,
  id_last4         text,
  relationship_en  text,
  relationship_ar  text,
  consent_at       timestamptz default now(),
  status           text not null default 'Active' check (status in ('Active','Revoked')),
  created_at       timestamptz default now()
);

create table if not exists public.cases_backup (
  case_id          text primary key,
  case_type        text not null check (case_type in ('Damage Claim','Non-Delivery Investigation','Complaint')),
  tracking_number  text references public.shipments_backup(tracking_number) on delete cascade,
  customer_id      text not null references public.customers_backup(customer_id) on delete cascade,
  status           text not null default 'Open'
                     check (status in ('Open','Under Review','Awaiting Customer','Resolved','Rejected','Escalated')),
  description_en   text,
  description_ar   text,
  opened_at        timestamptz default now(),
  due_at           timestamptz,
  assigned_to_en   text,
  assigned_to_ar   text,
  attachments      jsonb not null default '[]'::jsonb,
  resolution_en    text,
  resolution_ar    text,
  store_notified   boolean not null default false,
  regulator_phone  text,
  created_at       timestamptz default now()
);

create table if not exists public.returns_backup (
  return_id        text primary key,
  tracking_number  text not null references public.shipments_backup(tracking_number) on delete cascade,
  customer_id      text not null references public.customers_backup(customer_id) on delete cascade,
  reason_code      text not null check (reason_code in ('Wrong Item','Wrong Size','Damaged','Changed Mind','Other')),
  reason_detail    text,
  status           text not null default 'Requested'
                     check (status in ('Requested','Pickup Booked','Picked Up','Returned')),
  pickup_date      date,
  pickup_window    text references public.delivery_windows(window_code),
  label_ref        text,
  created_at       timestamptz default now()
);

create table if not exists public.payment_requests_backup (
  payment_id       text primary key,
  customer_id      text not null references public.customers_backup(customer_id) on delete cascade,
  tracking_number  text references public.shipments_backup(tracking_number) on delete cascade,
  purpose          text not null check (purpose in ('COD','Customs')),
  amount_sar       numeric(12,2) not null,
  status           text not null default 'Pending' check (status in ('Pending','Paid','Cancelled')),
  method           text,
  pay_token        text unique,
  created_at       timestamptz default now(),
  paid_at          timestamptz,
  line_items       jsonb not null default '[]'::jsonb
);

create table if not exists public.tax_invoices_backup (
  tax_invoice_id   text primary key,
  customer_id      text not null references public.customers_backup(customer_id) on delete cascade,
  tracking_number  text not null references public.shipments_backup(tracking_number) on delete cascade,
  company_name_en  text,
  company_name_ar  text,
  vat_number       text,
  subtotal_sar     numeric(12,2),
  vat_sar          numeric(12,2),
  total_sar        numeric(12,2),
  issued_at        timestamptz default now(),
  created_at       timestamptz default now()
);

create table if not exists public.store_alerts_backup (
  alert_id     text primary key,
  customer_id  text not null references public.customers_backup(customer_id) on delete cascade,
  store_id     text references public.stores(store_id),
  order_ref    text,
  status       text not null default 'Active' check (status in ('Active','Triggered','Cancelled')),
  created_at   timestamptz default now(),
  note_en      text
);

-- =============================================================================
-- FUNCTIONS
-- =============================================================================

-- updated_at maintenance on live tables
create or replace function public.set_updated_at() returns trigger
language plpgsql as $$
begin
  new.updated_at := now();
  return new;
end $$;

do $$
declare t text;
begin
  foreach t in array array['customers','shipments','cases','returns','payment_requests'] loop
    execute format('drop trigger if exists %I on public.%I', t || '_set_updated_at', t);
    execute format('create trigger %I before update on public.%I for each row execute function public.set_updated_at()',
                   t || '_set_updated_at', t);
  end loop;
end $$;

-- Per-vertical "relative to now" overrides applied after every clone (§1.4/§2.4).
-- Barq: none. Kept as a hook so the pattern matches Watheeq.
create or replace function public.apply_relative_overrides(p_owner uuid) returns void
language plpgsql security definer
set search_path = public, extensions, pg_temp
as $$
begin
  -- Barq Express has no relative-to-now overrides (SPEC §2.4). Out-for-Delivery
  -- ETAs are computed live by the API from route_stop.
  perform p_owner;
end $$;

-- Generic baseline clone with date shift (§1.3, §1.4).
-- For every per-tenant table (FK order), builds INSERT … SELECT from the
-- *_backup table's columns via information_schema:
--   date columns        → col + v_shift
--   timestamp(tz) cols  → col + v_shift * interval '1 day'
--   tracking_token /
--   pay_token           → fresh globally-unique token per clone
-- Rows the owner already has are left untouched (ON CONFLICT DO NOTHING), so
-- the function is safe to call twice.
create or replace function public.clone_baseline_for_user(p_owner uuid) returns jsonb
language plpgsql security definer
set search_path = public, extensions, pg_temp
as $$
declare
  v_anchor  date;
  v_tz      text;
  v_shift   int;
  v_tables  text[] := array['customers','shipments','shipment_events','authorized_receivers',
                            'cases','returns','payment_requests','tax_invoices','store_alerts'];
  t         text;
  v_cols    text;
  v_exprs   text;
  v_n       bigint;
  v_counts  jsonb := '{}'::jsonb;
begin
  if p_owner is null then
    raise exception 'clone_baseline_for_user: p_owner is required';
  end if;

  select value::date into v_anchor from public.demo_meta where key = 'anchor_date';
  if v_anchor is null then
    raise exception 'clone_baseline_for_user: demo_meta.anchor_date missing — run scripts/seed_baseline.py first';
  end if;
  -- "Today" is the Riyadh calendar date (DB clock is UTC; the API uses TZ_NAME
  -- = Asia/Riyadh). demo_meta.timezone overrides.
  select coalesce((select value from public.demo_meta where key = 'timezone'), 'Asia/Riyadh') into v_tz;
  v_shift := (now() at time zone v_tz)::date - v_anchor;

  foreach t in array v_tables loop
    select
      string_agg(quote_ident(c.column_name), ', ' order by c.ordinal_position),
      string_agg(
        case
          when c.column_name in ('tracking_token','pay_token')
            then 'encode(extensions.gen_random_bytes(12), ''hex'')'
          when c.data_type = 'date'
            then format('%I + %s', c.column_name, v_shift)
          when c.data_type in ('timestamp with time zone','timestamp without time zone')
            then format('%I + (%s * interval ''1 day'')', c.column_name, v_shift)
          else quote_ident(c.column_name)
        end,
        ', ' order by c.ordinal_position)
    into v_cols, v_exprs
    from information_schema.columns c
    where c.table_schema = 'public' and c.table_name = t || '_backup';

    if v_cols is null then
      raise exception 'clone_baseline_for_user: public.%_backup not found', t;
    end if;

    execute format('insert into public.%I (owner_id, %s) select $1, %s from public.%I on conflict do nothing',
                   t, v_cols, v_exprs, t || '_backup')
      using p_owner;
    get diagnostics v_n = row_count;
    v_counts := v_counts || jsonb_build_object(t, v_n);
  end loop;

  perform public.apply_relative_overrides(p_owner);

  return jsonb_build_object('ok', true, 'owner_id', p_owner, 'shift_days', v_shift,
                            'anchor_date', v_anchor, 'rows', v_counts);
end $$;

-- Reset the CALLER's demo data (§1.3). Children first, incl. agent_actions.
create or replace function public.reset_demo_data() returns json
language plpgsql security definer
set search_path = public, extensions, pg_temp
as $$
declare
  v_owner uuid := auth.uid();
  v_clone jsonb;
begin
  if v_owner is null then
    raise exception 'reset_demo_data: not authenticated' using errcode = '42501';
  end if;

  delete from public.agent_actions        where owner_id = v_owner;
  delete from public.store_alerts         where owner_id = v_owner;
  delete from public.tax_invoices         where owner_id = v_owner;
  delete from public.payment_requests     where owner_id = v_owner;
  delete from public.returns              where owner_id = v_owner;
  delete from public.cases                where owner_id = v_owner;
  delete from public.authorized_receivers where owner_id = v_owner;
  delete from public.shipment_events      where owner_id = v_owner;
  delete from public.shipments            where owner_id = v_owner;
  delete from public.customers            where owner_id = v_owner;

  v_clone := public.clone_baseline_for_user(v_owner);

  return json_build_object('ok', true, 'reset_at', now(), 'rows', v_clone -> 'rows',
                           'shift_days', v_clone -> 'shift_days');
end $$;

-- Optional helper — next tenant-scoped business id (the API computes its own).
-- next_business_id(owner, 'cases', 'case_id', 'CLM-', 5) → 'CLM-00001' / max+1
create or replace function public.next_business_id(p_owner uuid, p_table text, p_col text,
                                                   p_prefix text, p_width int) returns text
language plpgsql stable security definer
set search_path = public, extensions, pg_temp
as $$
declare v_max bigint;
begin
  execute format(
    'select max(nullif(regexp_replace(substr(%I, %s), ''[^0-9]'', '''', ''g''), '''')::bigint)
       from public.%I where owner_id = $1 and %I like $2',
    p_col, length(p_prefix) + 1, p_table, p_col)
  into v_max using p_owner, p_prefix || '%';
  return p_prefix || lpad((coalesce(v_max, 0) + 1)::text, p_width, '0');
end $$;

-- New auth user → demo_users (if metadata has a valid E.164 number) + clone.
-- Never blocks sign-up: failures are downgraded to warnings.
--   raw_user_meta_data.skip_clone = true → service account (Mode B): no
--   demo_users row, nothing cloned.
create or replace function public.handle_new_demo_user() returns trigger
language plpgsql security definer
set search_path = public, extensions, pg_temp
as $$
declare
  v_phone text := nullif(regexp_replace(coalesce(new.raw_user_meta_data ->> 'whatsapp_number', ''), '[\s\-\(\)]', '', 'g'), '');
  v_name  text := nullif(new.raw_user_meta_data ->> 'full_name', '');
begin
  if coalesce(new.raw_user_meta_data ->> 'skip_clone', 'false') = 'true' then
    return new;
  end if;

  if v_phone like '00%' then
    v_phone := '+' || substr(v_phone, 3);
  end if;

  if v_phone ~ '^\+[1-9][0-9]{7,14}$' then
    begin
      insert into public.demo_users (owner_id, email, whatsapp_number, full_name)
      values (new.id, new.email, v_phone, v_name)
      on conflict (owner_id) do update
        set email = excluded.email,
            whatsapp_number = excluded.whatsapp_number,
            full_name = coalesce(excluded.full_name, public.demo_users.full_name);
    exception when unique_violation then
      raise warning 'handle_new_demo_user: whatsapp_number % already registered to another user — demo_users row not created for %',
        v_phone, new.id;
    end;
  elsif v_phone is not null then
    raise warning 'handle_new_demo_user: whatsapp_number % is not E.164 — demo_users row not created for %', v_phone, new.id;
  end if;

  begin
    perform public.clone_baseline_for_user(new.id);
  exception when others then
    raise warning 'handle_new_demo_user: baseline clone failed for %: %', new.id, sqlerrm;
  end;

  return new;
end $$;

do $$
begin
  if to_regclass('auth.users') is not null then
    execute 'drop trigger if exists on_auth_user_created_barq on auth.users';
    execute 'create trigger on_auth_user_created_barq after insert on auth.users
             for each row execute function public.handle_new_demo_user()';
  else
    raise notice 'auth.users not found — clone trigger not installed';
  end if;
end $$;

-- =============================================================================
-- GRANTS + ROW LEVEL SECURITY (§1.3)
-- =============================================================================

do $$
declare
  v_shared  text[] := array['districts','centres','pickup_points','stores','drivers','business_rules',
                            'delivery_windows','demo_meta','demo_assets'];
  v_tenant  text[] := array['customers','shipments','shipment_events','authorized_receivers','cases',
                            'returns','payment_requests','tax_invoices','store_alerts','agent_actions'];
  v_backup  text[] := array['customers_backup','shipments_backup','shipment_events_backup',
                            'authorized_receivers_backup','cases_backup','returns_backup',
                            'payment_requests_backup','tax_invoices_backup','store_alerts_backup'];
  v_has_jwt boolean := to_regprocedure('auth.jwt()') is not null;
  t text;
begin
  -- shared: authenticated read-only
  foreach t in array v_shared loop
    execute format('alter table public.%I enable row level security', t);
    execute format('revoke all on public.%I from anon, public', t);
    execute format('revoke insert, update, delete, truncate on public.%I from authenticated', t);
    execute format('grant select on public.%I to authenticated', t);
    execute format('grant all on public.%I to service_role', t);
    execute format('drop policy if exists %I on public.%I', t || '_read', t);
    execute format('create policy %I on public.%I for select to authenticated using (true)', t || '_read', t);
  end loop;

  -- per-tenant: own rows only
  foreach t in array v_tenant loop
    execute format('alter table public.%I enable row level security', t);
    execute format('revoke all on public.%I from anon, public', t);
    execute format('revoke truncate on public.%I from authenticated', t);
    execute format('grant select, insert, update, delete on public.%I to authenticated', t);
    execute format('grant all on public.%I to service_role', t);
    execute format('drop policy if exists %I on public.%I', t || '_owner_all', t);
    execute format('create policy %I on public.%I for all to authenticated
                      using (owner_id = auth.uid()) with check (owner_id = auth.uid())', t || '_owner_all', t);
    -- Mode B (API signs in as a service-account user): additive cross-tenant
    -- policy for users whose app_metadata.role = 'service_agent' (set by an
    -- admin only — users cannot write app_metadata).
    execute format('drop policy if exists %I on public.%I', t || '_service_agent_rw', t);
    if v_has_jwt then
      execute format('create policy %I on public.%I for all to authenticated
                        using ((auth.jwt() -> ''app_metadata'' ->> ''role'') = ''service_agent'')
                        with check ((auth.jwt() -> ''app_metadata'' ->> ''role'') = ''service_agent'')',
                     t || '_service_agent_rw', t);
    end if;
  end loop;

  -- backups: service role only (RLS on, no policies for authenticated)
  foreach t in array v_backup loop
    execute format('alter table public.%I enable row level security', t);
    execute format('revoke all on public.%I from anon, authenticated, public', t);
    execute format('grant all on public.%I to service_role', t);
  end loop;
end $$;

revoke all on sequence public.agent_actions_id_seq from anon, public;
grant usage, select on sequence public.agent_actions_id_seq to authenticated, service_role;

-- demo_users: own row only (select / update; insert own row so the portal can
-- register a number for users created without metadata)
alter table public.demo_users enable row level security;
revoke all on public.demo_users from anon, public;
revoke delete, truncate on public.demo_users from authenticated;
grant select, insert, update on public.demo_users to authenticated;
grant all on public.demo_users to service_role;
drop policy if exists demo_users_select_own on public.demo_users;
drop policy if exists demo_users_update_own on public.demo_users;
drop policy if exists demo_users_insert_own on public.demo_users;
create policy demo_users_select_own on public.demo_users for select to authenticated using (owner_id = auth.uid());
create policy demo_users_update_own on public.demo_users for update to authenticated
  using (owner_id = auth.uid()) with check (owner_id = auth.uid());
create policy demo_users_insert_own on public.demo_users for insert to authenticated with check (owner_id = auth.uid());
-- service agent (Mode B) may read/write all rows for tenant routing
drop policy if exists demo_users_service_agent_rw on public.demo_users;
do $$ begin
  if to_regprocedure('auth.jwt()') is not null then
    create policy demo_users_service_agent_rw on public.demo_users for all to authenticated
      using ((auth.jwt() -> 'app_metadata' ->> 'role') = 'service_agent')
      with check ((auth.jwt() -> 'app_metadata' ->> 'role') = 'service_agent');
  end if;
end $$;

-- functions: clone/helper = service role only; reset = signed-in users
revoke all on function public.clone_baseline_for_user(uuid)                    from public, anon, authenticated;
revoke all on function public.apply_relative_overrides(uuid)                   from public, anon, authenticated;
revoke all on function public.next_business_id(uuid, text, text, text, int)   from public, anon, authenticated;
revoke all on function public.handle_new_demo_user()                           from public, anon, authenticated;
revoke all on function public.reset_demo_data()                                from public, anon;
grant execute on function public.clone_baseline_for_user(uuid)                  to service_role;
grant execute on function public.apply_relative_overrides(uuid)                 to service_role;
grant execute on function public.next_business_id(uuid, text, text, text, int) to service_role;
grant execute on function public.reset_demo_data()                              to authenticated, service_role;

-- =============================================================================
-- REALTIME (§2.3) — guarded, re-runnable
-- =============================================================================
do $$
declare t text;
begin
  if not exists (select 1 from pg_publication where pubname = 'supabase_realtime') then
    create publication supabase_realtime;
  end if;
  foreach t in array array['agent_actions','shipments','shipment_events','cases','payment_requests',
                           'returns','authorized_receivers','customers','tax_invoices','store_alerts'] loop
    if not exists (select 1 from pg_publication_tables
                   where pubname = 'supabase_realtime' and schemaname = 'public' and tablename = t) then
      execute format('alter publication supabase_realtime add table public.%I', t);
    end if;
  end loop;
end $$;

-- =============================================================================
-- STORAGE — bucket `documents` (private) + read policy
--   <owner_id>/<type>/…       generated PDFs (API, service role)
--   attachments/<owner_id>/…  customer media copies (API, service role)
--   demo-assets/…             Demo Kit files (scripts/upload_assets.py)
-- Authenticated users may read demo-assets/*, objects under their own
-- <owner_id>/ prefix and attachments/<owner_id>/. Writes stay with the service role.
-- Guarded so the migration applies where the storage schema is absent.
-- =============================================================================
do $$
begin
  if to_regclass('storage.buckets') is null or to_regclass('storage.objects') is null then
    raise notice 'storage schema not found — bucket/policies skipped';
    return;
  end if;
  insert into storage.buckets (id, name, public) values ('documents', 'documents', false)
    on conflict do nothing;
  execute 'drop policy if exists barq_documents_read on storage.objects';
  execute $p$
    create policy barq_documents_read on storage.objects for select to authenticated
    using (
      bucket_id = 'documents'
      and (
        split_part(name, '/', 1) = 'demo-assets'
        or split_part(name, '/', 1) = auth.uid()::text
        or (split_part(name, '/', 1) = 'attachments' and split_part(name, '/', 2) = auth.uid()::text)
      )
    )
  $p$;
end $$;

-- PostgREST schema cache reload (hosted Supabase + local harness)
notify pgrst, 'reload schema';
