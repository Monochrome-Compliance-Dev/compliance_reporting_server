-- seed_maximiser_demo.sql
-- Comprehensive demo data to light up Maximiser (predictability, value mix, focus, mentor/support, burnout)
-- Safe to run multiple times.

BEGIN;

-- =========================
-- 0) Safety: ensure teams are set on resources that already exist
-- =========================
UPDATE public.tbl_pulse_resource
SET team='Advisory', "updatedBy"='seed', "updatedAt"=now()
WHERE id='NAUZmkM_qQ' AND "customerId"='sqBxGzP123';

UPDATE public.tbl_pulse_resource
SET team='Assurance', "updatedBy"='seed', "updatedAt"=now()
WHERE id IN ('1234567890','STEVE00001','NATASHA001') AND "customerId"='sqBxGzP123';

-- =========================
-- 1) Resources (idempotent: create Steve & Natasha if missing)
-- =========================
INSERT INTO public.tbl_pulse_resource
  (id, "customerId", name, position, "hourlyRate", "capacityHoursPerWeek",
   email, "userId", "createdBy", "updatedBy", "createdAt", "updatedAt", team)
VALUES
  ('STEVE00001','sqBxGzP123','Steve','Analyst',140,40,NULL,NULL,'seed','seed',now(),now(),'Assurance'),
  ('NATASHA001','sqBxGzP123','Natasha','Analyst',150,40,NULL,NULL,'seed','seed',now(),now(),'Assurance')
ON CONFLICT (id) DO UPDATE
SET team=EXCLUDED.team, "updatedBy"='seed', "updatedAt"=now();

-- =========================
-- 2) Budget sections (ensure they exist)
-- =========================
INSERT INTO public.tbl_pulse_budget_section
  (id, "customerId", "budgetId", name, "order", "createdBy", "updatedBy", "createdAt", "updatedAt")
VALUES
  ('SEC0901',  'sqBxGzP123','BUDG0901','Control testing',1,'seed','seed',now(),now()),
  ('SEC0901D', 'sqBxGzP123','BUDG0901','Delivery',        2,'seed','seed',now(),now()),
  ('SEC0901P', 'sqBxGzP123','BUDG0901','Planning',        3,'seed','seed',now(),now())
ON CONFLICT (id) DO NOTHING;

-- =========================
-- 3) Budget items (must have non-null rate/amount)
-- =========================
INSERT INTO public.tbl_pulse_budget_item
  (id, "customerId", "budgetId", "sectionId", "engagementId",
   "resourceLabel", "sectionName", "billingType",
   hours, rate, amount, billable, notes, "order",
   "createdBy", "updatedBy", "createdAt", "updatedAt")
VALUES
  -- Advisory set
  ('bi_ct1',   'sqBxGzP123','BUDG0901','SEC0901',  'FS_QFU2t0Y', NULL,'Control testing','fixed', 40,150,40*150, TRUE,NULL,0,'seed','seed',now(),now()),
  ('bi_del1',  'sqBxGzP123','BUDG0901','SEC0901D', 'FS_QFU2t0Y', NULL,'Delivery',        'fixed',120,180,120*180,TRUE,NULL,1,'seed','seed',now(),now()),
  ('bi_plan1', 'sqBxGzP123','BUDG0901','SEC0901P', 'FS_QFU2t0Y', NULL,'Planning',        'fixed', 20,140,20*140,  TRUE,NULL,2,'seed','seed',now(),now()),
  -- Assurance set
  ('bi_ct2',   'sqBxGzP123','BUDG0901','SEC0901',  'FS_QFU2t0Y', NULL,'Control testing','fixed', 40,150,40*150, TRUE,NULL,0,'seed','seed',now(),now()),
  ('bi_del2',  'sqBxGzP123','BUDG0901','SEC0901D', 'FS_QFU2t0Y', NULL,'Delivery',        'fixed',120,180,120*180,TRUE,NULL,1,'seed','seed',now(),now()),
  ('bi_plan2', 'sqBxGzP123','BUDG0901','SEC0901P', 'FS_QFU2t0Y', NULL,'Planning',        'fixed', 20,140,20*140,  TRUE,NULL,2,'seed','seed',now(),now())
ON CONFLICT (id) DO UPDATE
SET "sectionId"=EXCLUDED."sectionId",
    hours=EXCLUDED.hours, rate=EXCLUDED.rate, amount=EXCLUDED.amount,
    "updatedBy"='seed', "updatedAt"=now();

-- =========================
-- 4) Timesheets for the week starting 2025-09-01 (Tony exists as Onp9QSXq07)
-- =========================
INSERT INTO public.tbl_pulse_timesheet
  (id, "customerId", "resourceId", "weekKey", status,
   "submittedAt", "submittedBy", "approvedAt", "approvedBy",
   "createdBy", "updatedBy", "createdAt", "updatedAt")
VALUES
  ('TSPEP0901','sqBxGzP123','NAUZmkM_qQ','2025-09-01','draft',NULL,NULL,NULL,NULL,'seed','seed',now(),now()),
  ('TSSTE0901','sqBxGzP123','STEVE00001','2025-09-01','draft',NULL,NULL,NULL,NULL,'seed','seed',now(),now()),
  ('TSNAT0901','sqBxGzP123','NATASHA001','2025-09-01','draft',NULL,NULL,NULL,NULL,'seed','seed',now(),now())
ON CONFLICT (id) DO NOTHING;

-- =========================
-- 5) Timesheet rows — baseline set (trow1..trow12)
-- =========================
INSERT INTO public.tbl_pulse_timesheet_row
  (id, "customerId", "timesheetId", "date", "engagementId", "budgetItemId",
   hours, billable, rate, notes, "createdBy", "updatedBy", "createdAt", "updatedAt")
VALUES
  -- Tony (Assurance, existing sheet Onp9QSXq07)
  ('trow1', 'sqBxGzP123','Onp9QSXq07','2025-09-01','FS_QFU2t0Y','bi_ct1',   4, TRUE, NULL, NULL, 'seed','seed',now(),now()),
  ('trow2', 'sqBxGzP123','Onp9QSXq07','2025-09-02','FS_QFU2t0Y','bi_del1',  5, TRUE, NULL, NULL, 'seed','seed',now(),now()),
  ('trow3', 'sqBxGzP123','Onp9QSXq07','2025-09-03','FS_QFU2t0Y','bi_plan1', 3, TRUE, NULL, NULL, 'seed','seed',now(),now()),
  -- Pepper (Advisory)
  ('trow4', 'sqBxGzP123','TSPEP0901', '2025-09-01','FS_QFU2t0Y','bi_ct1',   7, TRUE, NULL, NULL, 'seed','seed',now(),now()),
  ('trow5', 'sqBxGzP123','TSPEP0901', '2025-09-02','FS_QFU2t0Y','bi_del1',  8, TRUE, NULL, NULL, 'seed','seed',now(),now()),
  ('trow6', 'sqBxGzP123','TSPEP0901', '2025-09-03','FS_QFU2t0Y','bi_plan1', 6, TRUE, NULL, NULL, 'seed','seed',now(),now()),
  -- Steve (Assurance)
  ('trow7',  'sqBxGzP123','TSSTE0901','2025-09-01','FS_QFU2t0Y','bi_ct2',   5, TRUE, NULL, NULL, 'seed','seed',now(),now()),
  ('trow8',  'sqBxGzP123','TSSTE0901','2025-09-02','FS_QFU2t0Y','bi_del2',  4, TRUE, NULL, NULL, 'seed','seed',now(),now()),
  ('trow9',  'sqBxGzP123','TSSTE0901','2025-09-03','FS_QFU2t0Y','bi_plan2', 6, TRUE, NULL, NULL, 'seed','seed',now(),now()),
  -- Natasha (Assurance)
  ('trow10', 'sqBxGzP123','TSNAT0901','2025-09-01','FS_QFU2t0Y','bi_ct2',   9, TRUE, NULL, NULL, 'seed','seed',now(),now()),
  ('trow11', 'sqBxGzP123','TSNAT0901','2025-09-02','FS_QFU2t0Y','bi_del2', 10, TRUE, NULL, NULL, 'seed','seed',now(),now()),
  ('trow12', 'sqBxGzP123','TSNAT0901','2025-09-03','FS_QFU2t0Y','bi_plan2', 8, TRUE, NULL, NULL, 'seed','seed',now(),now())
ON CONFLICT (id) DO NOTHING;

-- =========================
-- 6) Timesheet rows — enrichment to trigger all signals (trow13..trow39)
-- =========================
-- Tony: efficient with multi-item days + a little non-billable
INSERT INTO public.tbl_pulse_timesheet_row
  (id, "customerId", "timesheetId", "date", "engagementId", "budgetItemId",
   hours, billable, rate, notes, "createdBy", "updatedBy", "createdAt", "updatedAt")
VALUES
('trow13','sqBxGzP123','Onp9QSXq07','2025-09-01','FS_QFU2t0Y','bi_ct1', 3, TRUE, NULL, NULL,'seed','seed',now(),now()),
('trow14','sqBxGzP123','Onp9QSXq07','2025-09-01','FS_QFU2t0Y','bi_del1', 3, TRUE, NULL, NULL,'seed','seed',now(),now()),
('trow15','sqBxGzP123','Onp9QSXq07','2025-09-01','FS_QFU2t0Y','bi_plan1',2, TRUE, NULL, NULL,'seed','seed',now(),now()),
('trow16','sqBxGzP123','Onp9QSXq07','2025-09-02','FS_QFU2t0Y','bi_ct1', 2, TRUE, NULL, NULL,'seed','seed',now(),now()),
('trow17','sqBxGzP123','Onp9QSXq07','2025-09-02','FS_QFU2t0Y','bi_del1', 3, TRUE, NULL, NULL,'seed','seed',now(),now()),
('trow18','sqBxGzP123','Onp9QSXq07','2025-09-02','FS_QFU2t0Y','bi_plan1',2, TRUE, NULL, NULL,'seed','seed',now(),now()),
('trow19','sqBxGzP123','Onp9QSXq07','2025-09-03','FS_QFU2t0Y','bi_plan1',2, TRUE, NULL, NULL,'seed','seed',now(),now()),
('trow20','sqBxGzP123','Onp9QSXq07','2025-09-03','FS_QFU2t0Y','bi_del1', 2, FALSE,NULL,'internal meeting','seed','seed',now(),now())
ON CONFLICT (id) DO NOTHING;

-- Pepper: higher hours + non-billable + multi-item days
INSERT INTO public.tbl_pulse_timesheet_row
  (id, "customerId", "timesheetId", "date", "engagementId", "budgetItemId",
   hours, billable, rate, notes, "createdBy", "updatedBy", "createdAt", "updatedAt")
VALUES
('trow21','sqBxGzP123','TSPEP0901','2025-09-01','FS_QFU2t0Y','bi_ct1', 7, TRUE, NULL, NULL,'seed','seed',now(),now()),
('trow22','sqBxGzP123','TSPEP0901','2025-09-01','FS_QFU2t0Y','bi_del1', 4, FALSE,NULL,'internal docs','seed','seed',now(),now()),
('trow23','sqBxGzP123','TSPEP0901','2025-09-02','FS_QFU2t0Y','bi_ct1', 4, TRUE, NULL, NULL,'seed','seed',now(),now()),
('trow24','sqBxGzP123','TSPEP0901','2025-09-02','FS_QFU2t0Y','bi_del1', 5, TRUE, NULL, NULL,'seed','seed',now(),now()),
('trow25','sqBxGzP123','TSPEP0901','2025-09-02','FS_QFU2t0Y','bi_plan1',3, FALSE,NULL,'training','seed','seed',now(),now()),
('trow26','sqBxGzP123','TSPEP0901','2025-09-03','FS_QFU2t0Y','bi_del1', 8, TRUE, NULL, NULL,'seed','seed',now(),now())
ON CONFLICT (id) DO NOTHING;

-- Steve: efficient + some non-billable
INSERT INTO public.tbl_pulse_timesheet_row
  (id, "customerId", "timesheetId", "date", "engagementId", "budgetItemId",
   hours, billable, rate, notes, "createdBy", "updatedBy", "createdAt", "updatedAt")
VALUES
('trow27','sqBxGzP123','TSSTE0901','2025-09-01','FS_QFU2t0Y','bi_ct2', 3, TRUE, NULL, NULL,'seed','seed',now(),now()),
('trow28','sqBxGzP123','TSSTE0901','2025-09-01','FS_QFU2t0Y','bi_del2', 2, TRUE, NULL, NULL,'seed','seed',now(),now()),
('trow29','sqBxGzP123','TSSTE0901','2025-09-01','FS_QFU2t0Y','bi_plan2',2, TRUE, NULL, NULL,'seed','seed',now(),now()),
('trow30','sqBxGzP123','TSSTE0901','2025-09-02','FS_QFU2t0Y','bi_ct2', 2, TRUE, NULL, NULL,'seed','seed',now(),now()),
('trow31','sqBxGzP123','TSSTE0901','2025-09-02','FS_QFU2t0Y','bi_del2', 3, TRUE, NULL, NULL,'seed','seed',now(),now()),
('trow32','sqBxGzP123','TSSTE0901','2025-09-03','FS_QFU2t0Y','bi_plan2',3, FALSE,NULL,'community','seed','seed',now(),now())
ON CONFLICT (id) DO NOTHING;

-- Natasha: 3 consecutive >=10h days + a little non-billable
INSERT INTO public.tbl_pulse_timesheet_row
  (id, "customerId", "timesheetId", "date", "engagementId", "budgetItemId",
   hours, billable, rate, notes, "createdBy", "updatedBy", "createdAt", "updatedAt")
VALUES
('trow33','sqBxGzP123','TSNAT0901','2025-09-01','FS_QFU2t0Y','bi_ct2', 5, TRUE, NULL, NULL,'seed','seed',now(),now()),
('trow34','sqBxGzP123','TSNAT0901','2025-09-01','FS_QFU2t0Y','bi_del2', 6, TRUE, NULL, NULL,'seed','seed',now(),now()),
('trow35','sqBxGzP123','TSNAT0901','2025-09-02','FS_QFU2t0Y','bi_ct2', 4, TRUE, NULL, NULL,'seed','seed',now(),now()),
('trow36','sqBxGzP123','TSNAT0901','2025-09-02','FS_QFU2t0Y','bi_del2', 7, TRUE, NULL, NULL,'seed','seed',now(),now()),
('trow37','sqBxGzP123','TSNAT0901','2025-09-03','FS_QFU2t0Y','bi_ct2', 6, TRUE, NULL, NULL,'seed','seed',now(),now()),
('trow38','sqBxGzP123','TSNAT0901','2025-09-03','FS_QFU2t0Y','bi_del2', 6, TRUE, NULL, NULL,'seed','seed',now(),now()),
('trow39','sqBxGzP123','TSNAT0901','2025-09-03','FS_QFU2t0Y','bi_plan2',1, FALSE,NULL,'internal demo','seed','seed',now(),now())
ON CONFLICT (id) DO NOTHING;

COMMIT;

-- =========================
-- 8) DEMO TEAM NEEDS_ASSISTANCE (Ops)
--    Purpose: create a team that clearly triggers Support Opportunity flags via API.
--    Characteristics:
--      • Two contributors per category (required for medians)
--      • One efficient (mentor-like), one inefficient (support-needed)
--      • High non-billable share (>30%)
--      • Heavy context switching (>2 items/day) for the struggling resource
-- =========================
BEGIN;

-- Resources (names ≤10 chars)
INSERT INTO public.tbl_pulse_resource
  (id,"customerId",name,position,"hourlyRate","capacityHoursPerWeek",
   email,"userId","createdBy","updatedBy","createdAt","updatedAt",team)
VALUES
  ('PETER00001','sqBxGzP123','Peter','Analyst',140,40,NULL,NULL,'seed','seed',now(),now(),'Ops'),
  ('QUILL00001','sqBxGzP123','Quill','Analyst',140,40,NULL,NULL,'seed','seed',now(),now(),'Ops')
ON CONFLICT (id) DO UPDATE
SET team=EXCLUDED.team, "updatedBy"='seed', "updatedAt"=now();

-- Budget items for Ops
INSERT INTO public.tbl_pulse_budget_item
  (id,"customerId","budgetId","sectionId","engagementId",
   "resourceLabel","sectionName","billingType",
   hours,rate,amount,billable,notes,"order",
   "createdBy","updatedBy","createdAt","updatedAt")
VALUES
  ('bi_ct5',  'sqBxGzP123','BUDG0901','SEC0901', 'FS_QFU2t0Y',NULL,'Control testing','fixed', 40,150,40*150, TRUE,NULL,0,'seed','seed',now(),now()),
  ('bi_del5', 'sqBxGzP123','BUDG0901','SEC0901D','FS_QFU2t0Y',NULL,'Delivery',        'fixed',120,180,120*180,TRUE,NULL,1,'seed','seed',now(),now()),
  ('bi_plan5','sqBxGzP123','BUDG0901','SEC0901P','FS_QFU2t0Y',NULL,'Planning',        'fixed', 20,140,20*140, TRUE,NULL,2,'seed','seed',now(),now())
ON CONFLICT (id) DO UPDATE
SET hours=EXCLUDED.hours, rate=EXCLUDED.rate, amount=EXCLUDED.amount,
    "sectionId"=EXCLUDED."sectionId", "updatedBy"='seed', "updatedAt"=now();

-- Timesheets (week starting 2025-09-01)
INSERT INTO public.tbl_pulse_timesheet
  (id,"customerId","resourceId","weekKey",status,
   "submittedAt","submittedBy","approvedAt","approvedBy",
   "createdBy","updatedBy","createdAt","updatedAt")
VALUES
  ('TSPET0901','sqBxGzP123','PETER00001','2025-09-01','draft',NULL,NULL,NULL,NULL,'seed','seed',now(),now()),
  ('TSQUI0901','sqBxGzP123','QUILL00001','2025-09-01','draft',NULL,NULL,NULL,NULL,'seed','seed',now(),now())
ON CONFLICT (id) DO NOTHING;

-- Peter (mentor-like: low avg hours by category, steady; purely billable)
INSERT INTO public.tbl_pulse_timesheet_row
 (id,"customerId","timesheetId","date","engagementId","budgetItemId",
  hours,billable,rate,notes,"createdBy","updatedBy","createdAt","updatedAt")
VALUES
 ('trow60','sqBxGzP123','TSPET0901','2025-09-01','FS_QFU2t0Y','bi_ct5', 2, TRUE,NULL,NULL,'seed','seed',now(),now()),
 ('trow61','sqBxGzP123','TSPET0901','2025-09-02','FS_QFU2t0Y','bi_del5',3, TRUE,NULL,NULL,'seed','seed',now(),now()),
 ('trow62','sqBxGzP123','TSPET0901','2025-09-03','FS_QFU2t0Y','bi_plan5',2, TRUE,NULL,NULL,'seed','seed',now(),now())
ON CONFLICT (id) DO NOTHING;

-- Quill (support-needed: high avg hours, lots of non-billable, heavy context switching)
INSERT INTO public.tbl_pulse_timesheet_row
 (id,"customerId","timesheetId","date","engagementId","budgetItemId",
  hours,billable,rate,notes,"createdBy","updatedBy","createdAt","updatedAt")
VALUES
 -- 9/1: three items, 11h total (include non-billable to push value mix)
 ('trow63','sqBxGzP123','TSQUI0901','2025-09-01','FS_QFU2t0Y','bi_ct5', 4, TRUE, NULL, NULL,'seed','seed',now(),now()),
 ('trow64','sqBxGzP123','TSQUI0901','2025-09-01','FS_QFU2t0Y','bi_del5',5, TRUE, NULL, NULL,'seed','seed',now(),now()),
 ('trow65','sqBxGzP123','TSQUI0901','2025-09-01','FS_QFU2t0Y','bi_plan5',2, FALSE,NULL,'rework / admin','seed','seed',now(),now()),
 -- 9/2: three items, 12h total (again with non-billable)
 ('trow66','sqBxGzP123','TSQUI0901','2025-09-02','FS_QFU2t0Y','bi_ct5', 3, TRUE, NULL, NULL,'seed','seed',now(),now()),
 ('trow67','sqBxGzP123','TSQUI0901','2025-09-02','FS_QFU2t0Y','bi_del5',6, TRUE, NULL, NULL,'seed','seed',now(),now()),
 ('trow68','sqBxGzP123','TSQUI0901','2025-09-02','FS_QFU2t0Y','bi_plan5',3, FALSE,NULL,'internal meeting','seed','seed',now(),now()),
 -- 9/3: two items, 9h total (billable only)
 ('trow69','sqBxGzP123','TSQUI0901','2025-09-03','FS_QFU2t0Y','bi_ct5', 4, TRUE, NULL, NULL,'seed','seed',now(),now()),
 ('trow70','sqBxGzP123','TSQUI0901','2025-09-03','FS_QFU2t0Y','bi_del5',5, TRUE, NULL, NULL,'seed','seed',now(),now())
ON CONFLICT (id) DO NOTHING;

COMMIT;

-- Optional: allow app role (or PUBLIC) to read views used by Maximiser & dashboard
-- Replace PUBLIC with your app role when known.
GRANT USAGE ON SCHEMA public TO PUBLIC;
GRANT SELECT ON
  public.v_weekly_burn,
  public.v_overruns,
  public.v_resource_utilisation,
  public.v_pulse_team_map,
  public.v_pulse_team_hours,
  public.v_pulse_estimation_by_category,
  public.v_pulse_context_switching,
  public.v_pulse_long_day_streaks,
  public.v_pulse_task_category_stats,
  public.v_pulse_task_category_medians
TO PUBLIC;

-- =========================
-- 7) EXTRA DEMO TEAMS (Tax, Risk) — idempotent block
--    Adds two more teams & people with varied patterns so multiple outcomes can be tested.
--    Safe to run multiple times.
-- =========================
BEGIN;

-- Teams & resources (names ≤10 chars)
INSERT INTO public.tbl_pulse_resource
  (id,"customerId",name,position,"hourlyRate","capacityHoursPerWeek",
   email,"userId","createdBy","updatedBy","createdAt","updatedAt",team)
VALUES
  ('BRUCE00001','sqBxGzP123','Bruce','Senior',180,40,NULL,NULL,'seed','seed',now(),now(),'Tax'),
  ('CAROL00001','sqBxGzP123','Carol','Analyst',140,40,NULL,NULL,'seed','seed',now(),now(),'Tax'),
  ('CLINT00001','sqBxGzP123','Clint','Senior',170,40,NULL,NULL,'seed','seed',now(),now(),'Risk'),
  ('WANDA00001','sqBxGzP123','Wanda','Analyst',145,40,NULL,NULL,'seed','seed',now(),now(),'Risk')
ON CONFLICT (id) DO UPDATE
SET team=EXCLUDED.team, "updatedBy"='seed', "updatedAt"=now();

-- Budget items for new teams (uses existing sections)
INSERT INTO public.tbl_pulse_budget_item
  (id,"customerId","budgetId","sectionId","engagementId",
   "resourceLabel","sectionName","billingType",
   hours,rate,amount,billable,notes,"order",
   "createdBy","updatedBy","createdAt","updatedAt")
VALUES
  -- Tax
  ('bi_ct3',  'sqBxGzP123','BUDG0901','SEC0901', 'FS_QFU2t0Y',NULL,'Control testing','fixed', 40,150,40*150, TRUE,NULL,0,'seed','seed',now(),now()),
  ('bi_del3', 'sqBxGzP123','BUDG0901','SEC0901D','FS_QFU2t0Y',NULL,'Delivery',        'fixed',120,180,120*180,TRUE,NULL,1,'seed','seed',now(),now()),
  ('bi_plan3','sqBxGzP123','BUDG0901','SEC0901P','FS_QFU2t0Y',NULL,'Planning',        'fixed', 20,140,20*140, TRUE,NULL,2,'seed','seed',now(),now()),
  -- Risk
  ('bi_ct4',  'sqBxGzP123','BUDG0901','SEC0901', 'FS_QFU2t0Y',NULL,'Control testing','fixed', 40,150,40*150, TRUE,NULL,0,'seed','seed',now(),now()),
  ('bi_del4', 'sqBxGzP123','BUDG0901','SEC0901D','FS_QFU2t0Y',NULL,'Delivery',        'fixed',120,180,120*180,TRUE,NULL,1,'seed','seed',now(),now()),
  ('bi_plan4','sqBxGzP123','BUDG0901','SEC0901P','FS_QFU2t0Y',NULL,'Planning',        'fixed', 20,140,20*140, TRUE,NULL,2,'seed','seed',now(),now())
ON CONFLICT (id) DO UPDATE
SET hours=EXCLUDED.hours, rate=EXCLUDED.rate, amount=EXCLUDED.amount,
    "sectionId"=EXCLUDED."sectionId", "updatedBy"='seed', "updatedAt"=now();

-- Timesheets (week starting 2025-09-01)
INSERT INTO public.tbl_pulse_timesheet
  (id,"customerId","resourceId","weekKey",status,
   "submittedAt","submittedBy","approvedAt","approvedBy",
   "createdBy","updatedBy","createdAt","updatedAt")
VALUES
  ('TSBRU0901','sqBxGzP123','BRUCE00001','2025-09-01','draft',NULL,NULL,NULL,NULL,'seed','seed',now(),now()),
  ('TSCAR0901','sqBxGzP123','CAROL00001','2025-09-01','draft',NULL,NULL,NULL,NULL,'seed','seed',now(),now()),
  ('TSCLI0901','sqBxGzP123','CLINT00001','2025-09-01','draft',NULL,NULL,NULL,NULL,'seed','seed',now(),now()),
  ('TSWAN0901','sqBxGzP123','WANDA00001','2025-09-01','draft',NULL,NULL,NULL,NULL,'seed','seed',now(),now())
ON CONFLICT (id) DO NOTHING;

-- TAX team rows (Carol high non‑billable; Bruce mentor)
INSERT INTO public.tbl_pulse_timesheet_row
 (id,"customerId","timesheetId","date","engagementId","budgetItemId",
  hours,billable,rate,notes,"createdBy","updatedBy","createdAt","updatedAt")
VALUES
 ('trow40','sqBxGzP123','TSBRU0901','2025-09-01','FS_QFU2t0Y','bi_ct3', 3, TRUE,NULL,NULL,'seed','seed',now(),now()),
 ('trow41','sqBxGzP123','TSBRU0901','2025-09-02','FS_QFU2t0Y','bi_del3',4, TRUE,NULL,NULL,'seed','seed',now(),now()),
 ('trow42','sqBxGzP123','TSBRU0901','2025-09-03','FS_QFU2t0Y','bi_plan3',2, TRUE,NULL,NULL,'seed','seed',now(),now()),
 ('trow43','sqBxGzP123','TSCAR0901','2025-09-01','FS_QFU2t0Y','bi_ct3', 7, TRUE,NULL,NULL,'seed','seed',now(),now()),
 ('trow44','sqBxGzP123','TSCAR0901','2025-09-01','FS_QFU2t0Y','bi_plan3',3, FALSE,NULL,'training','seed','seed',now(),now()),
 ('trow45','sqBxGzP123','TSCAR0901','2025-09-02','FS_QFU2t0Y','bi_del3',8, TRUE,NULL,NULL,'seed','seed',now(),now()),
 ('trow46','sqBxGzP123','TSCAR0901','2025-09-03','FS_QFU2t0Y','bi_plan3',4, FALSE,NULL,'internal docs','seed','seed',now(),now())
ON CONFLICT (id) DO NOTHING;

-- RISK team rows (Clint heavy context‑switching; Wanda healthy)
INSERT INTO public.tbl_pulse_timesheet_row
 (id,"customerId","timesheetId","date","engagementId","budgetItemId",
  hours,billable,rate,notes,"createdBy","updatedBy","createdAt","updatedAt")
VALUES
 ('trow47','sqBxGzP123','TSCLI0901','2025-09-01','FS_QFU2t0Y','bi_ct4', 3, TRUE,NULL,NULL,'seed','seed',now(),now()),
 ('trow48','sqBxGzP123','TSCLI0901','2025-09-01','FS_QFU2t0Y','bi_del4',3, TRUE,NULL,NULL,'seed','seed',now(),now()),
 ('trow49','sqBxGzP123','TSCLI0901','2025-09-01','FS_QFU2t0Y','bi_plan4',2, TRUE,NULL,NULL,'seed','seed',now(),now()),
 ('trow50','sqBxGzP123','TSCLI0901','2025-09-02','FS_QFU2t0Y','bi_ct4', 2, TRUE,NULL,NULL,'seed','seed',now(),now()),
 ('trow51','sqBxGzP123','TSCLI0901','2025-09-02','FS_QFU2t0Y','bi_del4',3, TRUE,NULL,NULL,'seed','seed',now(),now()),
 ('trow52','sqBxGzP123','TSCLI0901','2025-09-02','FS_QFU2t0Y','bi_plan4',3, TRUE,NULL,NULL,'seed','seed',now(),now()),
 ('trow53','sqBxGzP123','TSCLI0901','2025-09-03','FS_QFU2t0Y','bi_del4',4, TRUE,NULL,NULL,'seed','seed',now(),now()),
 ('trow54','sqBxGzP123','TSWAN0901','2025-09-01','FS_QFU2t0Y','bi_ct4', 4, TRUE,NULL,NULL,'seed','seed',now(),now()),
 ('trow55','sqBxGzP123','TSWAN0901','2025-09-02','FS_QFU2t0Y','bi_del4',5, TRUE,NULL,NULL,'seed','seed',now(),now()),
 ('trow56','sqBxGzP123','TSWAN0901','2025-09-03','FS_QFU2t0Y','bi_plan4',3, TRUE,NULL,NULL,'seed','seed',now(),now())
ON CONFLICT (id) DO NOTHING;

COMMIT;
