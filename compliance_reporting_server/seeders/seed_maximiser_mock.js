-- MAXIMISER VIEW CONTRACTS (ALIGNED TO CURRENT SCHEMA)
-- Assumptions from provided tables:
--   tbl_pulse_timesheet_row(date, hours, billable, engagementId, budgetItemId, timesheetId, customerId)
--   tbl_pulse_timesheet(id, resourceId, weekKey, customerId)
--   tbl_pulse_resource(id, name, customerId)
--   tbl_pulse_budget_item(id, engagementId, sectionName, hours, billable, customerId)
--   (No explicit team table yet) → we derive "team" = resource for now.

-- Helper mapping: each resource is considered its own team until a team model exists
CREATE OR REPLACE VIEW v_pulse_team_map AS
SELECT r.customerId, r.id AS teamId, r.name AS teamName
FROM tbl_pulse_resource r;

-- 1) Team hours per week (billable / non-billable / total). After-hours not computable without timestamps → 0 for now.
CREATE OR REPLACE VIEW v_pulse_team_hours AS
SELECT
  r.customerId,
  r.id AS teamId,
  date_trunc('week', tr.date)::date AS week_start,
  SUM(CASE WHEN tr.billable THEN tr.hours ELSE 0 END) AS billable_hours,
  SUM(CASE WHEN NOT tr.billable THEN tr.hours ELSE 0 END) AS non_billable_hours,
  SUM(tr.hours) AS total_hours,
  0::numeric(12,2) AS after_hours -- placeholder until we store per-entry timestamps
FROM tbl_pulse_timesheet_row tr
JOIN tbl_pulse_timesheet ts
  ON ts.id = tr.timesheetId AND ts.customerId = tr.customerId
JOIN tbl_pulse_resource r
  ON r.id = ts.resourceId AND r.customerId = ts.customerId
GROUP BY r.customerId, r.id, date_trunc('week', tr.date)::date;

-- 2) Estimation vs actual by category per team
-- We join budget items by engagementId (since budget_item lacks resourceId),
-- and treat sectionName as the category label; planned uses budget_item.hours.
CREATE OR REPLACE VIEW v_pulse_estimation_by_category AS
SELECT
  tr.customerId,
  r.id AS teamId,
  COALESCE(bi.sectionName, 'Uncategorised') AS category,
  SUM(COALESCE(bi.hours, 0)) AS planned,
  SUM(COALESCE(tr.hours, 0)) AS actual
FROM tbl_pulse_timesheet_row tr
JOIN tbl_pulse_timesheet ts
  ON ts.id = tr.timesheetId AND ts.customerId = tr.customerId
JOIN tbl_pulse_resource r
  ON r.id = ts.resourceId AND r.customerId = ts.customerId
LEFT JOIN tbl_pulse_budget_item bi
  ON bi.customerId = tr.customerId AND bi.engagementId = tr.engagementId
GROUP BY tr.customerId, r.id, COALESCE(bi.sectionName, 'Uncategorised');

-- 3) Context switching rate per team (avg distinct work items per resource-day)
CREATE OR REPLACE VIEW v_pulse_context_switching AS
WITH per_day AS (
  SELECT
    tr.customerId,
    ts.resourceId,
    tr.date AS d,
    COUNT(DISTINCT COALESCE(tr.engagementId::text, tr.budgetItemId::text)) AS distinct_work_items
  FROM tbl_pulse_timesheet_row tr
  JOIN tbl_pulse_timesheet ts
    ON ts.id = tr.timesheetId AND ts.customerId = tr.customerId
  GROUP BY tr.customerId, ts.resourceId, tr.date
)
SELECT
  r.customerId,
  r.id AS teamId,
  AVG(per_day.distinct_work_items)::numeric(10,2) AS context_switch_rate
FROM per_day
JOIN tbl_pulse_resource r
  ON r.id = per_day.resourceId AND r.customerId = per_day.customerId
GROUP BY r.customerId, r.id;

-- 4) Long-day streaks (>=10h/day) per team
CREATE OR REPLACE VIEW v_pulse_long_day_streaks AS
WITH per_day AS (
  SELECT tr.customerId, r.id AS teamId, tr.date AS d, SUM(tr.hours) AS hours
  FROM tbl_pulse_timesheet_row tr
  JOIN tbl_pulse_timesheet ts ON ts.id = tr.timesheetId AND ts.customerId = tr.customerId
  JOIN tbl_pulse_resource r ON r.id = ts.resourceId AND r.customerId = ts.customerId
  GROUP BY tr.customerId, r.id, tr.date
),
flags AS (
  SELECT *, (hours >= 10)::int AS is_heavy FROM per_day
),
seq AS (
  SELECT
    customerId,
    teamId,
    d,
    is_heavy,
    CASE WHEN is_heavy = 1 AND lag(is_heavy) OVER (PARTITION BY customerId, teamId ORDER BY d) = 1 THEN 0 ELSE 1 END AS grp_break
  FROM flags
),
streak_groups AS (
  SELECT
    customerId,
    teamId,
    SUM(grp_break) OVER (PARTITION BY customerId, teamId ORDER BY d) AS grp,
    d,
    is_heavy
  FROM seq
)
SELECT customerId, teamId, COUNT(*) FILTER (WHERE is_heavy = 1) AS streaks_over_10h
FROM streak_groups
GROUP BY customerId, teamId;
