ALTER TABLE tbl_ptrs ENABLE ROW LEVEL SECURITY;
ALTER TABLE tbl_tcp ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_ptrs_rls_policy ON tbl_ptrs;
CREATE POLICY tbl_ptrs_rls_policy
  ON tbl_ptrs
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);

DROP POLICY IF EXISTS tbl_tcp_rls_policy ON tbl_tcp;
CREATE POLICY tbl_tcp_rls_policy
  ON tbl_tcp
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);


ALTER TABLE tbl_ptrs FORCE ROW LEVEL SECURITY;
ALTER TABLE tbl_tcp FORCE ROW LEVEL SECURITY;

-- =============================
-- PTRS v2 core tables: RLS policies
-- =============================

-- PTRS Profiles
ALTER TABLE tbl_ptrs_profile ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_ptrs_profile_rls_policy ON tbl_ptrs_profile;
CREATE POLICY tbl_ptrs_profile_rls_policy
  ON tbl_ptrs_profile
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_ptrs_profile FORCE ROW LEVEL SECURITY;

-- PTRS Datasets (supporting files attached to a PTRS)
ALTER TABLE tbl_ptrs_dataset ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_ptrs_dataset_rls_policy ON tbl_ptrs_dataset;
CREATE POLICY tbl_ptrs_dataset_rls_policy
  ON tbl_ptrs_dataset
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_ptrs_dataset FORCE ROW LEVEL SECURITY;

-- PTRS Column Map (mappings + joins + rules)
ALTER TABLE tbl_ptrs_column_map ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_ptrs_column_map_rls_policy ON tbl_ptrs_column_map;
CREATE POLICY tbl_ptrs_column_map_rls_policy
  ON tbl_ptrs_column_map
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_ptrs_column_map FORCE ROW LEVEL SECURITY;

-- PTRS Rulesets (rule definitions per PTRS/profile)
ALTER TABLE tbl_ptrs_ruleset ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_ptrs_ruleset_rls_policy ON tbl_ptrs_ruleset;
CREATE POLICY tbl_ptrs_ruleset_rls_policy
  ON tbl_ptrs_ruleset
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_ptrs_ruleset FORCE ROW LEVEL SECURITY;

-- PTRS Uploads (PTRS header / upload metadata)
ALTER TABLE tbl_ptrs_upload ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_ptrs_upload_rls_policy ON tbl_ptrs_upload;
CREATE POLICY tbl_ptrs_upload_rls_policy
  ON tbl_ptrs_upload
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_ptrs_upload FORCE ROW LEVEL SECURITY;

-- PTRS Import (raw rows ingested from main CSV)
ALTER TABLE tbl_ptrs_import_raw ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_ptrs_import_raw_rls_policy ON tbl_ptrs_import_raw;
CREATE POLICY tbl_ptrs_import_raw_rls_policy
  ON tbl_ptrs_import_raw
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_ptrs_import_raw FORCE ROW LEVEL SECURITY;

-- PTRS Staging rows (post-join, pre-rules)
ALTER TABLE tbl_ptrs_stage_row ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_ptrs_stage_row_rls_policy ON tbl_ptrs_stage_row;
CREATE POLICY tbl_ptrs_stage_row_rls_policy
  ON tbl_ptrs_stage_row
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_ptrs_stage_row FORCE ROW LEVEL SECURITY;

-- TCP error rows (per-tenant)
ALTER TABLE tbl_tcp_error ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_tcp_error_rls_policy ON tbl_tcp_error;
CREATE POLICY tbl_tcp_error_rls_policy
  ON tbl_tcp_error
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_tcp_error FORCE ROW LEVEL SECURITY;

-- Big Bertha staging table (raw import rows) per-tenant
ALTER TABLE tbl_tcp_import_raw ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_tcp_import_raw_rls_policy ON tbl_tcp_import_raw;
CREATE POLICY tbl_tcp_import_raw_rls_policy
  ON tbl_tcp_import_raw
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_tcp_import_raw FORCE ROW LEVEL SECURITY;

ALTER TABLE tbl_esg_reporting_periods ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_esg_reporting_periods_rls_policy ON tbl_esg_reporting_periods;
CREATE POLICY tbl_esg_reporting_periods_rls_policy
  ON tbl_esg_reporting_periods
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);

ALTER TABLE tbl_esg_reporting_periods FORCE ROW LEVEL SECURITY;

ALTER TABLE tbl_esg_indicators ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_esg_indicators_rls_policy ON tbl_esg_indicators;
CREATE POLICY tbl_esg_indicators_rls_policy
  ON tbl_esg_indicators
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);

ALTER TABLE tbl_esg_indicators FORCE ROW LEVEL SECURITY;

ALTER TABLE tbl_esg_metrics ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_esg_metrics_rls_policy ON tbl_esg_metrics;
CREATE POLICY tbl_esg_metrics_rls_policy
  ON tbl_esg_metrics
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);

ALTER TABLE tbl_esg_metrics FORCE ROW LEVEL SECURITY;

ALTER TABLE tbl_esg_units ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_esg_units_rls_policy ON tbl_esg_units;
CREATE POLICY tbl_esg_units_rls_policy
  ON tbl_esg_units
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);

ALTER TABLE tbl_esg_units FORCE ROW LEVEL SECURITY;

ALTER TABLE tbl_ms_supplier_risks ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_ms_supplier_risks_rls_policy ON tbl_ms_supplier_risks;
CREATE POLICY tbl_ms_supplier_risks_rls_policy
  ON tbl_ms_supplier_risks
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);

ALTER TABLE tbl_ms_supplier_risks FORCE ROW LEVEL SECURITY;

ALTER TABLE tbl_ms_training ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_ms_training_rls_policy ON tbl_ms_training;
CREATE POLICY tbl_ms_training_rls_policy
  ON tbl_ms_training
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);

ALTER TABLE tbl_ms_training FORCE ROW LEVEL SECURITY;

ALTER TABLE tbl_ms_grievances ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_ms_grievances_rls_policy ON tbl_ms_grievances;
CREATE POLICY tbl_ms_grievances_rls_policy
  ON tbl_ms_grievances
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);

ALTER TABLE tbl_ms_grievances FORCE ROW LEVEL SECURITY;

ALTER TABLE tbl_ms_interview_responses ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_ms_interview_responses_rls_policy ON tbl_ms_interview_responses;
CREATE POLICY tbl_ms_interview_responses_rls_policy
  ON tbl_ms_interview_responses
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);

ALTER TABLE tbl_ms_interview_responses FORCE ROW LEVEL SECURITY;

ALTER TABLE tbl_invoice ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_invoice_rls_policy ON tbl_invoice;
CREATE POLICY tbl_invoice_rls_policy
  ON tbl_invoice
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);

ALTER TABLE tbl_invoice FORCE ROW LEVEL SECURITY;

ALTER TABLE tbl_invoice_line ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_invoice_line_rls_policy ON tbl_invoice_line;
CREATE POLICY tbl_invoice_line_rls_policy
  ON tbl_invoice_line
  FOR ALL
  USING (true)
  WITH CHECK (true);

ALTER TABLE tbl_invoice_line FORCE ROW LEVEL SECURITY;

ALTER TABLE tbl_product ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_product_rls_policy ON tbl_product;
CREATE POLICY tbl_product_rls_policy
  ON tbl_product
  FOR ALL
  USING (true)
  WITH CHECK (true);

ALTER TABLE tbl_product FORCE ROW LEVEL SECURITY;

-- =============================
-- Monochrome Pulse: RLS policies (tbl_pulse_*)
-- =============================

ALTER TABLE tbl_pulse_client ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_pulse_client_rls_policy ON tbl_pulse_client;
CREATE POLICY tbl_pulse_client_rls_policy
  ON tbl_pulse_client
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_pulse_client FORCE ROW LEVEL SECURITY;


-- Trackable (replaces Engagement)
ALTER TABLE tbl_pulse_trackable ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_pulse_trackable_rls_policy ON tbl_pulse_trackable;
CREATE POLICY tbl_pulse_trackable_rls_policy
  ON tbl_pulse_trackable
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_pulse_trackable FORCE ROW LEVEL SECURITY;

-- Resource
ALTER TABLE tbl_pulse_resource ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_pulse_resource_rls_policy ON tbl_pulse_resource;
CREATE POLICY tbl_pulse_resource_rls_policy
  ON tbl_pulse_resource
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_pulse_resource FORCE ROW LEVEL SECURITY;


-- Assignment (replaces Assignment)
ALTER TABLE tbl_pulse_assignments ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_pulse_assignments_rls_policy ON tbl_pulse_assignments;
CREATE POLICY tbl_pulse_assignments_rls_policy
  ON tbl_pulse_assignments
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_pulse_assignments FORCE ROW LEVEL SECURITY;

-- Budget (header)
ALTER TABLE tbl_pulse_budget ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_pulse_budget_rls_policy ON tbl_pulse_budget;
CREATE POLICY tbl_pulse_budget_rls_policy
  ON tbl_pulse_budget
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_pulse_budget FORCE ROW LEVEL SECURITY;

-- Budget Section (grouping)
ALTER TABLE tbl_pulse_budget_section ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_pulse_budget_section_rls_policy ON tbl_pulse_budget_section;
CREATE POLICY tbl_pulse_budget_section_rls_policy
  ON tbl_pulse_budget_section
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_pulse_budget_section FORCE ROW LEVEL SECURITY;

-- Budget Item (lines)
ALTER TABLE tbl_pulse_budget_item ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_pulse_budget_item_rls_policy ON tbl_pulse_budget_item;
CREATE POLICY tbl_pulse_budget_item_rls_policy
  ON tbl_pulse_budget_item
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_pulse_budget_item FORCE ROW LEVEL SECURITY;


-- Contribution (replaces Timesheet/Timesheet rows)
ALTER TABLE tbl_pulse_contribution ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_pulse_contribution_rls_policy ON tbl_pulse_contribution;
CREATE POLICY tbl_pulse_contribution_rls_policy
  ON tbl_pulse_contribution
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_pulse_contribution FORCE ROW LEVEL SECURITY;

-- =============================
-- Stripe / Billing: RLS policies
-- =============================
ALTER TABLE tbl_stripe_user ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_stripe_user_rls_policy ON tbl_stripe_user;
CREATE POLICY tbl_stripe_user_rls_policy
  ON tbl_stripe_user
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_stripe_user FORCE ROW LEVEL SECURITY;

-- Feature Entitlements (tenant-scoped)
ALTER TABLE tbl_feature_entitlements ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_feature_entitlements_rls_policy ON tbl_feature_entitlements;
CREATE POLICY tbl_feature_entitlements_rls_policy
  ON tbl_feature_entitlements
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);

ALTER TABLE tbl_feature_entitlements FORCE ROW LEVEL SECURITY;

-- =============================
-- Tenant resolution helper
-- =============================
-- Function to resolve customerId from email before RLS is set.
-- SECURITY DEFINER lets this run without an existing tenant context.
-- NOTE: Grant is PUBLIC here for dev; restrict to an app role in prod.
CREATE OR REPLACE FUNCTION fn_get_customer_id_by_email(p_email text)
RETURNS text
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT "customerId"
  FROM users
  WHERE lower(email) = lower(p_email)
  LIMIT 1;
$$;

-- Minimal hardening: avoid broad access in production.
REVOKE ALL ON FUNCTION fn_get_customer_id_by_email(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION fn_get_customer_id_by_email(text) TO PUBLIC; -- TODO: replace PUBLIC with app role in prod

-- Lookup performance for case-insensitive email matches
CREATE INDEX IF NOT EXISTS idx_users_email_lower ON users ((lower(email)));

-- =============================
-- Pulse Views: conditional GRANTS (to avoid "permission denied" on views)
-- These DO blocks only run GRANTs if the view exists.
-- In production, replace PUBLIC with a specific role (e.g., app_user).
-- =============================
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
             WHERE c.relkind='v' AND n.nspname='public' AND c.relname='v_overruns') THEN
    EXECUTE 'GRANT SELECT ON public.v_overruns TO PUBLIC';
  END IF;
END$$;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
             WHERE c.relkind='v' AND n.nspname='public' AND c.relname='v_weekly_burn') THEN
    EXECUTE 'GRANT SELECT ON public.v_weekly_burn TO PUBLIC';
  END IF;
END$$;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
             WHERE c.relkind='v' AND n.nspname='public' AND c.relname='v_resource_utilisation') THEN
    EXECUTE 'GRANT SELECT ON public.v_resource_utilisation TO PUBLIC';
  END IF;
END$$;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
             WHERE c.relkind='v' AND n.nspname='public' AND c.relname='v_pulse_team_map') THEN
    EXECUTE 'GRANT SELECT ON public.v_pulse_team_map TO PUBLIC';
  END IF;
END$$;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
             WHERE c.relkind='v' AND n.nspname='public' AND c.relname='v_pulse_team_hours') THEN
    EXECUTE 'GRANT SELECT ON public.v_pulse_team_hours TO PUBLIC';
  END IF;
END$$;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
             WHERE c.relkind='v' AND n.nspname='public' AND c.relname='v_pulse_estimation_by_category') THEN
    EXECUTE 'GRANT SELECT ON public.v_pulse_estimation_by_category TO PUBLIC';
  END IF;
END$$;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
             WHERE c.relkind='v' AND n.nspname='public' AND c.relname='v_pulse_context_switching') THEN
    EXECUTE 'GRANT SELECT ON public.v_pulse_context_switching TO PUBLIC';
  END IF;
END$$;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
             WHERE c.relkind='v' AND n.nspname='public' AND c.relname='v_pulse_long_day_streaks') THEN
    EXECUTE 'GRANT SELECT ON public.v_pulse_long_day_streaks TO PUBLIC';
  END IF;
END$$;

--- =============================
-- PTRS Reference Data: RLS policies
-- =============================
-- Customer-scoped references: enforce customerId RLS

-- Employees
ALTER TABLE tbl_ptrs_employee_ref ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_ptrs_employee_ref_rls_policy ON tbl_ptrs_employee_ref;
CREATE POLICY tbl_ptrs_employee_ref_rls_policy
  ON tbl_ptrs_employee_ref
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_ptrs_employee_ref FORCE ROW LEVEL SECURITY;

-- Intra-company
ALTER TABLE tbl_ptrs_intra_company_ref ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_ptrs_intra_company_ref_rls_policy ON tbl_ptrs_intra_company_ref;
CREATE POLICY tbl_ptrs_intra_company_ref_rls_policy
  ON tbl_ptrs_intra_company_ref
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_ptrs_intra_company_ref FORCE ROW LEVEL SECURITY;

-- Customer keywords
ALTER TABLE tbl_ptrs_exclusion_keyword_customer_ref ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_ptrs_exclusion_keyword_customer_ref_rls_policy ON tbl_ptrs_exclusion_keyword_customer_ref;
CREATE POLICY tbl_ptrs_exclusion_keyword_customer_ref_rls_policy
  ON tbl_ptrs_exclusion_keyword_customer_ref
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_ptrs_exclusion_keyword_customer_ref FORCE ROW LEVEL SECURITY;

-- Global/common reference (no RLS): Government Entities
-- Intentionally no RLS here because this table is shared across tenants.
-- If you created it as tbl_ptrs_gov_entity_ref, do NOT enable RLS.
-- (If RLS was previously enabled, consider: ALTER TABLE tbl_gov_entity_ref DISABLE ROW LEVEL SECURITY;)