ALTER TABLE tbl_tcp_audit ENABLE ROW LEVEL SECURITY;
ALTER TABLE tbl_ptrs ENABLE ROW LEVEL SECURITY;
ALTER TABLE tbl_tcp ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_tcp_audit_rls_policy ON tbl_tcp_audit;
CREATE POLICY tbl_tcp_audit_rls_policy
  ON tbl_tcp_audit
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);

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

ALTER TABLE tbl_tcp_audit FORCE ROW LEVEL SECURITY;
ALTER TABLE tbl_ptrs FORCE ROW LEVEL SECURITY;
ALTER TABLE tbl_tcp FORCE ROW LEVEL SECURITY;

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

-- Client
ALTER TABLE tbl_pulse_client ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_pulse_client_rls_policy ON tbl_pulse_client;
CREATE POLICY tbl_pulse_client_rls_policy
  ON tbl_pulse_client
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_pulse_client FORCE ROW LEVEL SECURITY;

-- Engagement
ALTER TABLE tbl_pulse_engagement ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_pulse_engagement_rls_policy ON tbl_pulse_engagement;
CREATE POLICY tbl_pulse_engagement_rls_policy
  ON tbl_pulse_engagement
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_pulse_engagement FORCE ROW LEVEL SECURITY;

-- Resource
ALTER TABLE tbl_pulse_resource ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_pulse_resource_rls_policy ON tbl_pulse_resource;
CREATE POLICY tbl_pulse_resource_rls_policy
  ON tbl_pulse_resource
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_pulse_resource FORCE ROW LEVEL SECURITY;

-- Assignment
ALTER TABLE tbl_pulse_assignment ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_pulse_assignment_rls_policy ON tbl_pulse_assignment;
CREATE POLICY tbl_pulse_assignment_rls_policy
  ON tbl_pulse_assignment
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_pulse_assignment FORCE ROW LEVEL SECURITY;

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

-- Timesheet (header)
ALTER TABLE tbl_pulse_timesheet ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_pulse_timesheet_rls_policy ON tbl_pulse_timesheet;
CREATE POLICY tbl_pulse_timesheet_rls_policy
  ON tbl_pulse_timesheet
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_pulse_timesheet FORCE ROW LEVEL SECURITY;

-- Timesheet rows (lines)
ALTER TABLE tbl_pulse_timesheet_row ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tbl_pulse_timesheet_row_rls_policy ON tbl_pulse_timesheet_row;
CREATE POLICY tbl_pulse_timesheet_row_rls_policy
  ON tbl_pulse_timesheet_row
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE tbl_pulse_timesheet_row FORCE ROW LEVEL SECURITY;

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