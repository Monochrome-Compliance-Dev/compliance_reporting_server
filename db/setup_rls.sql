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
-- Monochrome Pulse: RLS policies
-- =============================

-- Client
ALTER TABLE tbl_client ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_client_rls_policy ON tbl_client;
CREATE POLICY tbl_client_rls_policy
  ON tbl_client
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);

ALTER TABLE tbl_client FORCE ROW LEVEL SECURITY;

-- Engagement
ALTER TABLE tbl_engagement ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_engagement_rls_policy ON tbl_engagement;
CREATE POLICY tbl_engagement_rls_policy
  ON tbl_engagement
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);

ALTER TABLE tbl_engagement FORCE ROW LEVEL SECURITY;

-- Resource
ALTER TABLE tbl_resource ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_resource_rls_policy ON tbl_resource;
CREATE POLICY tbl_resource_rls_policy
  ON tbl_resource
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);

ALTER TABLE tbl_resource FORCE ROW LEVEL SECURITY;

-- Assignment
ALTER TABLE tbl_assignment ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_assignment_rls_policy ON tbl_assignment;
CREATE POLICY tbl_assignment_rls_policy
  ON tbl_assignment
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);

ALTER TABLE tbl_assignment FORCE ROW LEVEL SECURITY;

-- Budget Items
ALTER TABLE tbl_budget_item ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_budget_item_rls_policy ON tbl_budget_item;
CREATE POLICY tbl_budget_item_rls_policy
  ON tbl_budget_item
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);

ALTER TABLE tbl_budget_item FORCE ROW LEVEL SECURITY;

-- Timesheet (header)
ALTER TABLE tbl_timesheet ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_timesheet_rls_policy ON tbl_timesheet;
CREATE POLICY tbl_timesheet_rls_policy
  ON tbl_timesheet
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);

ALTER TABLE tbl_timesheet FORCE ROW LEVEL SECURITY;

-- Timesheet rows (lines)
ALTER TABLE tbl_timesheet_row ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_timesheet_row_rls_policy ON tbl_timesheet_row;
CREATE POLICY tbl_timesheet_row_rls_policy
  ON tbl_timesheet_row
  FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);

ALTER TABLE tbl_timesheet_row FORCE ROW LEVEL SECURITY;