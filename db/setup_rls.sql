ALTER TABLE tbl_tcp_audit ENABLE ROW LEVEL SECURITY;
ALTER TABLE tbl_report ENABLE ROW LEVEL SECURITY;
ALTER TABLE tbl_tcp ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_tcp_audit_rls_policy ON tbl_tcp_audit;
CREATE POLICY tbl_tcp_audit_rls_policy
  ON tbl_tcp_audit
  FOR ALL
  USING ("clientId" = current_setting('app.current_client_id', true)::text)
  WITH CHECK ("clientId" = current_setting('app.current_client_id', true)::text);

DROP POLICY IF EXISTS tbl_report_rls_policy ON tbl_report;
CREATE POLICY tbl_report_rls_policy
  ON tbl_report
  FOR ALL
  USING ("clientId" = current_setting('app.current_client_id', true)::text)
  WITH CHECK ("clientId" = current_setting('app.current_client_id', true)::text);

DROP POLICY IF EXISTS tbl_tcp_rls_policy ON tbl_tcp;
CREATE POLICY tbl_tcp_rls_policy
  ON tbl_tcp
  FOR ALL
  USING ("clientId" = current_setting('app.current_client_id', true)::text)
  WITH CHECK ("clientId" = current_setting('app.current_client_id', true)::text);

ALTER TABLE tbl_tcp_audit FORCE ROW LEVEL SECURITY;
ALTER TABLE tbl_report FORCE ROW LEVEL SECURITY;
ALTER TABLE tbl_tcp FORCE ROW LEVEL SECURITY;

ALTER TABLE tbl_esg_reporting_periods ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_esg_reporting_periods_rls_policy ON tbl_esg_reporting_periods;
CREATE POLICY tbl_esg_reporting_periods_rls_policy
  ON tbl_esg_reporting_periods
  FOR ALL
  USING ("clientId" = current_setting('app.current_client_id', true)::text)
  WITH CHECK ("clientId" = current_setting('app.current_client_id', true)::text);

ALTER TABLE tbl_esg_reporting_periods FORCE ROW LEVEL SECURITY;

ALTER TABLE tbl_esg_indicators ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_esg_indicators_rls_policy ON tbl_esg_indicators;
CREATE POLICY tbl_esg_indicators_rls_policy
  ON tbl_esg_indicators
  FOR ALL
  USING ("clientId" = current_setting('app.current_client_id', true)::text)
  WITH CHECK ("clientId" = current_setting('app.current_client_id', true)::text);

ALTER TABLE tbl_esg_indicators FORCE ROW LEVEL SECURITY;

ALTER TABLE tbl_esg_metrics ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_esg_metrics_rls_policy ON tbl_esg_metrics;
CREATE POLICY tbl_esg_metrics_rls_policy
  ON tbl_esg_metrics
  FOR ALL
  USING ("clientId" = current_setting('app.current_client_id', true)::text)
  WITH CHECK ("clientId" = current_setting('app.current_client_id', true)::text);

ALTER TABLE tbl_esg_metrics FORCE ROW LEVEL SECURITY;

ALTER TABLE tbl_esg_units ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_esg_units_rls_policy ON tbl_esg_units;
CREATE POLICY tbl_esg_units_rls_policy
  ON tbl_esg_units
  FOR ALL
  USING ("clientId" = current_setting('app.current_client_id', true)::text)
  WITH CHECK ("clientId" = current_setting('app.current_client_id', true)::text);

ALTER TABLE tbl_esg_units FORCE ROW LEVEL SECURITY;

ALTER TABLE tbl_ms_supplier_risks ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_ms_supplier_risks_rls_policy ON tbl_ms_supplier_risks;
CREATE POLICY tbl_ms_supplier_risks_rls_policy
  ON tbl_ms_supplier_risks
  FOR ALL
  USING ("clientId" = current_setting('app.current_client_id', true)::text)
  WITH CHECK ("clientId" = current_setting('app.current_client_id', true)::text);

ALTER TABLE tbl_ms_supplier_risks FORCE ROW LEVEL SECURITY;

ALTER TABLE tbl_ms_training ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_ms_training_rls_policy ON tbl_ms_training;
CREATE POLICY tbl_ms_training_rls_policy
  ON tbl_ms_training
  FOR ALL
  USING ("clientId" = current_setting('app.current_client_id', true)::text)
  WITH CHECK ("clientId" = current_setting('app.current_client_id', true)::text);

ALTER TABLE tbl_ms_training FORCE ROW LEVEL SECURITY;

ALTER TABLE tbl_ms_grievances ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_ms_grievances_rls_policy ON tbl_ms_grievances;
CREATE POLICY tbl_ms_grievances_rls_policy
  ON tbl_ms_grievances
  FOR ALL
  USING ("clientId" = current_setting('app.current_client_id', true)::text)
  WITH CHECK ("clientId" = current_setting('app.current_client_id', true)::text);

ALTER TABLE tbl_ms_grievances FORCE ROW LEVEL SECURITY;

ALTER TABLE tbl_ms_interview_responses ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_ms_interview_responses_rls_policy ON tbl_ms_interview_responses;
CREATE POLICY tbl_ms_interview_responses_rls_policy
  ON tbl_ms_interview_responses
  FOR ALL
  USING ("clientId" = current_setting('app.current_client_id', true)::text)
  WITH CHECK ("clientId" = current_setting('app.current_client_id', true)::text);

ALTER TABLE tbl_ms_interview_responses FORCE ROW LEVEL SECURITY;