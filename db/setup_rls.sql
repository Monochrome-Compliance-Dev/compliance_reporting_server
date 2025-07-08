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