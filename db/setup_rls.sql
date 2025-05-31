ALTER TABLE tbl_tcp_audit ENABLE ROW LEVEL SECURITY;
ALTER TABLE tbl_report ENABLE ROW LEVEL SECURITY;
ALTER TABLE tbl_tcp ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tbl_tcp_audit_rls_policy ON tbl_tcp_audit;
CREATE POLICY tbl_tcp_audit_rls_policy ON tbl_tcp_audit
  USING ("clientId" = current_setting('app.current_client_id', true)::text);

DROP POLICY IF EXISTS tbl_report_rls_policy ON tbl_report;
CREATE POLICY tbl_report_rls_policy ON tbl_report
  USING ("clientId" = current_setting('app.current_client_id', true)::text);

DROP POLICY IF EXISTS tbl_tcp_rls_policy ON tbl_tcp;
CREATE POLICY tbl_tcp_rls_policy ON tbl_tcp
  USING ("clientId" = current_setting('app.current_client_id', true)::text);

ALTER TABLE tbl_tcp_audit FORCE ROW LEVEL SECURITY;
ALTER TABLE tbl_report FORCE ROW LEVEL SECURITY;
ALTER TABLE tbl_tcp FORCE ROW LEVEL SECURITY;