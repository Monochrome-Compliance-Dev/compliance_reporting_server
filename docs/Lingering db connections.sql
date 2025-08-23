-- View active transactions, their age, current query, and wait events
SELECT
  pid,
  usename AS user,
  customer_addr,
  state,
  backend_xid,
  backend_xmin,
  query_start,
  xact_start,
  state_change,
  wait_event_type,
  wait_event,
  query
FROM pg_stat_activity
WHERE
  state IN ('active', 'idle in transaction')
  AND pid <> pg_backend_pid()
ORDER BY xact_start DESC NULLS LAST;