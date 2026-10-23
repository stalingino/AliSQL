# ==== Purpose ====
#
# Wait until row $sync_row of CALL dbms_duckdb.sync_status() has left the
# LOADING state, i.e. the sync job finished loading that table.
#
# ==== Usage ====
#
# --let $sync_row = 1
# --source suite/duckdb_rpl/include/wait_sync_table.inc
#
# Sets $sync_state to the state that was reached.

--let $_sync_wait = 600
--let $sync_state = LOADING
while ($_sync_wait)
{
  --let $sync_state = query_get_value(CALL dbms_duckdb.sync_status(), STATE, $sync_row)
  if ($sync_state != LOADING)
  {
    --let $_sync_wait = 0
  }
  if ($sync_state == LOADING)
  {
    --sleep 0.1
    --dec $_sync_wait
  }
}
if ($sync_state == LOADING)
{
  --echo Timeout waiting for the sync job.
  CALL dbms_duckdb.sync_status();
  --die Timeout in wait_sync_table.inc
}
