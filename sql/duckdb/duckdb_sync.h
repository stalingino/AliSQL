/*****************************************************************************

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/**
  Initial data load for DuckDB replicas (dbms_duckdb.sync_table).

  A sync job copies tables from the replication source into local DuckDB
  tables while the replica applier keeps running, then hands them over to
  the applier at an exact GTID boundary:

    1. Short applier pause. On the source: LOCK TABLES <tables> READ, start
       one consistent snapshot per copy worker, read @@GLOBAL.gtid_executed
       (= G) and the table definitions, UNLOCK TABLES. While the read lock is
       held no transaction touching the tables can commit, so the snapshot of
       each table contains exactly the transactions in G. Create the local
       tables and a delta table per table, publish G, resume the applier.
    2. Stream the snapshots into the local tables. Meanwhile the applier
       skips changes to these tables whose GTID is in G (they are in the
       snapshot) and appends the others to the delta tables instead of the
       tables.
    3. Short applier pause: merge each delta table into its table (last row
       image per primary key wins) and mark the tables CATCHING_UP. From then
       on the applier skips GTIDs in G and applies everything else directly.

  The registry of synced tables (mysql.duckdb_sync_tables inside DuckDB)
  overrides the replicate-* filters for the tables it contains.
*/

#pragma once

#include <string>
#include <vector>

#include "my_inttypes.h"

class THD;
class Table_ref;

namespace myduck {

enum class Sync_decision { NO_OPINION, INCLUDE, EXCLUDE };

/**
  Decide whether the replica applier should apply a row event changing
  db.table in the transaction currently owned by thd.

  @retval NO_OPINION  table is not registered; the replicate-* filters decide
  @retval INCLUDE     apply, regardless of the replicate-* filters (for a
                      table being loaded the handler buffers the change, see
                      sync_is_buffered())
  @retval EXCLUDE     skip
*/
Sync_decision sync_filter_decision(THD *thd, const char *db,
                                   const char *table);

/**
  Same for a statement (DDL) on the given tables; EXCLUDE wins over INCLUDE.
  DDL on a table being loaded aborts its load.
*/
Sync_decision sync_filter_decision(THD *thd, Table_ref *tables);

/**
  Whether the replica applier must append changes to db.table to the table's
  delta table instead of applying them, because the table is being loaded.
*/
bool sync_is_buffered(THD *thd, const char *db, const char *table);

/** Name of the delta table of db.table, quoted for DuckDB. */
std::string sync_delta_table(const std::string &db, const std::string &table);

/**
  Reserve n consecutive sequence numbers for rows appended to the delta table
  of db.table; returns the first one.
*/
ulonglong sync_reserve_delta_seq(const std::string &db,
                                 const std::string &table, ulonglong n);

/**
  Check that no sync job has paused the applier of the channel. A job stops
  the applier briefly to take the snapshot and to merge the deltas, and must
  be the one that restarts it.

  @retval false  the applier may be started by thd
  @retval true   a job holds it stopped; an error is set
*/
bool sync_check_applier_start(THD *thd, const char *channel);

/** Load the persisted registry. Called at startup before replication starts. */
void sync_registry_init();

/** Abort a running sync job and wait for it. Called at shutdown. */
void sync_shutdown();

struct Sync_status_row {
  std::string schema_name;
  std::string table_name;
  std::string state;
  ulonglong rows_loaded;
  ulonglong rows_expected;
  ulonglong job_id;
  std::string started_at;
  std::string finished_at;
  std::string snapshot_gtid_set;
  std::string last_error;
};

/**
  dbms_duckdb.sync_table(schema, tables, options).

  Starts a background sync job, or with adopt=1 registers already loaded
  tables directly. Reports errors with my_error().

  @param[out] job_id   id of the started job, 0 for adopt
  @param[out] message  human readable outcome
*/
bool sync_table(THD *thd, const std::string &schema, const std::string &tables,
                const std::string &options, ulonglong *job_id,
                std::string *message);

/** dbms_duckdb.sync_status() */
void sync_status(std::vector<Sync_status_row> *rows);

/** dbms_duckdb.unsync_table(schema, table) */
bool unsync_table(THD *thd, const std::string &schema,
                  const std::string &table);

}  // namespace myduck
