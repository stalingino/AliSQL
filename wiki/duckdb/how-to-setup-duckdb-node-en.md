# How to Set Up a DuckDB Node in AliSQL

[ [快速部署 AliSQL DuckDB 节点](./how-to-setup-duckdb-node-zh.md) | [How to Set Up a DuckDB Node in AliSQL](./how-to-setup-duckdb-node-en.md) ]

## Overview

AliSQL integrates DuckDB as an analytical storage engine.

- **User data** is stored in **DuckDB**
- **System tables and metadata** remain in **InnoDB** (e.g., `mysql.*`, data dictionary)

You can:

1. Bootstrap a **brand-new instance** that redirects user InnoDB table DDL to DuckDB.
2. **Convert an existing InnoDB instance** to DuckDB at startup.
3. Build a **DuckDB replica (HTAP)** to replay binlogs from an OLTP primary.

---

## 1. Bootstrap a Brand-New DuckDB Instance

Use this when you are creating a fresh instance and want user InnoDB table DDL to be redirected to DuckDB automatically.

### 1.1 Install AliSQL 8.0.44

#### Option A: Build from source
```bash
git clone https://github.com/alibaba/AliSQL.git
cd AliSQL

sh build.sh -t release -d /opt/alisql
make install
```

#### Option B: Install from RPM
```bash
# For example, use the x86_64 EL8 RPM
wget https://github.com/alibaba/AliSQL/releases/download/AliSQL-8.0.44-1/alisql-8.0.44-1.el8.x86_64.rpm
rpm -ivh alisql-8.0.44-1.el8.x86_64.rpm
```

---

### 1.2 Directory bootstrap + config generator script

```bash
cat > init_alisql_dir.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./init_alisql_dir.sh <absolute_path>

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <absolute_path>"
  exit 1
fi

DIR="$1"
if [[ "$DIR" != /* ]]; then
  echo "Error: Argument must be an ABSOLUTE PATH (starting with /)."
  exit 1
fi

DIR="${DIR%/}"

mkdir -p \
  "$DIR/data/dbs" \
  "$DIR/data/mysql" \
  "$DIR/run" \
  "$DIR/log/mysql" \
  "$DIR/tmp"

cat > "$DIR/alisql.cnf" <<EOF2
[mysqld]
# --- Paths ---
datadir = ${DIR}/data/dbs
socket  = ${DIR}/run/mysql.sock
pid-file = ${DIR}/run/mysqld.pid
tmpdir  = ${DIR}/tmp

# --- Basic ---
character_set_server = utf8mb4
skip_ssl
lower_case_table_names = 1
core-file

# --- Logs ---
log-error = ${DIR}/log/mysql/error.log

# Binary logging (optional)
log-bin = ${DIR}/log/mysql/mysql-bin
binlog_format = ROW

# --- InnoDB (system tables / metadata) ---
innodb_data_home_dir = ${DIR}/data/mysql
innodb_log_group_home_dir = ${DIR}/data/mysql

# --- DuckDB Storage Engine ---
duckdb_mode=ON
force_innodb_to_duckdb=ON

# DuckDB resources (0 = auto)
duckdb_memory_limit=2147483648
duckdb_threads=0
duckdb_temp_directory=${DIR}/tmp
EOF2

echo "OK: created $DIR and $DIR/alisql.cnf"
EOF

chmod +x init_alisql_dir.sh
```

References:

- MySQL 8.0 manual: https://dev.mysql.com/doc/refman/8.0/en/
- DuckDB variables: [AliSQL DuckDB variables](./duckdb_variables-en.md)

---

### 1.3 Initialize and start

```bash
# Example: store all data under $HOME/alisql_8044
./init_alisql_dir.sh $HOME/alisql_8044

# Initialize
/opt/alisql/bin/mysqld --defaults-file=$HOME/alisql_8044/alisql.cnf --initialize-insecure

# Start, the port is 3306 by default
/opt/alisql/bin/mysqld_safe --defaults-file=$HOME/alisql_8044/alisql.cnf &
```

---

### 1.4 Validate and use

```sql
CREATE DATABASE test;
USE test;

-- Create DuckDB table
CREATE TABLE t (
  id INT PRIMARY KEY,
  name VARCHAR(255)
) ENGINE = DuckDB;

-- Temporarily disable forced redirection for the DuckDB-to-InnoDB conversion.
SET GLOBAL force_innodb_to_duckdb = OFF;
ALTER TABLE t ENGINE = InnoDB;
ALTER TABLE t ENGINE = DuckDB;
SET GLOBAL force_innodb_to_duckdb = ON;

-- DML
INSERT INTO t VALUES (1, 'John');
SELECT * FROM t;
UPDATE t SET name = 'Jane' WHERE id = 1;
DELETE FROM t WHERE id = 1;

-- DDL
ALTER TABLE t ADD COLUMN age INT;
ALTER TABLE t RENAME TO t_new;
```

---

## 2. Startup Conversion from an Existing InnoDB Instance

Use this to migrate existing InnoDB tables to DuckDB for analytical queries.

### Steps

1. **Stop the existing instance** (clean shutdown)

2. **Update `my.cnf`**
```ini
[mysqld]
duckdb_mode=ON
force_innodb_to_duckdb=ON

duckdb_memory_limit=2147483648
duckdb_threads=0
duckdb_temp_directory=/path/to/duckdb_temp_dir

duckdb_convert_all_at_startup=ON
duckdb_convert_all_at_startup_threads=32
duckdb_convert_all_at_startup_ignore_error=OFF
```

3. **Start the instance**

4. **Verify**
```sql
-- Check the conversion stage
SHOW GLOBAL STATUS LIKE 'DuckDB_convert_stage_at_startup';

-- Check the converted tables
SELECT table_schema, table_name, engine
FROM information_schema.tables
WHERE engine = 'DuckDB';
```

### Notes

- Startup conversion is resource-intensive (CPU/IO). Monitor the error log for progress and failures.
- Keep `duckdb_convert_all_at_startup_ignore_error=OFF` for the initial migration so a failed table stops the conversion. If you intentionally enable it, the instance may start with only part of the user tables converted; verify every table before use.
- Ensure enough free disk space (recommended: ≥ original data size).
- Backup before conversion (backup set + cnf).
- Upgrade MySQL 5.7 or below to 8.0 first.
- If your source instance is not 8.0.44, an upgrade may be triggered on startup; ensure a clean shutdown beforehand.

---

## 3. DuckDB Replica (HTAP)

A common pattern is OLTP on the primary and OLAP on a DuckDB replica, fed by row-based binlogs.

### Steps

1. **Prepare the primary**

- enable binlog
- `binlog_format=ROW`
- set a nonzero, unique `server_id` (for example, `server_id=1`)
- enable GTID with `gtid_mode=ON` and `enforce_gtid_consistency=ON`

2. **Configure the replica `my.cnf`**

```ini
[mysqld]
server_id=2
gtid_mode=ON
enforce_gtid_consistency=ON
duckdb_mode=ON
duckdb_require_primary_key=ON
force_innodb_to_duckdb=ON

# Multi-Trx Batch is incompatible with parallel replication workers.
replica_parallel_workers=0

duckdb_dml_in_batch=ON
duckdb_update_modified_column_only=ON
duckdb_multi_trx_in_batch=ON
duckdb_multi_trx_timeout=5000
duckdb_multi_trx_max_batch_length=268435456
```

The replica `server_id` must differ from the primary and every other server in the replication topology.

This example uses DuckDB Multi-Trx Batch. BRR early apply requires parallel replication workers and must not be enabled by simply adding BRR workers to this configuration. See the [BRR guide](../binlog-realtime-replication/binlog-realtime-replication-en.md) for transmission-only and early-apply requirements. For AUTO_INCREMENT and background checkpoints added in the current DuckDB snapshot, see the [DuckDB guide](./duckdb-en.md#auto_increment-and-background-checkpoints).

3. **Provision existing data and GTID state**

For a nonempty primary, provision a consistent physical or logical backup before starting replication. The restored data and `gtid_executed`/`gtid_purged` state must describe the same source snapshot. For a physical backup, restore its InnoDB data and use the startup conversion in section 2. For a logical backup, import the dump into the DuckDB node and restore the GTID metadata recorded by the backup tool. Verify row counts and GTID state before continuing.

Starting from the empty instance in section 1 is valid only when the source is empty or still retains every required binlog transaction. Otherwise Auto Position stops with `ER_SOURCE_HAS_PURGED_REQUIRED_GTIDS`; provision a new replica from backup instead.

4. **Set up replication**
```sql
CHANGE MASTER TO
  MASTER_HOST='your-master-ip',
  MASTER_USER='repl',
  MASTER_PASSWORD='your-password',
  MASTER_AUTO_POSITION=1;

START REPLICA;
```

Confirm that `SHOW REPLICA STATUS\G` reports no I/O or SQL error before routing queries to the node.

5. **Done**
Route analytics/reporting queries to the DuckDB replica.

### Adding tables to a running replica

A replica that only replicates some tables (`replicate-do-table` / `replicate-wild-do-table`) can take on more tables without a backup, a `my.cnf` edit or a restart:

```sql
CALL dbms_duckdb.sync_table('financialForms', 'customer, loan_account', 'threads=4');
CALL dbms_duckdb.sync_status();
```

`sync_table` starts a background job and returns its id. The job:

1. Connects to the source with the channel's credentials, then briefly stops the replica applier (`STOP REPLICA SQL_THREAD`); the receiver keeps fetching binlogs. While it is stopped, the job:
   - runs `LOCK TABLES ... READ` on the listed tables;
   - starts one consistent snapshot per copy thread, reads `@@GLOBAL.gtid_executed` (the snapshot's GTID set) and the table definitions, then unlocks;
   - creates the tables as DuckDB tables from the source definitions;
   - restarts the applier.

   The source lock is held only for these few statements, but it waits for running transactions on those tables.
2. Streams the snapshot into the tables through the DuckDB appender. Tables with a single-column integer primary key are split across the copy threads.

   Replication keeps running during the copy. Changes to a loading table that are already in the snapshot are skipped. All other changes are written to a per-table delta table instead of the table.
3. Briefly stops the applier again and merges each delta into its table, keeping the last row image per primary key. Then marks the tables `CATCHING_UP` and restarts the applier.

After that, the applier skips changes to a synced table whose GTID is in its snapshot GTID set, because they are already in the copied data. It applies all other changes. The table becomes `ACTIVE` once the replica has executed the whole set.

Requirements:

- `gtid_mode=ON` on both servers; `binlog_format=ROW` and `binlog_row_image=FULL` on the source.
- Every synced table needs a primary key.
- The replication user needs `SELECT` and `LOCK TABLES` on the synced schemas.

Synced tables are recorded inside DuckDB (`mysql.duckdb_sync_tables`) and take precedence over the `replicate-*` filters: `ACTIVE`/`CATCHING_UP` tables are always replicated, `FAILED` tables never are.

Options (`key=value`, separated by `;` or `,`):

| Option | Default | Meaning |
|---|---|---|
| `threads` | 4 | copy threads / source snapshot connections |
| `refresh` | 0 | drop and reload a table that is already synced or not empty |
| `adopt` | 0 | register tables that are already loaded and replicated (for example, through `my.cnf` filters) as `ACTIVE` without copying |
| `verify` | 1 | compare the copied row count with `COUNT(*)` in the snapshot |
| `commit_rows` | 500000 | rows per local commit |
| `lock_wait_timeout` | 60 | seconds to wait for `LOCK TABLES` on the source |
| `net_timeout` | 3600 | network timeouts of the source connections |
| `channel` | `''` | replication channel to load from |

`CALL dbms_duckdb.unsync_table('db', 'table')` removes a table from the registry, so the `replicate-*` filters decide again. The local table is kept.

Notes:

- Until a table is `CATCHING_UP` or `ACTIVE`, queries on the replica see a partially loaded table.
- Long snapshots keep InnoDB undo history on the source, as `mysqldump --single-transaction` does. Prefer loading large tables from another replica of the source.
- DDL on a table while it is loading fails that table's load (`FAILED`). Run `sync_table` again afterwards.
- A table interrupted by a crash or an error is `FAILED` and excluded from replication. Running `sync_table` again reloads it from scratch.
- During the two short pauses, `Replica_SQL_Running` is `No` and `START REPLICA` on that channel fails with `duckdb sync job N has paused the replica applier and restarts it itself`. The job restarts the applier when the pause ends, but only if the applier was running when the pause began.
- `replicate-rewrite-db` / `replicate-rewrite-table` are not supported for synced tables: the source and local names must match.

## Managed RDS Alternative

RDS MySQL provides managed DuckDB analytical primary and read-only instances. Provisioning, topology, synchronization, and supported versions are described in the official [English](https://help.aliyun.com/en/rds/apsaradb-rds-for-mysql/duckdb-analysis-instance) and [Chinese](https://help.aliyun.com/zh/rds/apsaradb-rds-for-mysql/duckdb-analysis-instance) documentation. The bootstrap procedure on this page is for self-managed AliSQL.
