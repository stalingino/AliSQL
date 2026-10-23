/*****************************************************************************

Copyright (c) 2025, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

#pragma once

#include "sql/package/proc.h"

using namespace im;

namespace myduck {

extern const LEX_CSTRING DUCKDB_PROC_SCHEMA;

/**
  Execute sql by duckdb

  1) Uniform schema: dbms_duckdb
*/
class Duckdb_proc_base : public Proc, public Disable_copy_base {
 public:
  explicit Duckdb_proc_base(PSI_memory_key key) : Proc(key) {}

  virtual const std::string qname() const override {
    std::stringstream ss;
    ss << DUCKDB_PROC_SCHEMA.str << "." << str();
    return ss.str();
  }
};

class Sql_cmd_duckdb_proc_base : public Sql_cmd_admin_proc {
 public:
  explicit Sql_cmd_duckdb_proc_base(THD *thd, mem_root_deque<Item *> *list,
                                    const Proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc) {}
};

class Sql_cmd_duckdb_query : public Sql_cmd_duckdb_proc_base {
 public:
  explicit Sql_cmd_duckdb_query(THD *thd, mem_root_deque<Item *> *list,
                                const Proc *proc)
      : Sql_cmd_duckdb_proc_base(thd, list, proc) {}

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;

  void send_result(THD *thd, bool error) override;
};

class Duckdb_proc_query : public Duckdb_proc_base {
  using Sql_cmd_type = Sql_cmd_duckdb_query;

  /** All the parameters */
  enum enum_parameter { QUERY_PARAM_SQL = 0, QUERY_PARAM_LAST };

  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
      case QUERY_PARAM_SQL:
        return MYSQL_TYPE_VARCHAR;
      default:
        assert(0);
    }
    return MYSQL_TYPE_VARCHAR;
  }

  enum enum_column { COLUMN_RESULT = 0, COLUMN_LAST };

 public:
  explicit Duckdb_proc_query(PSI_memory_key key = 0) : Duckdb_proc_base(key) {
    /* Init parameters */
    for (size_t i = QUERY_PARAM_SQL; i < QUERY_PARAM_LAST; i++) {
      m_parameters.assign_at(
          i, get_field_type(static_cast<enum enum_parameter>(i)));
    }

    /* Result set protocol packet */
    m_result_type = Result_type::RESULT_SET;
    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_VARCHAR, STRING_WITH_LEN("RESULT"), 1024}};

    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.assign_at(i, elements[i]);
    }
  }

  /**
    Singleton instance for query()
  */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for query() proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  virtual ~Duckdb_proc_query() {}

  /** Proc name */
  virtual const std::string str() const override {
    return std::string("query");
  }
};

/**
  dbms_duckdb.sync_table(schema, tables, options)

  Load tables from the replication source and add them to replication.
*/
class Sql_cmd_duckdb_sync_table : public Sql_cmd_duckdb_proc_base {
 public:
  explicit Sql_cmd_duckdb_sync_table(THD *thd, mem_root_deque<Item *> *list,
                                     const Proc *proc)
      : Sql_cmd_duckdb_proc_base(thd, list, proc) {}

  bool pc_execute(THD *thd) override;

  void send_result(THD *thd, bool error) override;

 private:
  ulonglong m_job_id{0};
  std::string m_message;
};

class Duckdb_proc_sync_table : public Duckdb_proc_base {
  using Sql_cmd_type = Sql_cmd_duckdb_sync_table;

  enum enum_parameter {
    PARAM_SCHEMA = 0,
    PARAM_TABLES,
    PARAM_OPTIONS,
    PARAM_LAST
  };

  enum enum_column { COLUMN_JOB_ID = 0, COLUMN_MESSAGE, COLUMN_LAST };

 public:
  explicit Duckdb_proc_sync_table(PSI_memory_key key = 0)
      : Duckdb_proc_base(key) {
    for (size_t i = PARAM_SCHEMA; i < PARAM_LAST; i++) {
      m_parameters.assign_at(i, MYSQL_TYPE_VARCHAR);
    }

    m_result_type = Result_type::RESULT_SET;
    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_LONGLONG, STRING_WITH_LEN("JOB_ID"), 0},
        {MYSQL_TYPE_VARCHAR, STRING_WITH_LEN("MESSAGE"), 1024}};
    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.assign_at(i, elements[i]);
    }
  }

  static Proc *instance();

  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;

  const std::string str() const override { return std::string("sync_table"); }
};

/** dbms_duckdb.sync_status() */
class Sql_cmd_duckdb_sync_status : public Sql_cmd_duckdb_proc_base {
 public:
  explicit Sql_cmd_duckdb_sync_status(THD *thd, mem_root_deque<Item *> *list,
                                      const Proc *proc)
      : Sql_cmd_duckdb_proc_base(thd, list, proc) {}

  bool pc_execute(THD *) override { return false; }

  void send_result(THD *thd, bool error) override;
};

class Duckdb_proc_sync_status : public Duckdb_proc_base {
  using Sql_cmd_type = Sql_cmd_duckdb_sync_status;

  enum enum_column {
    COLUMN_SCHEMA_NAME = 0,
    COLUMN_TABLE_NAME,
    COLUMN_STATE,
    COLUMN_ROWS_LOADED,
    COLUMN_ROWS_EXPECTED,
    COLUMN_JOB_ID,
    COLUMN_STARTED_AT,
    COLUMN_FINISHED_AT,
    COLUMN_SNAPSHOT_GTID_SET,
    COLUMN_LAST_ERROR,
    COLUMN_LAST
  };

 public:
  explicit Duckdb_proc_sync_status(PSI_memory_key key = 0)
      : Duckdb_proc_base(key) {
    m_result_type = Result_type::RESULT_SET;
    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_VARCHAR, STRING_WITH_LEN("SCHEMA_NAME"), NAME_LEN},
        {MYSQL_TYPE_VARCHAR, STRING_WITH_LEN("TABLE_NAME"), NAME_LEN},
        {MYSQL_TYPE_VARCHAR, STRING_WITH_LEN("STATE"), 16},
        {MYSQL_TYPE_LONGLONG, STRING_WITH_LEN("ROWS_LOADED"), 0},
        {MYSQL_TYPE_LONGLONG, STRING_WITH_LEN("ROWS_EXPECTED"), 0},
        {MYSQL_TYPE_LONGLONG, STRING_WITH_LEN("JOB_ID"), 0},
        {MYSQL_TYPE_VARCHAR, STRING_WITH_LEN("STARTED_AT"), 32},
        {MYSQL_TYPE_VARCHAR, STRING_WITH_LEN("FINISHED_AT"), 32},
        {MYSQL_TYPE_VARCHAR, STRING_WITH_LEN("SNAPSHOT_GTID_SET"), 4096},
        {MYSQL_TYPE_VARCHAR, STRING_WITH_LEN("LAST_ERROR"), 1024}};
    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.assign_at(i, elements[i]);
    }
  }

  static Proc *instance();

  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;

  const std::string str() const override {
    return std::string("sync_status");
  }
};

/** dbms_duckdb.unsync_table(schema, table) */
class Sql_cmd_duckdb_unsync_table : public Sql_cmd_duckdb_proc_base {
 public:
  explicit Sql_cmd_duckdb_unsync_table(THD *thd, mem_root_deque<Item *> *list,
                                       const Proc *proc)
      : Sql_cmd_duckdb_proc_base(thd, list, proc) {}

  bool pc_execute(THD *thd) override;
};

class Duckdb_proc_unsync_table : public Duckdb_proc_base {
  using Sql_cmd_type = Sql_cmd_duckdb_unsync_table;

  enum enum_parameter { PARAM_SCHEMA = 0, PARAM_TABLE, PARAM_LAST };

 public:
  explicit Duckdb_proc_unsync_table(PSI_memory_key key = 0)
      : Duckdb_proc_base(key) {
    for (size_t i = PARAM_SCHEMA; i < PARAM_LAST; i++) {
      m_parameters.assign_at(i, MYSQL_TYPE_VARCHAR);
    }
    m_result_type = Result_type::RESULT_OK;
  }

  static Proc *instance();

  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;

  const std::string str() const override {
    return std::string("unsync_table");
  }
};

}  // namespace myduck
