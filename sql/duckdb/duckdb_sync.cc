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

#include "sql/duckdb/duckdb_sync.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <ctime>
#include <map>
#include <memory>
#include <mutex>
#include <regex>
#include <shared_mutex>
#include <thread>

#include "m_ctype.h"
#include "my_dbug.h"
#include "my_sys.h"
#include "mysql.h"
#include "mysql/components/services/log_builtins.h"
#include "mysqld_error.h"
#include "scope_guard.h"
#include "sql/debug_sync.h"
#include "sql/duckdb/duckdb_config.h"
#include "sql/duckdb/duckdb_manager.h"
#include "sql/duckdb/duckdb_sql_quote.h"
#include "sql/field.h"
#include "sql/mysqld.h"
#include "sql/rpl_gtid.h"
#include "sql/rpl_mi.h"
#include "sql/rpl_msr.h"
#include "sql/rpl_rli.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_plugin.h"
#include "sql/sql_prepare.h"
#include "sql/sql_thd_internal_api.h"
#include "sql/strfunc.h"
#include "sql/table.h"
#include "sql/thd_raii.h"
#include "sql/transaction.h"
#include "sql/tztime.h"
#include "violite.h"

namespace myduck {

namespace {

/*************************************************************************
  Registry of synced tables
*************************************************************************/

enum class Sync_state { LOADING, CATCHING_UP, ACTIVE, FAILED };

const char *state_name(Sync_state state) {
  switch (state) {
    case Sync_state::LOADING:
      return "LOADING";
    case Sync_state::CATCHING_UP:
      return "CATCHING_UP";
    case Sync_state::ACTIVE:
      return "ACTIVE";
    case Sync_state::FAILED:
      return "FAILED";
  }
  return "FAILED";
}

bool state_from_name(const std::string &name, Sync_state *state) {
  for (Sync_state s : {Sync_state::LOADING, Sync_state::CATCHING_UP,
                       Sync_state::ACTIVE, Sync_state::FAILED}) {
    if (name == state_name(s)) {
      *state = s;
      return false;
    }
  }
  return true;
}

struct Sync_entry {
  std::string db;
  std::string table;
  Sync_state state{Sync_state::LOADING};
  /**
    Source GTID set the snapshot corresponds to. Set while LOADING once the
    snapshot is taken (the applier then buffers changes outside it), and while
    CATCHING_UP.
  */
  std::string gtid_text;
  std::shared_ptr<const Gtid_set> gtids;
  /** The applier saw DDL on the table while it was loading. */
  std::atomic<bool> load_aborted{false};
  /** Next sequence number of rows appended to the delta table. */
  std::atomic<ulonglong> delta_seq{1};
  ulonglong job_id{0};
  std::atomic<ulonglong> rows_loaded{0};
  std::atomic<ulonglong> rows_expected{0};
  std::string started_at;
  std::string finished_at;
  std::string last_error;
};

using Entry_ptr = std::shared_ptr<Sync_entry>;

/*
  Lock order: job_mutex -> registry_lock -> global_sid_lock.
  The applier only takes registry_lock in shared mode.
*/
std::shared_mutex registry_lock;
std::map<std::string, Entry_ptr> registry;
/* Lets the applier skip all locking while nothing is registered. */
std::atomic<size_t> registry_size{0};
/* Number of tables whose changes are buffered; checked for every row. */
std::atomic<size_t> buffered_tables{0};
std::atomic<ulonglong> next_job_id{1};

/* The applier a job holds stopped (see Applier_pause), and that job. */
std::mutex applier_hold_mutex;
const THD *applier_holder = nullptr;
std::string applier_hold_channel;
ulonglong applier_hold_job = 0;

constexpr const char REGISTRY_TABLE[] = "mysql.duckdb_sync_tables";

/** Refresh the counters after the registry changed; needs registry_lock. */
void registry_changed() {
  size_t buffered = 0;
  for (auto &it : registry)
    if (it.second->state == Sync_state::LOADING && it.second->gtids) buffered++;
  buffered_tables = buffered;
  registry_size = registry.size();
}

std::string now_string() {
  char buf[32];
  time_t now = time(nullptr);
  struct tm tm;
  gmtime_r(&now, &tm);
  strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);
  return buf;
}

std::string normalize_name(const std::string &name) {
  if (!lower_case_table_names) return name;
  std::vector<char> buf(name.begin(), name.end());
  buf.push_back('\0');
  my_casedn_str(files_charset_info, buf.data());
  return std::string(buf.data());
}

std::string registry_key(const std::string &db, const std::string &table) {
  std::string key = normalize_name(db);
  key.push_back('\0');
  key.append(normalize_name(table));
  return key;
}

std::string quote_ident(const std::string &name) {
  return quote_duckdb_ident(name);
}

std::string quote_table(const std::string &db, const std::string &table) {
  return quote_ident(db) + "." + quote_ident(table);
}

/** Quote a MySQL string literal (backslash-safe under any sql_mode). */
std::string quote_mysql_string(const std::string &s) {
  std::string out = "'";
  for (char c : s) {
    if (c == '\'' || c == '\\') out += c;
    out += c;
  }
  out += "'";
  return out;
}

Sync_status_row to_row(const Sync_entry &e) {
  Sync_status_row row;
  row.schema_name = e.db;
  row.table_name = e.table;
  row.state = state_name(e.state);
  row.rows_loaded = e.rows_loaded.load();
  row.rows_expected = e.rows_expected.load();
  row.job_id = e.job_id;
  row.started_at = e.started_at;
  row.finished_at = e.finished_at;
  row.snapshot_gtid_set = e.gtid_text;
  row.last_error = e.last_error;
  return row;
}

/** Run a statement on a fresh DuckDB connection. */
std::unique_ptr<duckdb::MaterializedQueryResult> duckdb_run(
    const std::string &sql, std::string *error) {
  try {
    auto con = DuckdbManager::CreateConnection();
    auto res = con->Query(sql);
    if (res->HasError()) {
      *error = res->GetError();
      return nullptr;
    }
    return std::move(res);
  } catch (std::exception &ex) {
    *error = ex.what();
    return nullptr;
  }
}

/* Set once mysql.duckdb_sync_tables exists; guarded by persist_mutex. */
bool registry_table_created = false;

bool create_registry_table(std::string *error) {
  if (registry_table_created) return false;
  if (!duckdb_run("CREATE SCHEMA IF NOT EXISTS mysql", error) ||
      !duckdb_run(std::string("CREATE TABLE IF NOT EXISTS ") + REGISTRY_TABLE +
                      "(schema_name VARCHAR NOT NULL,"
                      " table_name VARCHAR NOT NULL,"
                      " state VARCHAR NOT NULL,"
                      " snapshot_gtid_set VARCHAR,"
                      " job_id UBIGINT,"
                      " rows_loaded UBIGINT,"
                      " rows_expected UBIGINT,"
                      " started_at VARCHAR,"
                      " finished_at VARCHAR,"
                      " last_error VARCHAR,"
                      " PRIMARY KEY (schema_name, table_name))",
                  error))
    return true;
  registry_table_created = true;
  return false;
}

void persist(const Sync_status_row &r) {
  std::string sql = std::string("INSERT OR REPLACE INTO ") + REGISTRY_TABLE +
                    " VALUES (" + quote_duckdb_string(r.schema_name) + ", " +
                    quote_duckdb_string(r.table_name) + ", " +
                    quote_duckdb_string(r.state) + ", " +
                    quote_duckdb_string(r.snapshot_gtid_set) + ", " +
                    std::to_string(r.job_id) + ", " +
                    std::to_string(r.rows_loaded) + ", " +
                    std::to_string(r.rows_expected) + ", " +
                    quote_duckdb_string(r.started_at) + ", " +
                    quote_duckdb_string(r.finished_at) + ", " +
                    quote_duckdb_string(r.last_error) + ")";
  std::string error;
  if (create_registry_table(&error) || !duckdb_run(sql, &error)) {
    std::string msg = "duckdb sync: failed to persist state of " +
                      r.schema_name + "." + r.table_name + ": " + error;
    LogErr(WARNING_LEVEL, ER_DUCKDB, msg.c_str());
  }
}

/*
  Serializes writes to the registry table: concurrent writes to the same row
  conflict in DuckDB, and taking it before reading the entry makes the newest
  state win.
*/
std::mutex persist_mutex;

void persist_entry(const Entry_ptr &entry) {
  std::lock_guard<std::mutex> persist_guard(persist_mutex);
  Sync_status_row row;
  {
    std::shared_lock<std::shared_mutex> guard(registry_lock);
    row = to_row(*entry);
  }
  persist(row);
}

void persist_delete(const std::string &db, const std::string &table) {
  std::string sql = std::string("DELETE FROM ") + REGISTRY_TABLE +
                    " WHERE schema_name = " + quote_duckdb_string(db) +
                    " AND table_name = " + quote_duckdb_string(table);
  std::string error;
  std::lock_guard<std::mutex> persist_guard(persist_mutex);
  if (!registry_table_created) return;
  if (!duckdb_run(sql, &error)) {
    std::string msg = "duckdb sync: failed to delete state of " + db + "." +
                      table + ": " + error;
    LogErr(WARNING_LEVEL, ER_DUCKDB, msg.c_str());
  }
}

/** Parse a GTID set into a set without its own lock (safe for readers). */
std::shared_ptr<const Gtid_set> parse_gtids(const std::string &text,
                                            std::string *error) {
  auto set = std::make_shared<Gtid_set>(global_sid_map, nullptr);
  global_sid_lock->wrlock();
  enum_return_status status = set->add_gtid_text(text.c_str());
  global_sid_lock->unlock();
  if (status != RETURN_STATUS_OK) {
    *error = "cannot parse GTID set '" + text + "'";
    return nullptr;
  }
  return set;
}

/**
  Move CATCHING_UP tables whose snapshot GTID set has been executed to ACTIVE.
  From then on no transaction can be in the set, so the set is dropped.
*/
void flip_caught_up() {
  std::vector<Entry_ptr> changed;
  {
    std::unique_lock<std::shared_mutex> guard(registry_lock);
    global_sid_lock->wrlock();
    for (auto &it : registry) {
      Sync_entry &e = *it.second;
      if (e.state != Sync_state::CATCHING_UP) continue;
      if (e.gtids && !e.gtids->is_subset(gtid_state->get_executed_gtids()))
        continue;
      e.state = Sync_state::ACTIVE;
      e.gtids.reset();
      changed.push_back(it.second);
    }
    global_sid_lock->unlock();
  }
  for (auto &entry : changed) persist_entry(entry);
}

bool in_snapshot(THD *thd, const Sync_entry &e) {
  return e.gtids && thd->owned_gtid.sidno > 0 &&
         e.gtids->contains_gtid(thd->owned_gtid);
}

Sync_decision decide(THD *thd, Sync_entry &e, bool row_event) {
  switch (e.state) {
    case Sync_state::LOADING:
      /*
        Before the snapshot is taken every transaction the applier sees is
        already committed on the source, so it will be in the snapshot.
      */
      if (!e.gtids || e.load_aborted || in_snapshot(thd, e))
        return Sync_decision::EXCLUDE;
      if (row_event) return Sync_decision::INCLUDE;  // buffered by the handler
      /* DDL cannot be buffered; the delta would no longer match the table. */
      e.load_aborted = true;
      return Sync_decision::EXCLUDE;
    case Sync_state::FAILED:
      return Sync_decision::EXCLUDE;
    case Sync_state::ACTIVE:
      return Sync_decision::INCLUDE;
    case Sync_state::CATCHING_UP:
      return in_snapshot(thd, e) ? Sync_decision::EXCLUDE
                                 : Sync_decision::INCLUDE;
  }
  return Sync_decision::NO_OPINION;
}

constexpr const char DELTA_PREFIX[] = "duckdb_sync_delta#";

bool is_system_schema_name(const std::string &db) {
  std::string n = db;
  std::transform(n.begin(), n.end(), n.begin(), ::tolower);
  return n == "mysql" || n == "sys" || n == "information_schema" ||
         n == "performance_schema";
}

/*************************************************************************
  Options
*************************************************************************/

struct Sync_options {
  uint threads{4};
  ulonglong commit_rows{500000};
  ulong lock_wait_timeout{60};
  ulong net_timeout{3600};
  bool refresh{false};
  bool adopt{false};
  bool verify{true};
  std::string channel;
};

std::string trim(const std::string &s) {
  size_t b = s.find_first_not_of(" \t\r\n");
  if (b == std::string::npos) return "";
  size_t e = s.find_last_not_of(" \t\r\n");
  return s.substr(b, e - b + 1);
}

std::vector<std::string> split(const std::string &s, const char *separators) {
  std::vector<std::string> out;
  size_t start = 0;
  while (start <= s.size()) {
    size_t pos = s.find_first_of(separators, start);
    if (pos == std::string::npos) pos = s.size();
    std::string item = trim(s.substr(start, pos - start));
    if (!item.empty()) out.push_back(item);
    start = pos + 1;
  }
  return out;
}

bool parse_number(const std::string &value, ulonglong min, ulonglong max,
                  ulonglong *out) {
  if (value.empty() ||
      !std::all_of(value.begin(), value.end(), ::isdigit))
    return true;
  errno = 0;
  ulonglong v = strtoull(value.c_str(), nullptr, 10);
  if (errno || v < min || v > max) return true;
  *out = v;
  return false;
}

bool parse_bool(const std::string &value, bool *out) {
  std::string v = value;
  std::transform(v.begin(), v.end(), v.begin(), ::tolower);
  if (v == "1" || v == "on" || v == "true" || v == "yes") {
    *out = true;
  } else if (v == "0" || v == "off" || v == "false" || v == "no") {
    *out = false;
  } else {
    return true;
  }
  return false;
}

bool parse_options(const std::string &text, Sync_options *opts,
                   std::string *error) {
  for (const std::string &item : split(text, ";,")) {
    size_t eq = item.find('=');
    std::string key = trim(item.substr(0, eq));
    std::string value = eq == std::string::npos ? "" : trim(item.substr(eq + 1));
    std::transform(key.begin(), key.end(), key.begin(), ::tolower);
    ulonglong n = 0;
    bool bad = false;
    if (key == "threads") {
      bad = parse_number(value, 1, 64, &n);
      opts->threads = static_cast<uint>(n);
    } else if (key == "commit_rows") {
      bad = parse_number(value, 1, ULLONG_MAX, &n);
      opts->commit_rows = n;
    } else if (key == "lock_wait_timeout") {
      bad = parse_number(value, 1, 31536000, &n);
      opts->lock_wait_timeout = static_cast<ulong>(n);
    } else if (key == "net_timeout") {
      bad = parse_number(value, 1, 31536000, &n);
      opts->net_timeout = static_cast<ulong>(n);
    } else if (key == "refresh") {
      bad = parse_bool(value, &opts->refresh);
    } else if (key == "adopt") {
      bad = parse_bool(value, &opts->adopt);
    } else if (key == "verify") {
      bad = parse_bool(value, &opts->verify);
    } else if (key == "channel") {
      opts->channel = value;
    } else {
      *error = "unknown option '" + key + "'";
      return true;
    }
    if (bad) {
      *error = "invalid value '" + value + "' for option '" + key + "'";
      return true;
    }
  }
  if (opts->refresh && opts->adopt) {
    *error = "options refresh and adopt cannot be combined";
    return true;
  }
  return false;
}

/*************************************************************************
  Replication source access
*************************************************************************/

struct Source_params {
  std::string channel;
  std::string host;
  uint port{0};
  std::string user;
  std::string password;
  std::string bind_addr;
  bool ssl{false};
  bool ssl_verify_server_cert{false};
  std::string ssl_ca, ssl_capath, ssl_cert, ssl_cipher, ssl_key, ssl_crl,
      ssl_crlpath, tls_version;
  bool tls_ciphersuites_null{true};
  std::string tls_ciphersuites;
  std::string public_key_path;
  bool get_public_key{false};
  std::string default_auth;
};

/**
  Read the connection settings of a replication channel.

  @param[out] sql_running  whether the channel's applier is running
*/
bool read_channel(const std::string &channel, Source_params *params,
                  bool *sql_running, std::string *error) {
  channel_map.rdlock();
  auto unlock_map = create_scope_guard([]() { channel_map.unlock(); });

  Master_info *mi = channel_map.get_mi(channel.c_str());
  if (mi == nullptr || !Master_info::is_configured(mi)) {
    *error = "replication channel '" + channel + "' is not configured";
    return true;
  }

  mi->channel_rdlock();
  auto unlock_channel = create_scope_guard([mi]() { mi->channel_unlock(); });

  if (sql_running) *sql_running = mi->rli != nullptr && mi->rli->slave_running;
  if (params == nullptr) return false;

  char password[MAX_PASSWORD_LENGTH + 1];
  size_t password_size = sizeof(password);
  if (mi->get_password(password, &password_size)) {
    *error = "cannot read the password of channel '" + channel + "'";
    return true;
  }

  params->channel = channel;
  params->host = mi->host;
  params->port = mi->port;
  params->user = mi->get_user() ? mi->get_user() : "";
  params->password.assign(password, password_size);
  params->bind_addr = mi->bind_addr;
  params->ssl = mi->ssl;
  params->ssl_verify_server_cert = mi->ssl_verify_server_cert;
  params->ssl_ca = mi->ssl_ca;
  params->ssl_capath = mi->ssl_capath;
  params->ssl_cert = mi->ssl_cert;
  params->ssl_cipher = mi->ssl_cipher;
  params->ssl_key = mi->ssl_key;
  params->ssl_crl = mi->ssl_crl;
  params->ssl_crlpath = mi->ssl_crlpath;
  params->tls_version = mi->tls_version;
  params->tls_ciphersuites_null = mi->tls_ciphersuites.first;
  params->tls_ciphersuites = mi->tls_ciphersuites.second;
  params->public_key_path = mi->public_key_path;
  params->get_public_key = mi->get_public_key;
  if (mi->is_start_plugin_auth_configured())
    params->default_auth = mi->get_start_plugin_auth();

  if (params->user.empty()) {
    *error = "replication channel '" + channel + "' has no user";
    return true;
  }
  return false;
}

/** A client connection to the replication source. */
class Source_conn {
 public:
  ~Source_conn() { close(); }

  bool connect(const Source_params &p, const Sync_options &opts,
               std::string *error) {
    m_mysql = mysql_init(nullptr);
    if (m_mysql == nullptr) {
      *error = "out of memory";
      return true;
    }

    uint connect_timeout = 30;
    mysql_options(m_mysql, MYSQL_OPT_CONNECT_TIMEOUT, &connect_timeout);
    if (!p.bind_addr.empty())
      mysql_options(m_mysql, MYSQL_OPT_BIND, p.bind_addr.c_str());

    /* Same SSL setup as the replication receiver (connect_to_master()). */
    enum mysql_ssl_mode ssl_mode = SSL_MODE_DISABLED;
    if (p.ssl) {
      auto opt = [](const std::string &s) {
        return s.empty() ? nullptr : s.c_str();
      };
      mysql_options(m_mysql, MYSQL_OPT_SSL_KEY, opt(p.ssl_key));
      mysql_options(m_mysql, MYSQL_OPT_SSL_CERT, opt(p.ssl_cert));
      mysql_options(m_mysql, MYSQL_OPT_SSL_CA, opt(p.ssl_ca));
      mysql_options(m_mysql, MYSQL_OPT_SSL_CAPATH, opt(p.ssl_capath));
      mysql_options(m_mysql, MYSQL_OPT_SSL_CIPHER, opt(p.ssl_cipher));
      mysql_options(m_mysql, MYSQL_OPT_SSL_CRL, opt(p.ssl_crl));
      mysql_options(m_mysql, MYSQL_OPT_SSL_CRLPATH, opt(p.ssl_crlpath));
      mysql_options(m_mysql, MYSQL_OPT_TLS_VERSION, opt(p.tls_version));
      mysql_options(m_mysql, MYSQL_OPT_TLS_CIPHERSUITES,
                    p.tls_ciphersuites_null ? nullptr
                                            : p.tls_ciphersuites.c_str());
      if (p.ssl_verify_server_cert)
        ssl_mode = SSL_MODE_VERIFY_IDENTITY;
      else if (!p.ssl_ca.empty() || !p.ssl_capath.empty())
        ssl_mode = SSL_MODE_VERIFY_CA;
      else
        ssl_mode = SSL_MODE_REQUIRED;
    }
    mysql_options(m_mysql, MYSQL_OPT_SSL_MODE, &ssl_mode);
    if (!p.public_key_path.empty())
      mysql_options(m_mysql, MYSQL_SERVER_PUBLIC_KEY,
                    p.public_key_path.c_str());
    bool get_public_key = p.get_public_key;
    mysql_options(m_mysql, MYSQL_OPT_GET_SERVER_PUBLIC_KEY, &get_public_key);
    if (!p.default_auth.empty())
      mysql_options(m_mysql, MYSQL_DEFAULT_AUTH, p.default_auth.c_str());
    if (opt_plugin_dir_ptr && *opt_plugin_dir_ptr)
      mysql_options(m_mysql, MYSQL_PLUGIN_DIR, opt_plugin_dir_ptr);
    mysql_options(m_mysql, MYSQL_SET_CHARSET_NAME, "utf8mb4");
    mysql_options4(m_mysql, MYSQL_OPT_CONNECT_ATTR_ADD, "program_name",
                   "mysqld");
    mysql_options4(m_mysql, MYSQL_OPT_CONNECT_ATTR_ADD, "_client_role",
                   "duckdb_sync");

    if (!mysql_real_connect(m_mysql, p.host.c_str(), p.user.c_str(),
                            p.password.c_str(), nullptr, p.port, nullptr,
                            0)) {
      *error = "cannot connect to source " + p.host + ":" +
               std::to_string(p.port) + ": " + mysql_error(m_mysql);
      return true;
    }

    /*
      - UTC, so TIMESTAMP values convert back to the same instant locally.
      - character_set_results=NULL, so strings arrive in their column charset.
      - Long network timeouts, the loader may pause reading while it commits.
    */
    std::string net_timeout = std::to_string(opts.net_timeout);
    return exec(
        "SET SESSION time_zone = '+00:00', character_set_results = NULL, "
        "sql_mode = '', net_write_timeout = " +
            net_timeout + ", net_read_timeout = " + net_timeout +
            ", wait_timeout = " + net_timeout +
            ", lock_wait_timeout = " + std::to_string(opts.lock_wait_timeout) +
            ", transaction_isolation = 'REPEATABLE-READ'",
        error);
  }

  /** Execute a statement and discard any result. */
  bool exec(const std::string &sql, std::string *error) {
    if (mysql_real_query(m_mysql, sql.data(), sql.length())) {
      set_error(sql, error);
      return true;
    }
    MYSQL_RES *res = mysql_store_result(m_mysql);
    if (res != nullptr) {
      mysql_free_result(res);
    } else if (mysql_field_count(m_mysql) != 0) {
      set_error(sql, error);
      return true;
    }
    return false;
  }

  /** Read column `col` of the first row of a query. */
  bool query_value(const std::string &sql, uint col, std::string *value,
                   bool *is_null, std::string *error) {
    if (mysql_real_query(m_mysql, sql.data(), sql.length())) {
      set_error(sql, error);
      return true;
    }
    MYSQL_RES *res = mysql_store_result(m_mysql);
    if (res == nullptr) {
      set_error(sql, error);
      return true;
    }
    auto free_res = create_scope_guard([res]() { mysql_free_result(res); });
    MYSQL_ROW row = mysql_fetch_row(res);
    if (row == nullptr || mysql_num_fields(res) <= col) {
      *error = "no result for: " + sql;
      return true;
    }
    unsigned long *lengths = mysql_fetch_lengths(res);
    *is_null = row[col] == nullptr;
    if (row[col] != nullptr)
      value->assign(row[col], lengths[col]);
    else
      value->clear();
    return false;
  }

  void set_error(const std::string &sql, std::string *error) {
    std::string stmt = sql.length() > 200 ? sql.substr(0, 200) + "..." : sql;
    *error = std::string("source error ") +
             std::to_string(mysql_errno(m_mysql)) + " (" +
             mysql_error(m_mysql) + ") on: " + stmt;
  }

  /** Make a blocked read on this connection fail; any thread may call it. */
  void interrupt() {
    if (m_mysql != nullptr && m_mysql->net.vio != nullptr)
      vio_shutdown(m_mysql->net.vio);
  }

  void close() {
    if (m_mysql != nullptr) {
      mysql_close(m_mysql);
      m_mysql = nullptr;
    }
  }

  MYSQL *handle() { return m_mysql; }

 private:
  MYSQL *m_mysql{nullptr};
};

/*************************************************************************
  Local (replica) side helpers
*************************************************************************/

/** Execute a statement on the local server as thd. */
bool local_exec(THD *thd, const std::string &sql, std::string *error,
                uint *error_no = nullptr) {
  Ed_connection con(thd);
  LEX_STRING str;
  thd->set_query_id(next_query_id());
  lex_string_strmake(thd->mem_root, &str, sql.c_str(), sql.size());
  if (con.execute_direct(str)) {
    *error = con.get_last_error();
    if (error_no) *error_no = con.get_last_errno();
    return true;
  }
  return false;
}

/** Whether a DuckDB table exists and whether it is empty. */
bool duckdb_table_state(const std::string &db, const std::string &table,
                        bool *exists, bool *empty, std::string *error) {
  auto res = duckdb_run(
      "SELECT count(*) FROM information_schema.tables WHERE "
      "lower(table_schema) = lower(" +
          quote_duckdb_string(db) + ") AND lower(table_name) = lower(" +
          quote_duckdb_string(table) + ")",
      error);
  if (!res) return true;
  *exists = res->GetValue(0, 0).GetValue<int64_t>() > 0;
  *empty = true;
  if (!*exists) return false;
  res = duckdb_run("SELECT count(*) FROM (SELECT 1 FROM " +
                       quote_table(db, table) + " LIMIT 1)",
                   error);
  if (!res) return true;
  *empty = res->GetValue(0, 0).GetValue<int64_t>() == 0;
  return false;
}

bool duckdb_count(const std::string &db, const std::string &table,
                  ulonglong *count, std::string *error) {
  auto res = duckdb_run("SELECT count(*) FROM " + quote_table(db, table), error);
  if (!res) return true;
  *count = static_cast<ulonglong>(res->GetValue(0, 0).GetValue<int64_t>());
  return false;
}

/** Columns that get a value from the source; generated ones are computed. */
void collect_base_fields(TABLE *table, std::vector<Field *> *fields) {
  fields->clear();
  for (Field **f = table->field; *f != nullptr; f++) {
    if ((*f)->is_gcol() || (*f)->is_hidden_by_system()) continue;
    fields->push_back(*f);
  }
}

/** One statement-transaction worth of access to a local table. */
class Local_table {
 public:
  explicit Local_table(THD *thd) : m_thd(thd) {}
  ~Local_table() {
    std::string ignored;
    close(false, &ignored);
  }

  bool open(const std::string &db, const std::string &table,
            thr_lock_type lock_type, std::string *error) {
    lex_start(m_thd);
    m_thd->set_query_id(next_query_id());
    m_table_ref = new (m_thd->mem_root)
        Table_ref(db.c_str(), db.length(), table.c_str(), table.length(),
                  table.c_str(), lock_type);
    m_open = true;
    if (m_table_ref == nullptr ||
        open_and_lock_tables(m_thd, m_table_ref, 0)) {
      *error = "cannot open local table " + db + "." + table + ": " +
               (m_thd->is_error() ? m_thd->get_stmt_da()->message_text()
                                  : "unknown error");
      close(false, error);
      return true;
    }
    m_table_ref->table->use_all_columns();
    return false;
  }

  TABLE *table() { return m_table_ref->table; }

  /** Commit (or roll back) and release the table. */
  bool close(bool commit, std::string *error) {
    if (!m_open) return false;
    m_open = false;
    bool failed = false;
    if (commit) {
      failed = trans_commit_stmt(m_thd) || trans_commit(m_thd);
      if (failed)
        *error = m_thd->is_error() ? m_thd->get_stmt_da()->message_text()
                                   : "commit failed";
    }
    if (!commit || failed) {
      trans_rollback_stmt(m_thd);
      trans_rollback(m_thd);
    }
    close_thread_tables(m_thd);
    m_thd->mdl_context.release_transactional_locks();
    lex_end(m_thd->lex);
    m_thd->clear_error();
    m_thd->get_stmt_da()->reset_condition_info(m_thd);
    m_thd->mem_root->ClearForReuse();
    m_table_ref = nullptr;
    return failed;
  }

 private:
  THD *m_thd;
  Table_ref *m_table_ref{nullptr};
  bool m_open{false};
};

/** Make thd suitable for writing replica-local data. */
void setup_local_thd(THD *thd) {
  thd->security_context()->skip_grants();
  thd->set_skip_readonly_check();
  /* Rows keep the source's values: explicit 0 in AUTO_INCREMENT, zero dates.*/
  thd->variables.sql_mode = MODE_NO_AUTO_VALUE_ON_ZERO;
  /* An offset zone: the DuckDB session time zone cannot be TZ_UTC. */
  thd->variables.time_zone = my_tz_OFFSET0;
  thd->variables.option_bits |= OPTION_NO_FOREIGN_KEY_CHECKS;
  /* Never write to the local binlog: no errant GTIDs on the replica. */
  thd->variables.option_bits &= ~OPTION_BIN_LOG;
  thd->variables.sql_log_bin = false;
}

/*************************************************************************
  Sync job
*************************************************************************/

struct Table_layout {
  /** Columns that get their value from the source, in table order. */
  std::vector<std::string> columns;
  std::vector<std::string> pk_columns;
  /** Single-column integer primary key used to split the copy, or empty. */
  std::string split_column;
  bool split_unsigned{false};
};

struct Table_task {
  std::string table;
  /** Drop and recreate the local table before loading. */
  bool recreate{false};
  Entry_ptr entry;
  std::string create_sql;
  Table_layout layout;
  std::string copy_error;
  /** The table reached its final state in this job. */
  bool done{false};
};

struct Sync_job {
  ulonglong id{0};
  std::string schema;
  std::vector<Table_task> tasks;
  Sync_options opts;
  Source_params source;

  std::thread thread;
  std::atomic<bool> abort{false};
  std::atomic<bool> finished{false};
  std::atomic<THD *> thd{nullptr};

  /* Index 0 is the lock connection, 1..threads are the snapshot readers. */
  std::mutex conn_mutex;
  std::vector<std::unique_ptr<Source_conn>> conns;

  bool stopping() const {
    THD *t = thd.load();
    return abort.load() || (t != nullptr && t->killed);
  }

  void interrupt() {
    abort = true;
    std::lock_guard<std::mutex> guard(conn_mutex);
    for (auto &conn : conns) conn->interrupt();
  }

  void close_connections() {
    std::lock_guard<std::mutex> guard(conn_mutex);
    conns.clear();
  }
};

std::mutex job_mutex;
std::unique_ptr<Sync_job> current_job;

void set_entry_state(const Entry_ptr &entry, Sync_state state,
                     const std::string &error) {
  {
    std::unique_lock<std::shared_mutex> guard(registry_lock);
    entry->state = state;
    entry->last_error = error;
    entry->finished_at = now_string();
    if (state != Sync_state::CATCHING_UP) {
      entry->gtids.reset();
      entry->gtid_text.clear();
    }
    registry_changed();
  }
  persist_entry(entry);
}

/** Reads one key range of a table from the source into the local table. */
struct Copy_worker {
  Sync_job *job{nullptr};
  Source_conn *source{nullptr};
  std::string db;
  std::string table;
  std::string select_sql;
  Entry_ptr entry;
  ulonglong fetched{0};
  std::string error;

  void run(THD *thd) {
    setup_local_thd(thd);
    MYSQL *mysql = source->handle();
    if (mysql_real_query(mysql, select_sql.data(), select_sql.length())) {
      source->set_error(select_sql, &error);
      return;
    }
    MYSQL_RES *res = mysql_use_result(mysql);
    if (res == nullptr) {
      source->set_error(select_sql, &error);
      return;
    }
    auto free_res = create_scope_guard([&]() {
      /* Don't drain the rest of a huge result set after an error. */
      if (!error.empty()) job->interrupt();
      mysql_free_result(res);
    });

    Local_table local(thd);
    TABLE *tbl = nullptr;
    std::vector<Field *> fields;
    ulonglong in_chunk = 0;
    MYSQL_ROW row;

    while ((row = mysql_fetch_row(res)) != nullptr) {
      if (in_chunk == 0) {
        if (local.open(db, table, TL_WRITE, &error)) return;
        tbl = local.table();
        collect_base_fields(tbl, &fields);
        if (fields.size() != mysql_num_fields(res)) {
          error = "column count mismatch between source query and " + db +
                  "." + table;
          return;
        }
        /* Append-only batch: rows go through the DuckDB appender. */
        tbl->file->ha_extra(HA_EXTRA_BEGIN_ALTER_COPY);
        thd->check_for_truncated_fields = CHECK_FIELD_IGNORE;
      }

      unsigned long *lengths = mysql_fetch_lengths(res);
      restore_record(tbl, s->default_values);
      for (size_t i = 0; i < fields.size(); i++) {
        Field *field = fields[i];
        if (row[i] == nullptr) {
          if (field->is_nullable()) {
            field->set_null();
          } else {
            field->set_notnull();
            field->reset();
          }
          continue;
        }
        field->set_notnull();
        /* JSON arrives as utf8mb4 text; everything else in its own charset. */
        const CHARSET_INFO *cs = field->type() == MYSQL_TYPE_JSON
                                     ? &my_charset_utf8mb4_bin
                                     : field->charset();
        if (field->store(row[i], lengths[i], cs) == TYPE_ERR_OOM) {
          error = "out of memory";
          return;
        }
      }
      if (tbl->vfield != nullptr &&
          update_generated_write_fields(tbl->write_set, tbl)) {
        error = thd->is_error() ? thd->get_stmt_da()->message_text()
                                : "cannot compute generated columns";
        return;
      }
      if (tbl->next_number_field != nullptr)
        tbl->autoinc_field_has_explicit_non_null_value = true;
      int err = tbl->file->ha_write_row(tbl->record[0]);
      tbl->autoinc_field_has_explicit_non_null_value = false;
      if (err) {
        tbl->file->print_error(err, MYF(0));
        error = "write to " + db + "." + table + " failed: " +
                (thd->is_error() ? thd->get_stmt_da()->message_text()
                                 : std::to_string(err));
        return;
      }

      fetched++;
      if (++in_chunk >= job->opts.commit_rows) {
        if (local.close(true, &error)) return;
        entry->rows_loaded += in_chunk;
        in_chunk = 0;
        DBUG_EXECUTE_IF("duckdb_sync_crash_after_commit", DBUG_SUICIDE(););
      }
      if ((fetched & 0xfff) == 0 && (job->stopping() || thd->killed)) {
        error = "interrupted";
        return;
      }
    }

    if (mysql_errno(mysql)) {
      source->set_error(select_sql, &error);
      return;
    }
    if (in_chunk > 0) {
      if (local.close(true, &error)) return;
      entry->rows_loaded += in_chunk;
    }
  }
};

void copy_worker_main(Copy_worker *worker) {
  my_thread_init();
  THD *thd = create_thd(false, false, false, PSI_NOT_INSTRUMENTED, 0);
  worker->run(thd);
  thd->free_items();
  destroy_thd(thd);
  my_thread_end();
}

bool read_layout(THD *thd, const std::string &db, const std::string &table,
                 Table_layout *layout, std::string *error) {
  Local_table local(thd);
  if (local.open(db, table, TL_READ, error)) return true;
  TABLE *tbl = local.table();
  if (tbl->s->db_type()->db_type != DB_TYPE_DUCKDB) {
    *error = "local table " + db + "." + table + " is not a DuckDB table";
    return true;
  }
  std::vector<Field *> fields;
  collect_base_fields(tbl, &fields);
  for (Field *f : fields) layout->columns.push_back(f->field_name);

  /* The DuckDB applier merges changes by primary key. */
  if (tbl->s->primary_key == MAX_KEY) {
    *error = "table " + db + "." + table + " has no primary key";
    return true;
  }
  {
    const KEY &pk = tbl->key_info[tbl->s->primary_key];
    for (uint i = 0; i < pk.user_defined_key_parts; i++)
      layout->pk_columns.push_back(pk.key_part[i].field->field_name);
    if (pk.user_defined_key_parts == 1) {
      Field *f = pk.key_part[0].field;
      switch (f->real_type()) {
        case MYSQL_TYPE_TINY:
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_LONGLONG:
          layout->split_column = f->field_name;
          layout->split_unsigned = f->is_flag_set(UNSIGNED_FLAG);
          break;
        default:
          break;
      }
    }
  }
  return local.close(true, error);
}

std::string int128_to_string(__int128 v, bool is_unsigned) {
  return is_unsigned ? std::to_string(static_cast<unsigned long long>(v))
                     : std::to_string(static_cast<long long>(v));
}

/** Split [min, max] of the key into at most n contiguous ranges. */
std::vector<std::string> build_ranges(const std::string &column,
                                      bool is_unsigned,
                                      const std::string &min_text,
                                      const std::string &max_text, uint n) {
  __int128 lo = is_unsigned ? static_cast<__int128>(
                                  strtoull(min_text.c_str(), nullptr, 10))
                            : strtoll(min_text.c_str(), nullptr, 10);
  __int128 hi = is_unsigned ? static_cast<__int128>(
                                  strtoull(max_text.c_str(), nullptr, 10))
                            : strtoll(max_text.c_str(), nullptr, 10);
  __int128 span = hi - lo + 1;
  if (span < static_cast<__int128>(n)) n = static_cast<uint>(span);
  __int128 step = (span + n - 1) / n;

  std::vector<std::string> wheres;
  __int128 start = lo;
  for (uint i = 0; i < n && start <= hi; i++) {
    __int128 end = (i == n - 1) ? hi : std::min(hi, start + step - 1);
    wheres.push_back(" WHERE " + quote_ident(column) + " BETWEEN " +
                     int128_to_string(start, is_unsigned) + " AND " +
                     int128_to_string(end, is_unsigned));
    start = end + 1;
  }
  return wheres;
}

/**
  Turn SHOW CREATE TABLE output from the source into a local DuckDB DDL:
  qualify the table name and switch every ENGINE clause (table and
  partitions) to DuckDB. Only the text after the column list is rewritten.
*/
bool rewrite_create_sql(const std::string &create_sql, const std::string &db,
                        std::string *out, std::string *error) {
  static const std::string prefix = "CREATE TABLE ";
  if (create_sql.compare(0, prefix.size(), prefix) != 0) {
    *error = "source object is not a base table";
    return true;
  }
  size_t options_pos = create_sql.rfind("\n) ");
  if (options_pos == std::string::npos) options_pos = create_sql.rfind(')');
  if (options_pos == std::string::npos) {
    *error = "unexpected SHOW CREATE TABLE output";
    return true;
  }

  std::string head = create_sql.substr(prefix.size(), options_pos - prefix.size());
  std::string options = create_sql.substr(options_pos);
  bool has_engine = std::regex_search(options, std::regex("ENGINE\\s*="));
  options = std::regex_replace(options, std::regex("ENGINE\\s*=\\s*\\w+"),
                               "ENGINE=DuckDB");
  /* InnoDB storage placement options make no sense for DuckDB. */
  options = std::regex_replace(
      options, std::regex("/\\*!50100 TABLESPACE `[^`]*` \\*/"), "");
  options = std::regex_replace(
      options, std::regex("(DATA|INDEX) DIRECTORY\\s*=\\s*'[^']*'"), "");
  options = std::regex_replace(options,
                               std::regex("COMPRESSION\\s*=\\s*'[^']*'"), "");
  if (!has_engine) options += " ENGINE=DuckDB";

  *out = prefix + quote_ident(db) + "." + head + options;
  return false;
}

/** Run statements in one transaction on a fresh DuckDB connection. */
bool duckdb_run_transaction(const std::vector<std::string> &statements,
                            std::string *error) {
  try {
    auto con = DuckdbManager::CreateConnection();
    auto run = [&](const std::string &sql) {
      auto res = con->Query(sql);
      if (res->HasError()) {
        *error = res->GetError();
        return true;
      }
      return false;
    };
    if (run("BEGIN")) return true;
    for (const std::string &sql : statements) {
      if (run(sql)) {
        con->Query("ROLLBACK");
        return true;
      }
    }
    return run("COMMIT");
  } catch (std::exception &ex) {
    *error = ex.what();
    return true;
  }
}

std::string column_list(const std::vector<std::string> &columns) {
  std::string out;
  for (const std::string &c : columns) {
    if (!out.empty()) out += ", ";
    out += quote_ident(c);
  }
  return out;
}

void drop_delta_table(const std::string &db, const std::string &table) {
  std::string error;
  if (!duckdb_run("DROP TABLE IF EXISTS " + sync_delta_table(db, table),
                  &error)) {
    std::string msg = "duckdb sync: cannot drop the delta table of " + db +
                      "." + table + ": " + error;
    LogErr(WARNING_LEVEL, ER_DUCKDB, msg.c_str());
  }
}

/**
  Create (or reuse) the local table and its delta table. Runs while the
  applier is stopped.

  The delta table has the layout of the applier's batch buffer (see
  DeltaAppender): the table's columns, delete flag, row number, transaction
  number, plus a sequence number that orders rows across batches.
*/
bool prepare_table(THD *thd, Sync_job *job, Table_task &task,
                   std::string *error) {
  const std::string &db = job->schema;
  const std::string &table = task.table;

  if (local_exec(thd, "CREATE DATABASE IF NOT EXISTS " + quote_ident(db),
                 error))
    return true;
  if (task.recreate &&
      local_exec(thd, "DROP TABLE IF EXISTS " + quote_table(db, table), error))
    return true;

  std::string ddl;
  if (rewrite_create_sql(task.create_sql, db, &ddl, error)) return true;
  uint error_no = 0;
  if (local_exec(thd, ddl, error, &error_no)) {
    if (error_no != ER_TABLE_EXISTS_ERROR) {
      *error = "cannot create local table: " + *error;
      return true;
    }
    bool exists = false, empty = false;
    if (duckdb_table_state(db, table, &exists, &empty, error)) return true;
    if (!exists) {
      *error = "local table " + db + "." + table +
               " exists but is not a DuckDB table";
      return true;
    }
    if (!empty) {
      *error = "local table " + db + "." + table +
               " is not empty; use refresh=1 to reload it";
      return true;
    }
    error->clear();
  }

  if (read_layout(thd, db, table, &task.layout, error)) return true;

  std::string delta = sync_delta_table(db, table);
  return duckdb_run_transaction(
      {"DROP TABLE IF EXISTS " + delta,
       "CREATE TABLE " + delta + " AS FROM " + quote_table(db, table) +
           " LIMIT 0",
       "ALTER TABLE " + delta + " ADD COLUMN `#alibaba_rds_delete_flag` BOOL",
       "ALTER TABLE " + delta + " ADD COLUMN `#alibaba_rds_row_no` INT",
       "ALTER TABLE " + delta + " ADD COLUMN `#alibaba_rds_trx_no` INT",
       "ALTER TABLE " + delta + " ADD COLUMN `#sync_seq` BIGINT"},
      error);
}

/** Copy the snapshot of a table into the local table. */
bool copy_table(Sync_job *job, Table_task &task, std::string *error) {
  const std::string &db = job->schema;
  const std::string &table = task.table;
  const Table_layout &layout = task.layout;

  Source_conn *reader = job->conns[1].get();
  std::string from = " FROM " + quote_table(db, table);
  std::string value;
  bool is_null = false;

  if (job->opts.verify) {
    if (reader->query_value("SELECT COUNT(*)" + from, 0, &value, &is_null,
                            error))
      return true;
    task.entry->rows_expected = strtoull(value.c_str(), nullptr, 10);
  }

  std::vector<std::string> wheres{""};
  if (!layout.split_column.empty() && job->opts.threads > 1) {
    std::string min_value, max_value;
    bool min_null = false, max_null = false;
    std::string col = quote_ident(layout.split_column);
    if (reader->query_value("SELECT MIN(" + col + ")" + from, 0, &min_value,
                            &min_null, error) ||
        reader->query_value("SELECT MAX(" + col + ")" + from, 0, &max_value,
                            &max_null, error))
      return true;
    if (!min_null && !max_null)
      wheres = build_ranges(layout.split_column, layout.split_unsigned,
                            min_value, max_value, job->opts.threads);
  }

  task.entry->rows_loaded = 0;
  std::vector<Copy_worker> workers(wheres.size());
  std::vector<std::thread> threads;
  for (size_t i = 0; i < wheres.size(); i++) {
    Copy_worker &w = workers[i];
    w.job = job;
    w.source = job->conns[1 + i].get();
    w.db = db;
    w.table = table;
    w.select_sql = "SELECT " + column_list(layout.columns) + from + wheres[i];
    w.entry = task.entry;
    threads.emplace_back(copy_worker_main, &w);
  }
  for (auto &t : threads) t.join();

  ulonglong fetched = 0;
  for (auto &w : workers) {
    fetched += w.fetched;
    if (!w.error.empty() && error->empty()) *error = w.error;
  }
  if (!error->empty()) return true;
  if (job->stopping()) {
    *error = "interrupted";
    return true;
  }

  /* The applier writes to the delta table, so this is the copy alone. */
  ulonglong local_rows = 0;
  if (duckdb_count(db, table, &local_rows, error)) return true;
  if (local_rows != fetched) {
    *error = "loaded " + std::to_string(fetched) +
             " rows but the local table has " + std::to_string(local_rows);
    return true;
  }
  if (job->opts.verify && fetched != task.entry->rows_expected) {
    *error = "source snapshot has " +
             std::to_string(task.entry->rows_expected.load()) + " rows but " +
             std::to_string(fetched) + " were copied";
    return true;
  }
  return false;
}

/**
  Apply the changes buffered while the table was loading, the same way the
  applier flushes a batch: for every primary key in the delta the last row
  image wins, a final delete removes the row. Runs while the applier is
  stopped.
*/
bool merge_table(Sync_job *job, Table_task &task, std::string *error) {
  const std::string target = quote_table(job->schema, task.table);
  const std::string delta = sync_delta_table(job->schema, task.table);
  const std::string pk = column_list(task.layout.pk_columns);
  const std::string cols = column_list(task.layout.columns);
  return duckdb_run_transaction(
      {"DELETE FROM " + target + " WHERE (" + pk + ") IN (SELECT " + pk +
           " FROM " + delta + ")",
       "INSERT INTO " + target + " (" + cols +
           ") SELECT UNNEST(r) FROM (SELECT LAST(STRUCT_PACK(" + cols +
           ") ORDER BY `#sync_seq`) AS r, LAST(`#alibaba_rds_delete_flag` "
           "ORDER BY `#sync_seq`) AS deleted FROM " +
           delta + " GROUP BY " + pk + ") WHERE NOT deleted",
       "DROP TABLE " + delta},
      error);
}

/** Open the lock connection and one snapshot connection per copy thread. */
bool connect_source(Sync_job *job, std::string *error) {
  uint nconns = job->opts.threads + 1;
  for (uint i = 0; i < nconns; i++) {
    auto conn = std::make_unique<Source_conn>();
    Source_conn *raw = conn.get();
    {
      std::lock_guard<std::mutex> guard(job->conn_mutex);
      job->conns.push_back(std::move(conn));
    }
    if (raw->connect(job->source, job->opts, error)) return true;
    if (job->stopping()) {
      *error = "interrupted";
      return true;
    }
  }

  std::string value;
  bool is_null = false;
  if (job->conns[0]->query_value("SELECT @@GLOBAL.gtid_mode", 0, &value,
                                 &is_null, error))
    return true;
  if (value != "ON") {
    *error = "gtid_mode on the source is " + value + ", ON is required";
    return true;
  }
  return false;
}

/**
  Lock the tables on the source, start the snapshot of every reader, read
  the GTID set the snapshots correspond to and the table definitions.
*/
bool take_snapshot(Sync_job *job, std::string *gtid_text, std::string *error) {
  Source_conn *lock_conn = job->conns[0].get();
  std::string lock_sql = "LOCK TABLES ";
  for (size_t i = 0; i < job->tasks.size(); i++) {
    if (i > 0) lock_sql += ", ";
    lock_sql += quote_table(job->schema, job->tasks[i].table) + " READ";
  }
  if (lock_conn->exec(lock_sql, error)) {
    *error += " (the replication user needs SELECT and LOCK TABLES on " +
              job->schema + ")";
    return true;
  }

  for (size_t i = 1; i < job->conns.size(); i++) {
    if (job->conns[i]->exec(
            "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY", error))
      return true;
  }
  bool is_null = false;
  if (lock_conn->query_value("SELECT @@GLOBAL.gtid_executed", 0, gtid_text,
                             &is_null, error))
    return true;
  gtid_text->erase(std::remove(gtid_text->begin(), gtid_text->end(), '\n'),
                   gtid_text->end());

  for (Table_task &task : job->tasks) {
    if (lock_conn->query_value(
            "SHOW CREATE TABLE " + quote_table(job->schema, task.table), 1,
            &task.create_sql, &is_null, error))
      return true;
  }
  return lock_conn->exec("UNLOCK TABLES", error);
}

/**
  Fail if the applier executed anything outside A + G while it was supposed
  to be stopped (e.g. someone ran START REPLICA): such a transaction may have
  been skipped for the tables without being in their snapshot.
*/
bool check_no_concurrent_apply(const Gtid_set &applied_before,
                               const Gtid_set &snapshot, std::string *error) {
  Gtid_set extra(global_sid_map, nullptr);
  global_sid_lock->wrlock();
  bool failed = extra.add_gtid_set(gtid_state->get_executed_gtids()) !=
                RETURN_STATUS_OK;
  if (!failed) {
    extra.remove_gtid_set(&applied_before);
    extra.remove_gtid_set(&snapshot);
  }
  char *text = nullptr;
  if (!failed && !extra.is_empty()) extra.to_string(&text);
  global_sid_lock->unlock();
  if (failed) {
    *error = "out of memory";
    return true;
  }
  if (text != nullptr) {
    *error = std::string("the replica applied transactions while the job had "
                         "stopped the applier (") +
             text + "); do not start the applier during a sync job";
    my_free(text);
    return true;
  }
  return false;
}

/**
  Stops the channel's applier if it is running, and restarts it. In between
  START REPLICA by anyone else fails (see sync_check_applier_start()), also
  when the applier was already stopped.
*/
class Applier_pause {
 public:
  Applier_pause(THD *thd, Sync_job *job)
      : m_thd(thd),
        m_job(job),
        m_channel_clause(" FOR CHANNEL " +
                         quote_mysql_string(job->opts.channel)) {}

  ~Applier_pause() { resume(); }

  bool stop(std::string *error) {
    hold(true);
    bool running = false;
    if (read_channel(m_job->opts.channel, nullptr, &running, error))
      return true;
    if (!running) return false;
    if (local_exec(m_thd, "STOP REPLICA SQL_THREAD" + m_channel_clause,
                   error)) {
      *error = "STOP REPLICA failed: " + *error;
      return true;
    }
    m_stopped = true;
    return false;
  }

  void resume() {
    if (m_stopped && !connection_events_loop_aborted()) {
      /* Restart the applier even if the job was killed; remember the kill. */
      if (m_thd->killed) {
        m_job->abort = true;
        m_thd->killed = THD::NOT_KILLED;
      }
      std::string error;
      if (local_exec(m_thd, "START REPLICA SQL_THREAD" + m_channel_clause,
                     &error)) {
        std::string msg = "duckdb sync: START REPLICA failed: " + error;
        LogErr(ERROR_LEVEL, ER_DUCKDB, msg.c_str());
      }
    }
    m_stopped = false;
    hold(false);
  }

 private:
  void hold(bool on) {
    std::lock_guard<std::mutex> guard(applier_hold_mutex);
    if (on) {
      applier_holder = m_thd;
      applier_hold_channel = m_job->opts.channel;
      applier_hold_job = m_job->id;
    } else if (applier_holder == m_thd) {
      applier_holder = nullptr;
    }
  }

  THD *m_thd;
  Sync_job *m_job;
  std::string m_channel_clause;
  bool m_stopped{false};
};

void run_job(THD *thd, Sync_job *job) {
  setup_local_thd(thd);
  std::string msg = "duckdb sync job " + std::to_string(job->id) +
                    " started for " + job->schema;
  LogErr(INFORMATION_LEVEL, ER_DUCKDB, msg.c_str());

  /* Fail the tables still being worked on. */
  auto fail_tables = [&](const std::string &error, bool drop_deltas) {
    for (Table_task &task : job->tasks) {
      if (task.done) continue;
      task.done = true;
      set_entry_state(task.entry, Sync_state::FAILED, error);
      if (drop_deltas) drop_delta_table(job->schema, task.table);
    }
    std::string m = "duckdb sync job " + std::to_string(job->id) +
                    " failed: " + error;
    LogErr(ERROR_LEVEL, ER_DUCKDB, m.c_str());
  };

  std::string error;
  if (connect_source(job, &error)) {
    job->close_connections();
    fail_tables(error, false);
    return;
  }

  /*
    Pause 1: take the snapshot and set up the tables while the applied GTID
    set A is frozen. Anything the applier did before is in the snapshot.
  */
  Applier_pause pause(thd, job);
  if (pause.stop(&error)) {
    job->close_connections();
    fail_tables(error, false);
    return;
  }

  Gtid_set applied_before(global_sid_map, nullptr);
  global_sid_lock->wrlock();
  enum_return_status status =
      applied_before.add_gtid_set(gtid_state->get_executed_gtids());
  global_sid_lock->unlock();
  if (status != RETURN_STATUS_OK) {
    job->close_connections();
    fail_tables("out of memory", false);
    return;
  }

  DBUG_EXECUTE_IF("duckdb_sync_after_stop_replica", {
    const char act[] =
        "now SIGNAL sync_applier_stopped WAIT_FOR sync_continue_snapshot";
    assert(!debug_sync_set_action(thd, STRING_WITH_LEN(act)));
  });

  std::string gtid_text;
  std::shared_ptr<const Gtid_set> snapshot;
  if (take_snapshot(job, &gtid_text, &error) ||
      !(snapshot = parse_gtids(gtid_text, &error))) {
    job->close_connections();
    fail_tables(error, false);
    return;
  }
  for (Table_task &task : job->tasks) {
    if (prepare_table(thd, job, task, &error)) {
      error = job->schema + "." + task.table + ": " + error;
      job->close_connections();
      fail_tables(error, true);
      return;
    }
  }
  if (check_no_concurrent_apply(applied_before, *snapshot, &error)) {
    job->close_connections();
    fail_tables(error, true);
    return;
  }

  /* From here on the applier buffers changes outside G in the deltas. */
  for (Table_task &task : job->tasks) {
    {
      std::unique_lock<std::shared_mutex> guard(registry_lock);
      task.entry->gtids = snapshot;
      task.entry->gtid_text = gtid_text;
      task.entry->delta_seq = 1;
      task.entry->load_aborted = false;
      registry_changed();
    }
    persist_entry(task.entry);
  }
  pause.resume();

  DBUG_EXECUTE_IF("duckdb_sync_after_snapshot", {
    const char act[] =
        "now SIGNAL sync_snapshot_taken WAIT_FOR sync_continue_load";
    assert(!debug_sync_set_action(thd, STRING_WITH_LEN(act)));
  });

  /* Copy while the applier runs. */
  for (Table_task &task : job->tasks) {
    if (job->stopping())
      task.copy_error = "interrupted";
    else
      copy_table(job, task, &task.copy_error);
    if (!task.copy_error.empty()) {
      /* FAILED tables are excluded; the delta is dropped in pause 2. */
      task.done = true;
      set_entry_state(task.entry, Sync_state::FAILED, task.copy_error);
    }
  }
  job->close_connections();

  DBUG_EXECUTE_IF("duckdb_sync_before_merge", {
    const char act[] = "now SIGNAL sync_copied WAIT_FOR sync_continue_merge";
    assert(!debug_sync_set_action(thd, STRING_WITH_LEN(act)));
  });

  /* Pause 2: merge the deltas while the applier is stopped. */
  if (pause.stop(&error)) {
    /* The deltas stay until the next load of the table or a restart. */
    fail_tables(error, false);
    return;
  }
  size_t loaded = 0;
  for (Table_task &task : job->tasks) {
    if (!task.copy_error.empty()) {
      drop_delta_table(job->schema, task.table);
      continue;
    }
    task.done = true;
    std::string merge_error;
    if (task.entry->load_aborted) {
      merge_error = "DDL on the table was replicated during the load; "
                    "run the sync again";
    } else if (merge_table(job, task, &merge_error)) {
      merge_error = "cannot merge the changes made during the load: " +
                    merge_error;
    }
    if (!merge_error.empty()) {
      set_entry_state(task.entry, Sync_state::FAILED, merge_error);
      drop_delta_table(job->schema, task.table);
      continue;
    }
    {
      std::unique_lock<std::shared_mutex> guard(registry_lock);
      task.entry->state = Sync_state::CATCHING_UP;
      task.entry->finished_at = now_string();
      task.entry->last_error.clear();
      registry_changed();
    }
    persist_entry(task.entry);
    loaded++;
  }
  pause.resume();

  msg = "duckdb sync job " + std::to_string(job->id) + " loaded " +
        std::to_string(loaded) + " of " + std::to_string(job->tasks.size()) +
        " tables at source GTID set " + gtid_text;
  LogErr(loaded == job->tasks.size() ? INFORMATION_LEVEL : ERROR_LEVEL,
         ER_DUCKDB, msg.c_str());
}

void job_main(Sync_job *job) {
  my_thread_init();
  THD *thd = create_thd(false, false, false, PSI_NOT_INSTRUMENTED, 0);
  thd->set_command(COM_DAEMON);
  job->thd = thd;
  run_job(thd, job);
  job->thd = nullptr;
  thd->free_items();
  destroy_thd(thd);
  my_thread_end();
  job->finished = true;
}

}  // namespace

/*************************************************************************
  Public API
*************************************************************************/

static Sync_decision filter_decision(THD *thd, const char *db,
                                     const char *table, bool row_event) {
  if (registry_size.load(std::memory_order_acquire) == 0 || db == nullptr ||
      table == nullptr)
    return Sync_decision::NO_OPINION;
  std::string key = registry_key(db, table);
  std::shared_lock<std::shared_mutex> guard(registry_lock);
  auto it = registry.find(key);
  if (it == registry.end()) return Sync_decision::NO_OPINION;
  return decide(thd, *it->second, row_event);
}

Sync_decision sync_filter_decision(THD *thd, const char *db,
                                   const char *table) {
  return filter_decision(thd, db, table, true);
}

Sync_decision sync_filter_decision(THD *thd, Table_ref *tables) {
  if (registry_size.load(std::memory_order_acquire) == 0)
    return Sync_decision::NO_OPINION;
  bool include = false;
  for (Table_ref *t = tables; t != nullptr; t = t->next_global) {
    switch (filter_decision(thd, t->db, t->table_name, false)) {
      case Sync_decision::EXCLUDE:
        return Sync_decision::EXCLUDE;
      case Sync_decision::INCLUDE:
        include = true;
        break;
      case Sync_decision::NO_OPINION:
        break;
    }
  }
  return include ? Sync_decision::INCLUDE : Sync_decision::NO_OPINION;
}

bool sync_is_buffered(THD *thd, const char *db, const char *table) {
  if (buffered_tables.load(std::memory_order_acquire) == 0 ||
      !thd->slave_thread || db == nullptr || table == nullptr)
    return false;
  std::string key = registry_key(db, table);
  std::shared_lock<std::shared_mutex> guard(registry_lock);
  auto it = registry.find(key);
  return it != registry.end() && it->second->state == Sync_state::LOADING &&
         it->second->gtids != nullptr;
}

std::string sync_delta_table(const std::string &db, const std::string &table) {
  return "mysql." + quote_ident(DELTA_PREFIX + db + "#" + table);
}

ulonglong sync_reserve_delta_seq(const std::string &db,
                                 const std::string &table, ulonglong n) {
  std::shared_lock<std::shared_mutex> guard(registry_lock);
  auto it = registry.find(registry_key(db, table));
  return it == registry.end() ? 1 : it->second->delta_seq.fetch_add(n);
}

void sync_registry_init() {
  if (!global_mode_on()) return;

  /* The registry table only exists once sync_table() has been used. */
  std::string error;
  auto exists = duckdb_run(
      "SELECT count(*) FROM information_schema.tables WHERE table_schema = "
      "'mysql' AND table_name = 'duckdb_sync_tables'",
      &error);
  if (!exists) {
    std::string msg = "duckdb sync: cannot read registry: " + error;
    LogErr(ERROR_LEVEL, ER_DUCKDB, msg.c_str());
    return;
  }
  if (exists->GetValue(0, 0).GetValue<int64_t>() == 0) return;
  registry_table_created = true;

  auto res = duckdb_run(
      std::string("SELECT schema_name, table_name, state, snapshot_gtid_set, "
                  "job_id, rows_loaded, rows_expected, started_at, "
                  "finished_at, last_error FROM ") +
          REGISTRY_TABLE,
      &error);
  if (!res) {
    std::string msg = "duckdb sync: cannot read registry: " + error;
    LogErr(ERROR_LEVEL, ER_DUCKDB, msg.c_str());
    return;
  }

  auto str = [&](duckdb::idx_t col, duckdb::idx_t row) {
    duckdb::Value v = res->GetValue(col, row);
    return v.IsNull() ? std::string() : v.ToString();
  };
  auto num = [&](duckdb::idx_t col, duckdb::idx_t row) -> ulonglong {
    duckdb::Value v = res->GetValue(col, row);
    return v.IsNull() ? 0 : v.GetValue<uint64_t>();
  };

  std::vector<Entry_ptr> changed;
  ulonglong max_job_id = 0;
  {
    std::unique_lock<std::shared_mutex> guard(registry_lock);
    registry.clear();
    for (duckdb::idx_t row = 0; row < res->RowCount(); row++) {
      auto e = std::make_shared<Sync_entry>();
      e->db = str(0, row);
      e->table = str(1, row);
      if (state_from_name(str(2, row), &e->state)) e->state = Sync_state::FAILED;
      e->gtid_text = str(3, row);
      e->job_id = num(4, row);
      e->rows_loaded = num(5, row);
      e->rows_expected = num(6, row);
      e->started_at = str(7, row);
      e->finished_at = str(8, row);
      e->last_error = str(9, row);
      max_job_id = std::max(max_job_id, e->job_id);

      if (e->state == Sync_state::LOADING) {
        /* The server stopped during the load: the local data is partial. */
        e->state = Sync_state::FAILED;
        e->last_error = "server restarted while the table was loading";
        changed.push_back(e);
      } else if (e->state == Sync_state::CATCHING_UP) {
        std::string parse_error;
        e->gtids = parse_gtids(e->gtid_text, &parse_error);
        if (!e->gtids) {
          e->state = Sync_state::FAILED;
          e->last_error = parse_error;
          changed.push_back(e);
        }
      }
      registry[registry_key(e->db, e->table)] = e;
    }
    registry_changed();
  }
  next_job_id = max_job_id + 1;
  for (auto &e : changed) persist_entry(e);
  /* No table is loading after a restart; deltas are leftovers. */
  {
    std::shared_lock<std::shared_mutex> guard(registry_lock);
    for (auto &it : registry) drop_delta_table(it.second->db, it.second->table);
  }
  flip_caught_up();
}

void sync_shutdown() {
  std::unique_ptr<Sync_job> job;
  {
    std::lock_guard<std::mutex> guard(job_mutex);
    job = std::move(current_job);
  }
  if (!job) return;
  job->interrupt();
  if (job->thread.joinable()) job->thread.join();
}

bool sync_check_applier_start(THD *thd, const char *channel) {
  std::lock_guard<std::mutex> guard(applier_hold_mutex);
  if (applier_holder == nullptr || applier_holder == thd ||
      my_strcasecmp(system_charset_info, applier_hold_channel.c_str(),
                    channel) != 0)
    return false;
  std::string error = "duckdb sync job " + std::to_string(applier_hold_job) +
                      " has paused the replica applier and restarts it "
                      "itself; see dbms_duckdb.sync_status()";
  my_error(ER_DUCKDB_CLIENT, MYF(0), error.c_str());
  return true;
}

bool sync_table(THD *, const std::string &schema, const std::string &tables,
                const std::string &options, ulonglong *job_id,
                std::string *message) {
  auto fail = [](const std::string &error) {
    my_error(ER_DUCKDB_CLIENT, MYF(0), error.c_str());
    return true;
  };

  if (!global_mode_on()) return fail("duckdb_mode is not ON");
  if (global_gtid_mode.get() != Gtid_mode::ON)
    return fail("gtid_mode must be ON on the replica");

  std::string db = trim(schema);
  if (db.empty() || db.length() > NAME_LEN) return fail("invalid schema name");
  if (is_system_schema_name(db))
    return fail("system schemas cannot be synced");

  std::vector<std::string> names = split(tables, ",");
  if (names.empty()) return fail("no tables given");
  for (const std::string &name : names) {
    if (name.length() > NAME_LEN) return fail("invalid table name " + name);
    if (std::count(names.begin(), names.end(), name) > 1)
      return fail("table " + name + " is listed twice");
  }

  Sync_options opts;
  std::string error;
  if (parse_options(options, &opts, &error)) return fail(error);

  Source_params source;
  if (!opts.adopt && read_channel(opts.channel, &source, nullptr, &error))
    return fail(error);
  if (opts.adopt && read_channel(opts.channel, nullptr, nullptr, &error))
    return fail(error);

  std::lock_guard<std::mutex> job_guard(job_mutex);

  /* Validate every table before registering any. */
  std::vector<bool> recreate(names.size(), opts.refresh);
  {
    std::shared_lock<std::shared_mutex> guard(registry_lock);
    for (size_t i = 0; i < names.size(); i++) {
      auto it = registry.find(registry_key(db, names[i]));
      if (it == registry.end()) continue;
      Sync_state state = it->second->state;
      if (state == Sync_state::LOADING)
        return fail(db + "." + names[i] + " is being loaded");
      if (opts.adopt && state != Sync_state::FAILED)
        return fail(db + "." + names[i] + " is already registered");
      if (state == Sync_state::FAILED) recreate[i] = true;
      else if (!opts.refresh)
        return fail(db + "." + names[i] +
                    " is already synced; use refresh=1 to reload it");
    }
  }
  for (size_t i = 0; i < names.size(); i++) {
    bool exists = false, empty = false;
    if (duckdb_table_state(db, names[i], &exists, &empty, &error))
      return fail(error);
    if (opts.adopt && !exists)
      return fail("cannot adopt " + db + "." + names[i] +
                  ": no such DuckDB table");
    if (!opts.adopt && exists && !empty && !recreate[i])
      return fail("local table " + db + "." + names[i] +
                  " is not empty; use refresh=1 to reload it");
  }

  if (opts.adopt) {
    std::vector<Entry_ptr> adopted;
    {
      std::unique_lock<std::shared_mutex> guard(registry_lock);
      for (const std::string &name : names) {
        auto e = std::make_shared<Sync_entry>();
        e->db = db;
        e->table = name;
        e->state = Sync_state::ACTIVE;
        e->started_at = e->finished_at = now_string();
        registry[registry_key(db, name)] = e;
        adopted.push_back(e);
      }
      registry_changed();
    }
    for (auto &e : adopted) persist_entry(e);
    *job_id = 0;
    *message = "adopted " + std::to_string(names.size()) + " table(s)";
    return false;
  }

  if (current_job) {
    if (!current_job->finished) {
      /* Once no table is LOADING the job is only restarting the applier. */
      std::shared_lock<std::shared_mutex> guard(registry_lock);
      for (const Table_task &task : current_job->tasks)
        if (task.entry->state == Sync_state::LOADING)
          return fail("another sync job is running");
    }
    current_job->thread.join();
    current_job.reset();
  }

  auto job = std::make_unique<Sync_job>();
  job->id = next_job_id++;
  job->schema = db;
  job->opts = opts;
  job->source = source;
  {
    std::unique_lock<std::shared_mutex> guard(registry_lock);
    for (size_t i = 0; i < names.size(); i++) {
      auto e = std::make_shared<Sync_entry>();
      e->db = db;
      e->table = names[i];
      e->state = Sync_state::LOADING;
      e->job_id = job->id;
      e->started_at = now_string();
      registry[registry_key(db, names[i])] = e;
      Table_task task;
      task.table = names[i];
      task.recreate = recreate[i];
      task.entry = e;
      job->tasks.push_back(task);
    }
    registry_changed();
  }
  for (Table_task &task : job->tasks) persist_entry(task.entry);

  Sync_job *raw = job.get();
  current_job = std::move(job);
  try {
    raw->thread = std::thread(job_main, raw);
  } catch (std::exception &ex) {
    for (Table_task &task : raw->tasks)
      set_entry_state(task.entry, Sync_state::FAILED, ex.what());
    current_job.reset();
    return fail(std::string("cannot start sync job: ") + ex.what());
  }

  *job_id = raw->id;
  *message = "started loading " + std::to_string(names.size()) +
             " table(s); see dbms_duckdb.sync_status()";
  return false;
}

void sync_status(std::vector<Sync_status_row> *rows) {
  flip_caught_up();
  std::shared_lock<std::shared_mutex> guard(registry_lock);
  for (auto &it : registry) rows->push_back(to_row(*it.second));
}

bool unsync_table(THD *, const std::string &schema, const std::string &table) {
  std::string db = trim(schema);
  std::string name = trim(table);
  /* A running job drops the delta tables of its tables itself. */
  bool job_owns_delta = false;
  {
    std::lock_guard<std::mutex> job_guard(job_mutex);
    if (current_job && !current_job->finished) {
      for (const Table_task &task : current_job->tasks)
        if (registry_key(current_job->schema, task.table) ==
            registry_key(db, name))
          job_owns_delta = true;
    }
    std::unique_lock<std::shared_mutex> guard(registry_lock);
    auto it = registry.find(registry_key(db, name));
    if (it == registry.end()) {
      my_error(ER_DUCKDB_CLIENT, MYF(0),
               (db + "." + name + " is not synced").c_str());
      return true;
    }
    if (it->second->state == Sync_state::LOADING) {
      my_error(ER_DUCKDB_CLIENT, MYF(0),
               (db + "." + name + " is being loaded").c_str());
      return true;
    }
    db = it->second->db;
    name = it->second->table;
    registry.erase(it);
    registry_changed();
  }
  persist_delete(db, name);
  if (!job_owns_delta) drop_delta_table(db, name);
  return false;
}

}  // namespace myduck
