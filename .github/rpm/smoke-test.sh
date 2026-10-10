#!/bin/sh
# Installs an AliSQL RPM in a bare EL container (no systemd), starts the server the way
# alisql.service does, checks it and DuckDB, then removes the package.
#   docker run --rm -v <dir>:/rpm -v <this dir>:/t oraclelinux:9 sh /t/smoke-test.sh /rpm/<file>.rpm
set -eux
rpm_file=$1
if command -v dnf >/dev/null; then dnf install -y -q "$rpm_file"; else yum install -y -q "$rpm_file"; fi
rpm -q alisql
# systemd's RuntimeDirectory=alisql, then ExecStartPre and ExecStart as the service user.
install -d -o mysql -g mysql /run/alisql
su -s /bin/sh mysql -c /usr/libexec/alisql-prepare-db-dir
su -s /bin/sh mysql -c '/opt/alisql/bin/mysqld --defaults-file=/etc/alisql/my.cnf --duckdb_mode=ON &'
for i in $(seq 120); do /opt/alisql/bin/mysqladmin ping >/dev/null 2>&1 && break; sleep 1; done
# The client finds the socket through /opt/alisql/my.cnf.
pw=$(sed -n 's/.*temporary password is generated for root@localhost: //p' /var/log/alisql/mysqld.log)
/opt/alisql/bin/mysql -uroot -p"$pw" --connect-expired-password \
  -e "ALTER USER root@localhost IDENTIFIED BY 'smoke-test'"
q() { /opt/alisql/bin/mysql -uroot -psmoke-test -N -e "$1"; }
q "SELECT VERSION()" | grep -E '^8\.0\.[0-9]+-g[0-9a-f]{7}$'
q "CALL dbms_duckdb.query('PRAGMA platform')" | grep -E 'linux_(amd64|arm64)'
q "CREATE DATABASE smoke; CREATE TABLE smoke.t (a INT PRIMARY KEY, b VARCHAR(8)) ENGINE=DuckDB;
   INSERT INTO smoke.t VALUES (1, 'x'), (2, 'y'); SELECT SUM(a) FROM smoke.t" | grep -x 3
/opt/alisql/bin/mysqladmin -uroot -psmoke-test shutdown
for i in $(seq 60); do [ -e /run/alisql/mysqld.pid ] || break; sleep 1; done
rpm -e alisql
test ! -e /opt/alisql/bin/mysqld
echo "smoke test passed: $(basename "$rpm_file")"
