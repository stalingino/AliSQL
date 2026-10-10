# AliSQL as one RPM, repackaged from the release tarball the build workflow makes.
# The tree goes to /opt/alisql unchanged (its binaries find the bundled OpenSSL and ncurses
# through RPATH $ORIGIN/../lib/private), so the package needs no distro MySQL libraries and
# can sit next to a distro MySQL or MariaDB.
#
#   rpmbuild -bb alisql.spec --define "_sourcedir <dir with the tarball and this directory's files>" \
#     --define "alisql_version 8.0.44" --define "alisql_release <yyyymmdd>.g<sha>" \
#     --define "alisql_tarball alisql-8.0.44-g<sha>-linux-glibc2.17-x86_64.tar.xz"

# Ship the binaries as built: no stripping, debuginfo or RPATH checks.
%global debug_package %{nil}
%global __os_install_post %{nil}
%global _build_id_links none
# xz keeps the package installable by EL7's rpm 4.11.
%global _binary_payload w6.xzdio
%global alisql_topdir %(basename %{alisql_tarball} .tar.xz)

Name:           alisql
Version:        %{alisql_version}
Release:        %{alisql_release}
Summary:        AliSQL: MySQL 8.0 server and client with the DuckDB storage engine
License:        GPLv2
URL:            https://github.com/alibaba/AliSQL
Source0:        %{alisql_tarball}
Source1:        alisql.service
Source2:        my.cnf
Source3:        alisql-prepare-db-dir
# The bundled libraries must not satisfy, or be satisfied by, other packages.
AutoReqProv:    no
Requires:       libaio
Requires(pre):  shadow-utils

%description
AliSQL, Alibaba's MySQL 8.0 branch with DuckDB as an analytical storage engine,
installed in /opt/alisql with its own OpenSSL and ncurses. Configuration is in
/etc/alisql/my.cnf, data in /var/lib/alisql, and the server runs as the systemd
service alisql.

%prep
%setup -q -n %{alisql_topdir}

%build

%install
mkdir -p %{buildroot}/opt/alisql
cp -a . %{buildroot}/opt/alisql/
# Clients read SYSCONFDIR/my.cnf (SYSCONFDIR is /opt/alisql) after /etc/my.cnf, so this
# points them at the server's socket. The server itself reads only /etc/alisql/my.cnf.
printf '!include /etc/alisql/my.cnf\n' > %{buildroot}/opt/alisql/my.cnf
install -D -m 0644 %{SOURCE2} %{buildroot}/etc/alisql/my.cnf
install -D -m 0644 %{SOURCE1} %{buildroot}/usr/lib/systemd/system/alisql.service
install -D -m 0755 %{SOURCE3} %{buildroot}/usr/libexec/alisql-prepare-db-dir
install -d -m 0750 %{buildroot}/var/lib/alisql %{buildroot}/var/log/alisql

%pre
getent group mysql >/dev/null || groupadd -r mysql
getent passwd mysql >/dev/null || \
  useradd -r -g mysql -d /var/lib/alisql -s /sbin/nologin -c "AliSQL server" mysql
exit 0

%post
systemctl daemon-reload >/dev/null 2>&1 || :

%preun
if [ "$1" -eq 0 ]; then
  systemctl --no-reload disable --now alisql.service >/dev/null 2>&1 || :
fi

%postun
systemctl daemon-reload >/dev/null 2>&1 || :
if [ "$1" -ge 1 ]; then
  systemctl try-restart alisql.service >/dev/null 2>&1 || :
fi

%files
%defattr(-,root,root,-)
/opt/alisql
%dir /etc/alisql
%config(noreplace) /etc/alisql/my.cnf
/usr/lib/systemd/system/alisql.service
/usr/libexec/alisql-prepare-db-dir
%attr(0750,mysql,mysql) %dir /var/lib/alisql
%attr(0750,mysql,mysql) %dir /var/log/alisql
