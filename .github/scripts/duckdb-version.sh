#!/usr/bin/env bash
# Prints the version the embedded DuckDB reports (SELECT version()), derived from the
# describe that cmake/duckdb.cmake pins (DUCKDB_GIT_DESCRIBE) the way DuckDB's CMake
# does: vX.Y.Z on a tag, else vX.Y.(Z+1)-devN. Usage: duckdb-version.sh [source dir]
set -euo pipefail

describe=$(sed -n 's/^SET(DUCKDB_GIT_DESCRIBE "\(.*\)")$/\1/p' "${1:-.}/cmake/duckdb.cmake")
if [[ ! $describe =~ ^v([0-9]+)\.([0-9]+)\.([0-9]+)-([0-9]+)-g[0-9a-f]+$ ]]; then
  echo "cmake/duckdb.cmake: unexpected DUCKDB_GIT_DESCRIBE '${describe}'" >&2
  exit 1
fi
major=${BASH_REMATCH[1]} minor=${BASH_REMATCH[2]} patch=${BASH_REMATCH[3]} dev=${BASH_REMATCH[4]}
if [[ $dev == 0 ]]; then
  echo "v${major}.${minor}.${patch}"
else
  echo "v${major}.${minor}.$((patch + 1))-dev${dev}"
fi
