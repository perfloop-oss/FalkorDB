#!/usr/bin/env bash
# Build the module and one arm-local RDB fixture for the overlap benchmark.
set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "usage: $0 BENCHMARK_BIN overlap|nonoverlap" >&2
    exit 2
fi

benchmark_bin="$1"
shape="$2"
case "$shape" in
    overlap|nonoverlap) ;;
    *)
        echo "unknown fixture shape: $shape" >&2
        exit 2
        ;;
esac
repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

revision="ccb449a9a0efe9b95744284fded766819b23efc9"
deps_archive="/workspace/deps/falkordb-proof/falkordb-deps-${revision}.tar.gz"
deps_sha256="eebc2778a99bb52ef85c7a3c877f679e76ba46594055a2e037e1e694fa0f07c8"

if [ -f "$deps_archive" ]; then
    printf '%s  %s\n' "$deps_sha256" "$deps_archive" | sha256sum --check --status
    tar -xzf "$deps_archive" -C .
else
    git submodule update --init --recursive --depth 1 --jobs 8
fi

redis_root="/workspace/deps/proof-tools/redis-server@8.0.0"
redis_server="${redis_root}/bin/redis-server"
redis_archive="/workspace/deps/falkordb-proof/redis-8.0.0.tar.gz"
redis_sha256="66e327dd3c785107e74d4c57c6e4783733ed59a748595c5d55511178054cc481"

if [ ! -x "$redis_server" ]; then
    rm -rf "${redis_root}/src"
    mkdir -p "${redis_root}/src" "${redis_root}/bin"
    if [ -f "$redis_archive" ]; then
        printf '%s  %s\n' "$redis_sha256" "$redis_archive" | sha256sum --check --status
        tar -xzf "$redis_archive" --strip-components=1 -C "${redis_root}/src"
    else
        git clone --depth 1 --branch 8.0.0 --single-branch \
            https://github.com/redis/redis.git "${redis_root}/src"
    fi
    make -C "${redis_root}/src" -j8 BUILD_TLS=no
    cp "${redis_root}/src/src/redis-server" "$redis_server"
fi

./build.sh VARIANT=perfloop

fixture="${repo_root}/tests/benchmarks/perf_graph_memory_overlap/fixture-${shape}.rdb"
python3 tests/benchmarks/perf_graph_memory_overlap/driver.py prepare \
    --redis-server "$redis_server" \
    --module "${repo_root}/bin/linux-x64-release.perfloop/falkordb.so" \
    --dataset "$fixture" \
    --shape "$shape"

mkdir -p "$(dirname "$benchmark_bin")"
install -m 755 tests/benchmarks/perf_graph_memory_overlap/driver.py "$benchmark_bin"
