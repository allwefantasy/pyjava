#!/usr/bin/env bash
# Copy the Python data-plane load onto remoteservice and run it once.
# Does not restart Infinity SQL / Byzer, and does not start a Ray cluster.
set -euo pipefail

cd "$(dirname "$0")/.."
host="${PYJAVA_LOADTEST_HOST:-remoteservice}"
dest="${PYJAVA_LOADTEST_DIR:-pyjava-loadtest}"
workers="${PYJAVA_LOADTEST_WORKERS:-4}"
requests="${PYJAVA_LOADTEST_REQUESTS:-40}"
rows="${PYJAVA_LOADTEST_ROWS:-20000}"

ssh -o BatchMode=yes "$host" "mkdir -p ~/${dest}/python"

rsync -az --delete --exclude '__pycache__' \
  python/pyjava/ "${host}:${dest}/python/pyjava/"

ssh -o BatchMode=yes "$host" "PYTHONPATH=\$HOME/${dest}/python python3 -m pyjava.loadtest \
    --scenario all --workers ${workers} --requests ${requests} --rows ${rows} \
    --output \$HOME/${dest}/report.json"

mkdir -p target/loadtest
scp -q "${host}:${dest}/report.json" target/loadtest/remoteservice-report.json
echo "wrote target/loadtest/remoteservice-report.json"
