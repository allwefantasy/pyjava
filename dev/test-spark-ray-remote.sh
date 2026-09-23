#!/usr/bin/env bash
# Run isolated real Spark + Ray processes; reuses runtime jars read-only.
# First build Scala 2.12/Spark 3.3 with dev/test-transport.sh --spark=3.3.
set -euo pipefail
cd "$(dirname "$0")/.."
host="${PYJAVA_LOADTEST_HOST:-remoteservice}"
rows="${PYJAVA_INTEGRATION_ROWS:-20000}"
width="${PYJAVA_INTEGRATION_WIDTH:-256}"
mode="${PYJAVA_INTEGRATION_MODE:-batches}"
passes="${PYJAVA_INTEGRATION_PASSES:-2}"
[[ "$rows" =~ ^[0-9]+$ && "$width" =~ ^[0-9]+$ && "$passes" =~ ^[0-9]+$ && "$mode" =~ ^(rows|batches)$ ]] || exit 2
test -f target/test-classes/tech/mlsql/arrow/python/SparkRayIntegration.class
python3 - <<'PY'
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
items = {}
for root in (Path('target/classes'), Path('target/test-classes')):
    for p in root.rglob('*'):
        if p.is_file():
            items[str(p.relative_to(root))] = p
with ZipFile('target/pyjava-validation.jar', 'w', ZIP_DEFLATED) as z:
    for name, p in items.items():
        z.write(p, name)
PY
dest=$(ssh -o BatchMode=yes "$host" 'mktemp -d "$HOME/pyjava-validation.XXXXXXXX"')
[[ "$dest" =~ ^/[a-zA-Z0-9_./-]+$ ]] || exit 2
echo "Isolated workspace: $dest"
rsync -az --exclude '__pycache__' python/pyjava "${host}:${dest}/python/"
scp -q target/pyjava-validation.jar src/test/resources/ray_exchange_driver.py dev/monitor-validation.py "${host}:${dest}/"
remote_exit=0
ssh -o BatchMode=yes "$host" "bash -s -- '$dest' '$rows' '$width' '$mode' '$passes'" <<'REMOTE' || remote_exit=$?
set -euo pipefail
dest="$1"
runtime="${PYJAVA_SPARK33_RUNTIME:-$HOME/softwares/infinity-sql-all-in-one-linux-amd64-3.3.0-2.4.8}"
test -x "$runtime/jdk8/bin/java"
test -f "$runtime/spark/spark-core_2.12-3.3.0.jar"
export PYTHONPATH="$dest/python"
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1
export PYJAVA_SPOOL_MAX_BYTES=2147483648
export PYJAVA_REPLAY_TTL_SECONDS=600
mkdir -p "$dest/run"
cd "$dest"
nice -n 10 python3 monitor-validation.py "$dest/run" \
  "$runtime/jdk8/bin/java" -Xmx1g -XX:MaxDirectMemorySize=512m \
  -cp "$dest/pyjava-validation.jar:$runtime/spark/*:$runtime/libs/*" \
  tech.mlsql.arrow.python.SparkRayIntegration python3 "$dest/ray_exchange_driver.py" \
  "$dest/run" "$2" "$3" "$4" "$5"
cat "$dest/run/report.json"
REMOTE
local_dir="target/engine-validation/$(basename "$dest")"
mkdir -p "$local_dir"
rsync -az --exclude ray-runtime --exclude '*.arrow' "${host}:${dest}/run/" "$local_dir/"
echo "Evidence: $local_dir; remote workspace: $dest"
exit "$remote_exit"
