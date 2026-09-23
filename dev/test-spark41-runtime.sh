#!/usr/bin/env bash
# Fallback when Maven Central is unavailable: compile against an installed Spark 4.1 runtime.
# This validates main sources and the real Spark/Ray executable, not the Maven test suite.
set -euo pipefail
cd "$(dirname "$0")/.."
host="${PYJAVA_LOADTEST_HOST:-remoteservice}"
dest=$(ssh -o BatchMode=yes "$host" 'mktemp -d "$HOME/pyjava-spark41.XXXXXXXX"')
[[ "$dest" =~ ^/[a-zA-Z0-9_./-]+$ ]] || exit 2
echo "Isolated workspace: $dest"
ssh -o BatchMode=yes "$host" "mkdir -p '$dest/src/main' '$dest/deps' '$dest/run'"
rsync -az --exclude '__pycache__' python/pyjava "${host}:${dest}/python/"
rsync -az src/main/ "${host}:${dest}/src/main/"
scp -q src/test/java/tech/mlsql/arrow/python/SparkRayIntegration.scala \
  src/test/resources/ray_exchange_driver.py dev/monitor-validation.py "${host}:${dest}/"
scp -q "$HOME/.m2/repository/com/lihaoyi/os-lib_2.13/0.7.8/os-lib_2.13-0.7.8.jar" \
  "$HOME/.m2/repository/com/lihaoyi/geny_2.13/0.6.10/geny_2.13-0.6.10.jar" "${host}:${dest}/deps/"
remote_exit=0
ssh -o BatchMode=yes "$host" "bash -s -- '$dest'" <<'REMOTE' || remote_exit=$?
set -euo pipefail
cd "$1"
export PYTHONPATH="$PWD/python"
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1
export PYJAVA_REPLAY_TTL_SECONDS=600 PYJAVA_SPOOL_MAX_BYTES=2147483648
python3 - <<'PY'
import os, subprocess
from pathlib import Path
root=Path.cwd()
runtime=Path(os.environ.get('PYJAVA_SPARK41_RUNTIME', str(Path.home()/'softwares/infinity-sql-spark412-2.4.10-codex')))
java=str(runtime/'jdk17/bin/java')
assert (runtime/'spark/spark-core_2.13-4.1.2.jar').is_file()
cp=os.pathsep.join(str(p) for d in (runtime/'spark', root/'deps') for p in d.glob('*.jar'))
classes=root/'classes'
classes.mkdir()
sources=[str(p) for p in (root/'src/main').rglob('*.scala')]+[str(root/'SparkRayIntegration.scala')]
with (root/'run/compile.log').open('w') as log:
    subprocess.run([java,'-cp',cp,'scala.tools.nsc.Main','-classpath',cp,'-d',str(classes)]+sources,
                   stdout=log,stderr=subprocess.STDOUT,check=True,timeout=180)
cmd=['python3','monitor-validation.py',str(root/'run'),java,'-Xmx1g','-XX:MaxDirectMemorySize=512m',
     '--add-opens=java.base/java.nio=ALL-UNNAMED','--add-opens=java.base/sun.nio.ch=ALL-UNNAMED',
     '--add-opens=java.base/sun.util.calendar=ALL-UNNAMED','-cp',str(classes)+os.pathsep+cp,
     'tech.mlsql.arrow.python.SparkRayIntegration','python3',str(root/'ray_exchange_driver.py'),
     str(root/'run'),'20000','256','batches']
subprocess.run(cmd,check=True,timeout=300)
print((root/'run/report.json').read_text())
PY
REMOTE
local_dir="target/engine-validation/$(basename "$dest")"
mkdir -p "$local_dir"
rsync -az --exclude ray-runtime "${host}:${dest}/run/" "$local_dir/"
echo "Evidence: $local_dir"
exit "$remote_exit"
