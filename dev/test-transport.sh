#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
spark_line="3.3"
if [[ "${1:-}" == --spark=* ]]; then
  spark_line="${1#--spark=}"
  shift
fi

if [[ -z "${PYJAVA_TEST_PYTHON:-}" ]]; then
  if [[ ! -x target/transport-venv/bin/python ]]; then
    "${PYTHON:-python3}" -m venv target/transport-venv
    target/transport-venv/bin/python -m pip install -r python/requirements-transport-test.txt
  fi
  export PYJAVA_TEST_PYTHON="$PWD/target/transport-venv/bin/python"
fi

for pattern in test_transport.py test_barrier.py test_exchange.py test_transfer.py; do
  PYTHONPATH="$PWD/python" "$PYJAVA_TEST_PYTHON" -m unittest discover \
    -s python/pyjava/tests -p "$pattern" -v
done

build_args=()
case "$spark_line" in
  3.3)
    build_args=(-Dspark.bigversion=3.3 -Dspark.version=3.3.0
      -Dscala.binary.version=2.12 -Dscala.version=2.12.15
      -Darrow.version=7.0.0 -Djackson.version=2.13.4 -Dhadoop-client-version=3.3.2
      -Dos-lib.version=0.2.9)
    ;;
  4.1) build_args=(-Dpyjava.build.directory=target/spark41) ;;
  *) echo "Supported test targets: --spark=3.3 or --spark=4.1" >&2; exit 2 ;;
esac

suites="tech.mlsql.arrow.python.PartitionLifecycleSpec,tech.mlsql.arrow.python.IdleWorkerPoolSpec,tech.mlsql.arrow.python.PythonWorkerFactorySpec,tech.mlsql.arrow.python.ArrowTransportSpec,tech.mlsql.arrow.python.SparkSocketTransportSpec,tech.mlsql.arrow.python.SparkSocketServeSpec,tech.mlsql.test.ArrowUtilsSpec,tech.mlsql.test.ArrowConvertersSpec,tech.mlsql.test.WowRowEncoderSpec,tech.mlsql.test.SparkUtilsSpec"
jvm_args="${PYJAVA_TEST_JVM_ARGS:---add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED}"
"${MVN:-mvn}" -B "${build_args[@]}" "-Dtest.jvm.args=$jvm_args" \
  "-Dsuites=$suites" "$@" test
