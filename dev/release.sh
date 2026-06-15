#!/usr/bin/env bash

set -euo pipefail

version=${1:-0.3.3}

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
MVN="${MVN:-mvn}"

cd "${ROOT_DIR}"

quoteVersion=$(grep "__version__" python/pyjava/version.py | awk -F'=' '{print $2}' | xargs)

if [[ "${version}" != "${quoteVersion}" ]]; then
  echo "version[${quoteVersion}] in python/pyjava/version.py does not match version[${version}] you specified"
  exit 1
fi

if [[ ! -d ".repo" ]]; then
  echo "Make sure this script is executed in the root directory of pyjava"
  exit 1
fi

run_publish() {
  local label="$1"
  shift

  echo "==> Publishing ${label}"
  "${MVN}" clean deploy \
    -DskipTests=true \
    -Pdisable-java8-doclint \
    -Prelease-sign-artifacts \
    "$@"
}

run_publish "Spark 2.4 / Scala 2.11.8 artifacts (*_2.11)" -Pscala-2.11
run_publish "Spark 4.1 / default Scala artifacts (*_2.13)"

echo "==> Publishing pyjava pip package"
cd python
rm -rf dist
pip uninstall -y pyjava
python setup.py sdist bdist_wheel
pip install "dist/pyjava-${version}-py3-none-any.whl"
twine upload dist/*
