#!/usr/bin/env bash
# Focused Ray actor checks for the shared snapshot pool.
# Uses the isolated transport venv only. Does not call ray stop or uninstall packages.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ ! -x target/transport-venv/bin/python ]]; then
  echo "missing target/transport-venv; run dev/test-transport.sh once or create that venv" >&2
  exit 2
fi
export PYTHONPATH="$PWD/python"
exec target/transport-venv/bin/python -m unittest pyjava.tests.test_ray_snapshot -v
