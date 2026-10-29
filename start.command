#!/bin/sh
set -eu
mra_start_root=$(CDPATH= cd "$(dirname "$0")" && pwd)
cd "$mra_start_root"
exec "${MRA_PYTHON:-python3}" -B tools/start.py "$@"
