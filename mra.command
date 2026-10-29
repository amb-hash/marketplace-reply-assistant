#!/bin/sh
set -eu
mra_root=$(CDPATH= cd "$(dirname "$0")" && pwd)
cd "$mra_root"
if [ "$#" -eq 0 ]; then set -- review-session --live; fi
exec "${MRA_PYTHON:-python3}" -B -m mra.cli "$@"
