#!/bin/sh
set -eu
mra_root=$(CDPATH= cd "$(dirname "$0")" && pwd)
cd "$mra_root"
mra_python=${MRA_PYTHON:-python3}
"$mra_python" -B -c 'import sys; sys.exit("Требуется Python 3.9 или новее" if sys.version_info < (3, 9) else 0)'
"$mra_python" -B tools/check.py
"$mra_python" -B -m mra.cli demo
"$mra_python" -B -m mra.cli init
"$mra_python" -B -m mra.cli doctor
printf '\nГотово. Рабочие правила, каноны и карта Ozon подключены.\nСостав: ./mra-cli project\nЗапрос без API: ./mra-cli preview --article 44167 --question "Что это за товар?"\n'
