"""Read private installation settings containing credential paths, never key values."""

import json
from pathlib import Path


def read_runtime(path):
    path = Path(path)
    if not path.is_file():
        return {}, ""
    try:
        if path.stat().st_size > 65536:
            raise ValueError
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or set(data) - {"schema_version", "provider", "model", "key_file", "operator"}:
            raise ValueError
        if data.get("schema_version") != 1 or data.get("provider") not in {"openai", "openrouter"}:
            raise ValueError
        if any(not isinstance(data.get(key), str) or not data[key].strip() for key in ("model", "key_file", "operator")):
            raise ValueError
        return data, ""
    except (OSError, UnicodeError, ValueError):
        return {"provider": "invalid"}, "Некорректный data/runtime.json; запустите настройку заново. Провайдер автоматически не заменялся."
