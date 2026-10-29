"""Runtime configuration. Paths do not depend on the author's Mac."""

from __future__ import annotations

import os
from pathlib import Path
from .runtime import read_runtime

VERSION_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.environ.get("MRA_DATA_DIR", str(VERSION_ROOT / "data"))).expanduser()
DB_PATH = Path(os.environ.get("MRA_DB_PATH", str(DATA_DIR / "mra.sqlite3"))).expanduser()
ACCOUNTS_FILE = Path(os.environ.get("MRA_ACCOUNTS_FILE", str(DATA_DIR / "accounts.json" if (DATA_DIR / "accounts.json").is_file()
    else VERSION_ROOT / "examples/accounts.example.json"))).expanduser()
KNOWLEDGE_DIR = VERSION_ROOT / "knowledge"
RULES_FILE = Path(os.environ.get("MRA_RULES_FILE", str(KNOWLEDGE_DIR / "ПРАВИЛА ОТВЕТОВ.md"))).expanduser()
RUNTIME, RUNTIME_ERROR = ({}, "") if os.environ.get("MRA_IGNORE_RUNTIME") == "1" else read_runtime(DATA_DIR / "runtime.json")
PROVIDER = os.environ.get("MRA_PROVIDER", RUNTIME.get("provider", "openai")).strip().lower()
MODEL = os.environ.get("MRA_MODEL", RUNTIME.get("model") if RUNTIME.get("provider") == PROVIDER and RUNTIME.get("model")
    else "openai/gpt-6.1-sol" if PROVIDER == "openrouter" else "gpt-6.1-sol")
RUNTIME_KEY_FILE = RUNTIME.get("key_file", "") if RUNTIME.get("provider") == PROVIDER else ""
RUNTIME_OPERATOR = RUNTIME.get("operator", "")
REASONING_EFFORT = os.environ.get("MRA_REASONING_EFFORT", "high")
OPENAI_KEY_FILE_ENV = "MRA_OPENAI_KEY_FILE"
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
OPENROUTER_CHAT_URL = "https://openrouter.ai/api/v1/chat/completions"
DISABLE_LLM = os.environ.get("MRA_DISABLE_LLM") == "1"


def positive_int(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError as exc:
        raise ValueError(f"{name}: требуется целое положительное число") from exc
    if value <= 0:
        raise ValueError(f"{name}: требуется целое положительное число")
    return value
