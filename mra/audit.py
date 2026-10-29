"""Business audit payloads: keep source data and remove authentication secrets."""

from __future__ import annotations

import re

_SECRET_FIELDS = {"authorization", "api_key", "apikey", "access_token", "refresh_token", "password",
                  "client_secret", "cookie", "set_cookie", "secret", "token"}
_KEY_PATTERN = re.compile(r"\b(?:sk-(?:proj-|or-v1-)?[A-Za-z0-9_-]{20,}|github_pat_[A-Za-z0-9_]{20,}|gh[pousr]_[A-Za-z0-9]{20,})")


def redact(value, secrets=()):
    if isinstance(value, dict):
        return {str(key): "[REDACTED]" if str(key).lower().replace('-', '_') in _SECRET_FIELDS
                else redact(item, secrets) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact(item, secrets) for item in value]
    if isinstance(value, str):
        for secret in secrets:
            if secret:
                value = value.replace(secret, "[REDACTED]")
        return _KEY_PATTERN.sub("[REDACTED]", value)
    return value
