"""Persistent method budgets and account leases shared by every marketplace client."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import math
import re
import time
import uuid


class RateLimitWait(ValueError):
    def __init__(self, seconds, reason, now):
        self.seconds, self.reason = math.ceil(seconds), reason
        self.until = datetime.fromtimestamp(now + seconds, timezone.utc).isoformat()
        super().__init__(f"пауза API до {self.until}: {reason}; сетевой запрос не выполнялся")


def retry_seconds(value, *, clock=time.time, fallback=60):
    try:
        seconds = float(value)
    except (ValueError, TypeError):
        try:
            parsed = parsedate_to_datetime(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            seconds = parsed.timestamp() - clock()
        except (ValueError, TypeError, OverflowError, IndexError):
            return fallback
    return max(fallback, seconds) if math.isfinite(seconds) and seconds <= 31_536_000 else fallback


@dataclass(frozen=True)
class Policy:
    family: str
    interval: float
    requests: int
    window: float


def policy(account, method, path):
    mp = account["marketplace"]
    if mp == "wildberries":
        if path == "/content/v2/get/cards/list":
            return Policy("catalog", 1.1, 50, 60)  # Local ceiling, below content quota.
        if path.startswith("/api/v1/"):
            if account.get("token_type") in {"personal", "service", "base_with_secret"}:
                return Policy("questions_reviews", 0.4, 3, 1)
            return Policy("questions_reviews", 720.1, 5, 3600)
    elif mp == "ozon":
        if re.fullmatch(r"/v[123]/(?:product|question|review)/[a-z/]+", path):
            return Policy(path, 1.1, 50, 60)  # MRA ceiling, not a claimed Ozon method quota.
    elif mp == "yandex_market":
        family = re.sub(r"/businesses/[1-9][0-9]*", "/businesses/{id}", path)
        if path.endswith("/offer-mappings"):
            return Policy(family, 1.1, 50, 60)
        if path == "/v2/campaigns" or re.fullmatch(r"/v[12]/businesses/[1-9][0-9]*/(?:offer-mappings|goods-questions(?:/answers|/update)?|goods-feedback(?:/comments(?:/update)?)?)", path):
            writing = path.endswith("/update")
            return Policy(family, 7.3 if writing else 3.7, 500 if writing else 1000, 3600)
    raise ValueError("для метода не определена политика лимитера")


class AccountLimiter:
    def __init__(self, store, account, *, clock=time.time):
        self.store, self.account, self.clock = store, account, clock
        self.scope = account["marketplace"] + ":" + account["account_id"]
        # Non-secret IDs unify aliases of the same seller in this installation.
        if account["marketplace"] == "ozon":
            from .catalog_api import runtime_value
            identity = runtime_value(account, "client_id")
        else:
            identity = str(account.get("business_id") or "") if account["marketplace"] == "yandex_market" else ""
        if identity:
            self.scope = account["marketplace"] + ":seller:" + identity

    def acquire(self, method, path):
        current = self.clock()
        selected = policy(self.account, method, path)
        # One in-flight request per seller; separate route and shared rolling budgets.
        global_policy = Policy("global", 0.4 if self.account["marketplace"] == "wildberries" else 1.1, 50, 60)
        gates = [(self.scope, global_policy), (self.scope + ":" + selected.family, selected)]
        reason, wait = "интервал метода", 0.0
        with self.store.transaction():
            for scope, gate in gates:
                self.store._db.execute("INSERT OR IGNORE INTO api_limits(scope,next_at,lease_until,lease_token) VALUES(?,0,0,'')", (scope,))
                row = self.store._db.execute("SELECT next_at,lease_until,blocked_until FROM api_limits WHERE scope=?", (scope,)).fetchone()
                if row[1] > current:
                    raise RateLimitWait(row[1] - current, "запрос другого процесса ещё выполняется", current)
                if row[2] > current:
                    raise RateLimitWait(row[2] - current, "пауза, назначенная после ответа площадки", current)
                wait = max(wait, row[0] - current)
                history = self.store._db.execute("SELECT created_at FROM api_limit_events WHERE scope=? AND created_at>? ORDER BY created_at", (scope, current - gate.window)).fetchall()
                if len(history) >= gate.requests:
                    available = history[len(history) - gate.requests][0] + gate.window + 0.01
                    if available - current > wait:
                        wait, reason = available - current, "исчерпана квота за скользящее окно"
            wait = max(0.0, wait)
            if wait > 30:
                raise RateLimitWait(wait, reason, current)
            token = uuid.uuid4().hex
            for scope, gate in gates:
                self.store._db.execute("UPDATE api_limits SET next_at=?,lease_until=?,lease_token=? WHERE scope=?", (current + wait + gate.interval, current + wait + 120, token, scope))
                self.store._db.execute("INSERT INTO api_limit_events(scope,created_at) VALUES(?,?)", (scope, current + wait))
            self.store._db.execute("DELETE FROM api_limit_events WHERE created_at<?", (current - 86400,))
            self.store.audit_event(None, "api.rate_reserved", {"scope": self.scope, "family": selected.family, "method": method,
                "wait_seconds": wait, "requests": selected.requests, "window_seconds": selected.window})
        return token, wait

    def release(self, token, *, status=None, headers=None, cooldown=0):
        now = self.clock()
        headers = {str(k).lower(): str(v) for k, v in (headers or {}).items()}
        limited = status == 429 or (status == 420 and self.account["marketplace"] == "yandex_market")
        with self.store.transaction():
            rows = self.store._db.execute("SELECT scope,streak FROM api_limits WHERE lease_token=?", (token,)).fetchall()
            for scope, streak in rows:
                pause, next_streak = max(0, cooldown), streak
                if limited:
                    next_streak += 1
                    pause = max(pause, min(900 * 2 ** min(next_streak - 1, 4), 14400))
                    for name in ("retry-after", "x-ratelimit-retry"):
                        pause = max(pause, retry_seconds(headers.get(name), clock=self.clock, fallback=0))
                elif status is not None and 200 <= status < 300:
                    next_streak = 0
                # Exhaustion is honored even on HTTP 200 before another call is made.
                remaining = headers.get("x-ratelimit-resource-remaining")
                if remaining is not None and retry_seconds(remaining, fallback=0) == 0:
                    pause = max(pause, retry_seconds(headers.get("x-ratelimit-resource-until"), clock=self.clock, fallback=3600))
                if headers.get("x-ratelimit-remaining") == "0":
                    pause = max(pause, retry_seconds(headers.get("x-ratelimit-reset"), clock=self.clock, fallback=60))
                self.store._db.execute("UPDATE api_limits SET lease_until=0,lease_token='',blocked_until=max(blocked_until,?),streak=?,last_status=? WHERE scope=? AND lease_token=?", (now + pause if pause else 0, next_streak, status, scope, token))
            if rows:
                self.store.audit_event(None, "api.rate_completed", {"scope": self.scope, "http_status": status,
                    "limited": limited, "cooldown_seconds": pause, "consecutive_limits": next_streak,
                    "server_wait_seconds": max(retry_seconds(headers.get(k), clock=self.clock, fallback=0) for k in ("retry-after", "x-ratelimit-retry", "x-ratelimit-resource-until"))})

    def snapshot(self):
        return [{"scope": row[0], "next_at": row[1], "lease_until": row[2], "blocked_until": row[3], "last_status": row[4], "consecutive_limits": row[5]}
                for row in self.store._db.execute("SELECT scope,next_at,lease_until,blocked_until,last_status,streak FROM api_limits WHERE scope=? OR scope LIKE ?", (self.scope, self.scope + ":%"))]
