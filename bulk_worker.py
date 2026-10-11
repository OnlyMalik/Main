"""
═══════════════════════════════════════════════════════════
 🔧 Bulk worker pool — in-process concurrency, pacing, progress
 👨‍💻 Developer: MalikAbubakker
 📁 File Path: src/bulk_worker.py
═══════════════════════════════════════════════════════════
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

from config import settings
from src.safe_errors import safe_error


# ═══════════════════════════════════════════════════════════
# SIGNUP LIMITER (global)
# ═══════════════════════════════════════════════════════════

class SignupLimiter:
    def __init__(
        self,
        per_minute: int = 0,
        per_hour: int = 400,
        burst: int = 3,
        base_cooldown_ms: int = 60_000,
        max_cooldown_ms: int = 900_000,
    ) -> None:
        self.per_minute = per_minute
        self.per_hour = per_hour
        self.burst = max(1, burst)
        self.attempts: list[float] = []
        self.cooldown_until = 0.0
        self.current_cooldown = base_cooldown_ms / 1000.0
        self.base_cooldown = self.current_cooldown
        self.max_cooldown = max_cooldown_ms / 1000.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                if now < self.cooldown_until:
                    await asyncio.sleep(min(self.cooldown_until - now, 1.0))
                    continue

                self.attempts = [t for t in self.attempts if now - t < 3600]

                last_10 = [t for t in self.attempts if now - t < 10]
                if len(last_10) >= self.burst:
                    await asyncio.sleep(max(0.05, 10 - (now - last_10[0]) + 0.05))
                    continue

                if self.per_minute:
                    last_60 = [t for t in self.attempts if now - t < 60]
                    if len(last_60) >= self.per_minute:
                        await asyncio.sleep(max(0.1, 60 - (now - last_60[0]) + 0.1))
                        continue

                if len(self.attempts) >= self.per_hour:
                    await asyncio.sleep(max(0.2, 3600 - (now - self.attempts[0]) + 0.2))
                    continue

                self.attempts.append(now)
                return

    def on_rate_limit(self) -> None:
        self.cooldown_until = time.monotonic() + self.current_cooldown
        self.current_cooldown = min(self.current_cooldown * 2, self.max_cooldown)
        self.attempts.clear()

    def on_success(self) -> None:
        self.current_cooldown = max(self.current_cooldown / 2, self.base_cooldown)


signup_limiter = SignupLimiter(
    per_minute=settings.BULK_SIGNUPS_PER_MINUTE,
    per_hour=settings.BULK_SIGNUPS_PER_HOUR,
    burst=settings.BULK_SIGNUP_BURST,
)


# ═══════════════════════════════════════════════════════════
# STAGE-AWARE ERRORS
# ═══════════════════════════════════════════════════════════

class BulkStageError(Exception):
    """Raised inside a task to record which step failed."""
    def __init__(self, stage: str, message: str,
                 email: Optional[str] = None,
                 password: Optional[str] = None) -> None:
        super().__init__(message)
        self.stage = stage
        self.email = email
        self.password = password


_RATE_LIMIT_TOKENS = ("too_many_attempts", "too many attempts",
                      "rate limit", "quota exceeded", " 429", "http 429")
_NETWORK_TOKENS = ("timeout", "timed out", "connection", "network",
                   " 500", " 502", " 503", " 504")


def classify_error(exc: BaseException) -> str:
    text = str(exc).lower()
    if any(t in text for t in _RATE_LIMIT_TOKENS):
        return "rate_limit"
    if any(t in text for t in _NETWORK_TOKENS):
        return "network"
    if "weak_password" in text or "weak password" in text:
        return "weak_password"
    if "email_exists" in text or "already registered" in text or "already exists" in text:
        return "exists"
    if "invalid_email" in text or "invalid email" in text:
        return "invalid"
    if any(t in text for t in ("email_not_found", "invalid_password",
                                "invalid_login", "user_disabled")):
        return "credentials"
    return "other"


_MESSAGES = {
    "rate_limit":    "Service rate limit reached. Try a smaller batch later.",
    "network":       "Service connection failed.",
    "weak_password": "Password rejected as too weak.",
    "exists":        "Email already registered.",
    "invalid":       "Email format rejected.",
    "credentials":   "Login credentials rejected or account disabled.",
    "other":         "Game rejected or could not confirm this update.",
}


def describe_error(exc: BaseException) -> str:
    stage = getattr(exc, "stage", None) or "Account operation"
    return f"{stage}: {_MESSAGES[classify_error(exc)]}"


# ═══════════════════════════════════════════════════════════
# PROGRESS REPORTER
# ═══════════════════════════════════════════════════════════

@dataclass
class WorkerState:
    index: int
    email: str
    stage: str = "queued"
    started_at: float = 0.0
    updated_at: float = 0.0
    attempts: int = 0
    last_error: Optional[str] = None

    def to_dict(self, now: float) -> dict:
        elapsed_ms = int((now - self.started_at) * 1000) if self.started_at else 0
        return {
            "index": self.index,
            "email": self.email,
            "stage": self.stage,
            "attempts": self.attempts,
            "elapsedMs": elapsed_ms,
            "lastError": self.last_error,
        }


class ProgressReporter:
    __slots__ = ("_state", "_clock")

    def __init__(self, state: WorkerState) -> None:
        self._state = state
        self._clock = time.monotonic

    def stage(self, message: str) -> None:
        self._state.stage = str(message)[:120]
        self._state.updated_at = self._clock()

    def error(self, message: str) -> None:
        self._state.last_error = str(message)[:200]
        self._state.updated_at = self._clock()

    def retry(self) -> None:
        self._state.attempts += 1
        self._state.updated_at = self._clock()


# ═══════════════════════════════════════════════════════════
# POOL
# ═══════════════════════════════════════════════════════════

TaskFn = Callable[[int, dict, ProgressReporter], Awaitable[dict]]


async def run_pool(
    *,
    kind: str,
    accounts: list[dict],
    task_fn: TaskFn,
    on_retry_factory: Optional[Callable[[dict], dict]] = None,
    workers: Optional[int] = None,
    interval_ms: Optional[int] = None,
    max_consecutive_failures: Optional[int] = None,
    max_rate_limit_hits: Optional[int] = None,
):
    workers = workers or settings.BULK_WORKERS
    interval_ms = interval_ms if interval_ms is not None else settings.BULK_INTERVAL_MS
    max_consecutive_failures = (max_consecutive_failures
                                if max_consecutive_failures is not None
                                else settings.BULK_MAX_CONSECUTIVE_FAILURES)
    max_rate_limit_hits = (max_rate_limit_hits
                           if max_rate_limit_hits is not None
                           else settings.BULK_MAX_RATE_LIMIT_HITS)

    total = len(accounts)
    interval = max(0.0, interval_ms / 1000.0)
    interval_min = max(0.0, settings.BULK_INTERVAL_MIN_MS / 1000.0)
    interval_max = max(interval_min, settings.BULK_INTERVAL_MAX_MS / 1000.0)

    results: list[Optional[dict]] = [None] * total
    states: dict[int, WorkerState] = {
        i: WorkerState(index=i, email=acc.get("email") or f"Account {i+1}")
        for i, acc in enumerate(accounts)
    }

    success = 0
    fail = 0
    consecutive_failures = 0
    rate_limit_hits = 0
    cancelled = False
    stop_reason = ""
    next_index = 0
    next_start = 0.0
    pending: dict[int, asyncio.Task] = {}
    recent_results: list[bool] = []

    def workers_snapshot() -> list[dict]:
        now = time.monotonic()
        return [states[i].to_dict(now) for i in sorted(states.keys())]

    def decorate(ev: dict) -> dict:
        ev["workers"] = workers_snapshot()
        ev["intervalMs"] = int(interval * 1000)
        return ev

    yield decorate({
        "status": "start",
        "message": f"Starting {total} account(s) with {workers} worker(s)…",
        "total": total,
    })

    try:
        while True:
            while (
                next_index < total
                and len(pending) < workers
                and not cancelled
                and time.monotonic() >= next_start
            ):
                idx = next_index
                next_index += 1
                account = accounts[idx]
                reporter = ProgressReporter(states[idx])
                reporter.stage("queued")
                states[idx].started_at = time.monotonic()

                yield decorate({
                    "status": "account_pending",
                    "email": account.get("email"),
                    "password": account.get("password"),
                    "current": idx + 1, "total": total,
                    "message": "Preparing account",
                })

                task = asyncio.create_task(
                    task_fn(idx, account, reporter),
                    name=f"bulk-{kind}-{idx}",
                )
                pending[idx] = task
                next_start = time.monotonic() + interval

            if not pending:
                if next_index >= total or cancelled:
                    break
                await asyncio.sleep(0.05)
                continue

            timeout = None
            if next_index < total and len(pending) < workers and not cancelled:
                timeout = max(0.05, next_start - time.monotonic())

            done, _ = await asyncio.wait(
                list(pending.values()),
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                continue

            for task in done:
                idx = next(i for i, t in pending.items() if t is task)
                del pending[idx]
                account = accounts[idx]
                email = account.get("email")
                password = account.get("password")
                state = states[idx]

                if task.cancelled():
                    results[idx] = {"email": email, "password": password,
                                    "success": False, "error": "Cancelled"}
                    state.stage = "cancelled"
                    continue

                exc = task.exception()
                if exc is not None:
                    fail += 1
                    state.stage = "failed"
                    # FIXED: WorkerState has no .error() method — set last_error directly.
                    state.last_error = describe_error(exc)[:200]
                    state.updated_at = time.monotonic()
                    recent_results.append(False)
                    recent_results[:] = recent_results[-20:]

                    results[idx] = {"email": email, "password": password,
                                    "success": False,
                                    "error": safe_error(describe_error(exc))}

                    category = classify_error(exc)
                    if category == "rate_limit":
                        rate_limit_hits += 1
                        signup_limiter.on_rate_limit()
                        interval = min(interval_max, max(interval * 1.6, 1.0))
                        next_start = max(next_start, time.monotonic() + 5.0)
                        if rate_limit_hits >= max_rate_limit_hits:
                            cancelled = True
                            stop_reason = (f"Stopped after {rate_limit_hits} "
                                           f"rate-limit responses. Wait a few "
                                           f"minutes before retrying.")
                    else:
                        consecutive_failures += 1
                        if consecutive_failures >= max_consecutive_failures:
                            cancelled = True
                            stop_reason = (f"Stopped after "
                                           f"{max_consecutive_failures} "
                                           f"consecutive failures. Remaining "
                                           f"accounts were skipped.")

                    yield decorate({
                        "status": "account_failed",
                        "email": email, "password": password,
                        "error": safe_error(describe_error(exc)),
                        "message": safe_error(describe_error(exc))[:120],
                        "current": success + fail, "total": total,
                        "successCount": success, "failCount": fail,
                        "pct": round(((success + fail) / total) * 100) if total else 0,
                    })
                else:
                    success += 1
                    consecutive_failures = 0
                    state.stage = "done"
                    signup_limiter.on_success()
                    recent_results.append(True)
                    recent_results[:] = recent_results[-20:]

                    if len(recent_results) >= 10 and all(recent_results[-10:]):
                        if interval > interval_min:
                            interval = max(interval_min, interval * 0.85)

                    results[idx] = {"email": email, "password": password,
                                    "success": True}

                    yield decorate({
                        "status": "account_done",
                        "email": email, "password": password,
                        "message": f"[{success + fail}/{total}] {email}",
                        "current": success + fail, "total": total,
                        "successCount": success, "failCount": fail,
                        "pct": round(((success + fail) / total) * 100) if total else 0,
                    })

            if cancelled:
                break

    finally:
        for t in list(pending.values()):
            if not t.done():
                t.cancel()
        if pending:
            await asyncio.gather(*pending.values(), return_exceptions=True)

    skipped = total - success - fail
    txt = "\n".join(
        f"{r['email']}:{r['password']}"
        for r in results if r and r.get("success") and r.get("password")
    )

    yield decorate({
        "status": "error" if cancelled else "complete",
        "message": stop_reason if cancelled
                   else f"{success}/{total} completed. {fail} failed.",
        "successCount": success,
        "failCount": fail,
        "skippedCount": max(0, skipped),
        "total": total,
        "results": [r for r in results if r is not None],
        "txtContent": txt,
        "cancelled": cancelled,
        "skippedAccounts": [
            accounts[i] for i, r in enumerate(results)
            if r is None or (not r.get("success") and i >= next_index - 1)
        ] if cancelled and on_retry_factory else [],
    })