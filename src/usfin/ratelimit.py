"""Per-host rate limiting: one thread-safe token bucket per bucket name, shared in the process.

`config/hosts.yaml` groups hosts into named buckets with a rate (requests/second) and extra
headers; hosts in one bucket share its limit (`www.sec.gov` + `data.sec.gov`). Unlisted hosts get
a bucket of their own at `default_rate`. A SEC bucket faster than 8 rps is a `ConfigError`.
"""

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from usfin.settings import ConfigError

DEFAULT_RATE = 4.0
SEC_MAX_RATE = 8.0


def is_sec_host(host: str) -> bool:
    host = host.lower()
    return host == "sec.gov" or host.endswith(".sec.gov")


class TokenBucket:
    """Token bucket of capacity 1 (no banked burst), in virtual-scheduling form.

    Each grant reserves the next slot `1/rate` after the previous one, so grants are never closer
    than `1/rate` apart: at most `rate` grants in any 1 s window, across any number of threads.
    The lock is held only to reserve; waiting happens outside it.
    """

    def __init__(
        self,
        rate: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if rate <= 0:
            raise ValueError(f"rate must be > 0, got {rate}")
        self.rate = float(rate)
        self._interval = 1.0 / self.rate
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._next: float | None = None

    def acquire(self) -> float:
        """Block until a token is granted; return the clock time of the grant."""
        while True:
            with self._lock:
                now = self._clock()
                if self._next is None or now >= self._next:
                    self._next = now + self._interval
                    return now
                wait = self._next - now
            self._sleep(wait)


@dataclass(frozen=True)
class HostPolicy:
    host: str
    bucket: str
    rate: float
    headers: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Hosts:
    default_rate: float
    by_host: dict[str, HostPolicy]

    def policy(self, host: str) -> HostPolicy:
        host = host.lower()
        found = self.by_host.get(host)
        if found is not None:
            return found
        return HostPolicy(host=host, bucket=host, rate=self.default_rate)


def load_hosts(config_dir: Path | str) -> Hosts:
    """Read `<config_dir>/hosts.yaml`; a missing file means every host at the default rate."""
    path = Path(config_dir) / "hosts.yaml"
    data = (yaml.safe_load(path.read_text("utf-8")) if path.is_file() else None) or {}
    default_rate = float(data.get("default_rate", DEFAULT_RATE))
    by_host: dict[str, HostPolicy] = {}
    for name, spec in (data.get("buckets") or {}).items():
        rate = float(spec.get("rate", default_rate))
        headers = {str(k): str(v) for k, v in (spec.get("headers") or {}).items()}
        for host in spec.get("hosts") or []:
            host = str(host).lower()
            if is_sec_host(host) and rate > SEC_MAX_RATE:
                raise ConfigError(
                    f"{path}: bucket {name!r} sets {rate:g} rps for SEC host {host}; "
                    f"SEC allows at most {SEC_MAX_RATE:g}"
                )
            if host in by_host:
                raise ConfigError(f"{path}: host {host} is in more than one bucket")
            by_host[host] = HostPolicy(host=host, bucket=str(name), rate=rate, headers=headers)
    return Hosts(default_rate=default_rate, by_host=by_host)


class Limiters:
    """One `TokenBucket` per bucket name, created on first use."""

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._buckets: dict[str, TokenBucket] = {}

    def for_policy(self, policy: HostPolicy) -> TokenBucket:
        with self._lock:
            bucket = self._buckets.get(policy.bucket)
            if bucket is None:
                bucket = TokenBucket(policy.rate, clock=self._clock, sleep=self._sleep)
                self._buckets[policy.bucket] = bucket
            return bucket


LIMITERS = Limiters()
"""Process-wide limiters: every `http.Client` uses these unless a test injects its own."""
