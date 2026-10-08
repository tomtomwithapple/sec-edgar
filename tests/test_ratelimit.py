import threading
from itertools import pairwise
from pathlib import Path

import pytest

from usfin import ratelimit
from usfin.settings import ConfigError

REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"


class FakeClock:
    """Thread-safe fake monotonic clock; `sleep` advances it instead of waiting."""

    def __init__(self, start: float = 0.0):
        self.t = start
        self._lock = threading.Lock()

    def __call__(self) -> float:
        with self._lock:
            return self.t

    def sleep(self, seconds: float) -> None:
        assert seconds >= 0
        with self._lock:
            self.t += seconds


def _max_in_any_window(grants: list[float], window: float = 1.0) -> int:
    grants = sorted(grants)
    return max(sum(1 for g in grants if start <= g < start + window) for start in grants)


def test_single_thread_grants_are_evenly_spaced_at_the_rate():
    clock = FakeClock()
    bucket = ratelimit.TokenBucket(8, clock=clock, sleep=clock.sleep)
    grants = [bucket.acquire() for _ in range(16)]
    assert grants[0] == 0.0
    assert grants[-1] == pytest.approx(15 / 8)
    assert _max_in_any_window(grants) == 8


def test_8_rps_never_exceeds_8_grants_in_any_1s_window_across_4_threads():
    clock = FakeClock(start=100.0)
    bucket = ratelimit.TokenBucket(8, clock=clock, sleep=clock.sleep)
    grants: list[float] = []
    lock = threading.Lock()
    start = threading.Barrier(4)

    def worker():
        start.wait()
        for _ in range(25):
            g = bucket.acquire()
            with lock:
                grants.append(g)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(grants) == 100
    assert _max_in_any_window(grants) <= 8
    ordered = sorted(grants)
    assert min(b - a for a, b in pairwise(ordered)) >= 1 / 8 - 1e-9


def test_idle_bucket_does_not_bank_a_burst():
    clock = FakeClock()
    bucket = ratelimit.TokenBucket(8, clock=clock, sleep=clock.sleep)
    bucket.acquire()
    clock.sleep(60)
    grants = [bucket.acquire() for _ in range(9)]
    assert _max_in_any_window(grants) == 8


def test_rate_must_be_positive():
    with pytest.raises(ValueError):
        ratelimit.TokenBucket(0)


def test_sec_hosts_share_one_8_rps_bucket_in_the_shipped_config():
    hosts = ratelimit.load_hosts(REPO_CONFIG)
    www = hosts.policy("www.sec.gov")
    data = hosts.policy("data.sec.gov")
    assert www.bucket == data.bucket
    assert www.rate == data.rate == 8

    limiters = ratelimit.Limiters()
    assert limiters.for_policy(www) is limiters.for_policy(data)


def test_other_hosts_get_their_own_default_4_rps_bucket():
    hosts = ratelimit.load_hosts(REPO_CONFIG)
    fd = hosts.policy("api.fiscaldata.treasury.gov")
    bls = hosts.policy("api.bls.gov")
    assert fd.rate == bls.rate == 4
    assert fd.bucket != bls.bucket

    limiters = ratelimit.Limiters()
    assert limiters.for_policy(fd) is not limiters.for_policy(bls)
    assert limiters.for_policy(fd) is limiters.for_policy(
        hosts.policy("api.fiscaldata.treasury.gov")
    )


def test_host_lookup_is_case_insensitive_and_carries_headers(tmp_path):
    (tmp_path / "hosts.yaml").write_text(
        "default_rate: 2\n"
        "buckets:\n"
        "  slow:\n"
        "    hosts: [Example.TEST]\n"
        "    rate: 1\n"
        "    headers: {Accept: application/json}\n"
    )
    hosts = ratelimit.load_hosts(tmp_path)
    p = hosts.policy("example.test")
    assert (p.bucket, p.rate, p.headers) == ("slow", 1, {"Accept": "application/json"})
    assert hosts.policy("other.test").rate == 2


def test_missing_hosts_file_falls_back_to_defaults(tmp_path):
    hosts = ratelimit.load_hosts(tmp_path)
    assert hosts.policy("www.sec.gov").rate == 4


def test_sec_bucket_faster_than_8_rps_is_rejected(tmp_path):
    (tmp_path / "hosts.yaml").write_text(
        "buckets:\n  sec:\n    hosts: [www.sec.gov]\n    rate: 10\n"
    )
    with pytest.raises(ConfigError, match="8"):
        ratelimit.load_hosts(tmp_path)
