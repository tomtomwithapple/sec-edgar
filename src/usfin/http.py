"""HTTP client: per-host rate limit, retries, SEC user agent and block window, archive-first.

`Client.get/post(url, source=, dataset=, ...)` returns `(raw_id, response)`. Before sending it
checks the SEC rules (no `SEC_USER_AGENT` → `ConfigError`; inside a block window →
`SecBlocked`), then takes a token from the host's bucket for every attempt. 429, 5xx and
transport errors are retried with exponential backoff (1, 2, 4, 8 s; `Retry-After` honoured),
at most `MAX_TRIES` attempts. The final response, whatever its status, is archived through the
raw store before the call returns or raises. A SEC 403 opens a process-wide block window
(`SEC_BLOCK_SECONDS`) and raises `SecBlocked`; nothing sleeps through it.

www.bls.gov (the release calendar; not api.bls.gov) answers 403 to any User-Agent without a
contact email, and also to one that carries a URL (probed 2026-10-08), so requests to it send
`usfin/<version> (<BLS_CONTACT_EMAIL>)`; without that setting → `ConfigError`, no request.
"""

import re
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from hashlib import sha256
from typing import Any, Self

import duckdb
import httpx

from usfin import __version__, ratelimit, raw
from usfin.settings import ConfigError, Settings, load_settings

MAX_TRIES = 5
BACKOFF_BASE = 1.0
SEC_BLOCK_SECONDS = 600.0
TIMEOUT = httpx.Timeout(60.0, connect=15.0)
USFIN_USER_AGENT = f"usfin/{__version__} (+https://github.com/tomtomtomdev/sec-edgar)"
_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
BLS_WWW_HOSTS = frozenset({"www.bls.gov"})
_EMAIL = re.compile(r"[^@\s()<>/]+@[^@\s()<>/]+\.[^@\s()<>/]+")


class HttpError(Exception):
    """The request failed after retries. `raw_id`/`response` are set when a response arrived."""

    def __init__(
        self, message: str, *, raw_id: int | None = None, response: httpx.Response | None = None
    ):
        super().__init__(message)
        self.raw_id = raw_id
        self.response = response


class SecBlocked(HttpError):
    """SEC answered 403 (or did recently): SEC requests are refused until the window ends."""


def _wall_now() -> datetime:
    return datetime.now(UTC)


class SecBlock:
    """Process-wide SEC block window, on a monotonic clock."""

    def __init__(self, *, clock: Callable[[], float] = time.monotonic):
        self._clock = clock
        self._lock = threading.Lock()
        self._until = 0.0

    def block(self, seconds: float = SEC_BLOCK_SECONDS) -> None:
        with self._lock:
            self._until = max(self._until, self._clock() + seconds)

    def remaining(self) -> float:
        with self._lock:
            return max(0.0, self._until - self._clock())

    def blocked(self) -> bool:
        return self.remaining() > 0


SEC_BLOCK = SecBlock()
"""Process-wide: SEC jobs check `SEC_BLOCK.blocked()` at start and record `skipped`."""


def _retry_after(response: httpx.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max(0.0, (when - _wall_now()).total_seconds())


class Client:
    """Sync HTTP client that archives every final response into the raw store via `con`."""

    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        *,
        settings: Settings | None = None,
        transport: httpx.BaseTransport | None = None,
        limiters: ratelimit.Limiters | None = None,
        sec_block: SecBlock | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.con = con
        self.settings = settings or load_settings()
        self.hosts = ratelimit.load_hosts(self.settings.config_dir)
        self.limiters = limiters or ratelimit.LIMITERS
        self.sec_block = sec_block or SEC_BLOCK
        self._clock = clock
        self._sleep = sleep
        self._http = httpx.Client(transport=transport, timeout=TIMEOUT, follow_redirects=True)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def get(self, url: str, *, source: str, dataset: str, **kwargs: Any):
        return self.request("GET", url, source=source, dataset=dataset, **kwargs)

    def post(self, url: str, *, source: str, dataset: str, **kwargs: Any):
        return self.request("POST", url, source=source, dataset=dataset, **kwargs)

    def request(
        self,
        method: str,
        url: str,
        *,
        source: str,
        dataset: str,
        params: Any = None,
        headers: dict[str, str] | None = None,
        json: Any = None,
        data: Any = None,
        stream: bool = False,
    ) -> tuple[int, httpx.Response]:
        """Send with limits and retries; archive the final response; return (raw_id, response).

        With `stream` the body goes to the raw store in chunks as it arrives (EDGAR bulk ZIPs,
        over a GiB) and is never held in memory; the returned response is then closed, so read
        the body back with `raw.open(con, raw_id)`. A body cut off mid-stream is retried like a
        transport error, and raises `HttpError` after `MAX_TRIES` (S26: before this, an httpx
        read error mid-body escaped the client, so a 1.3 GB download could die after minutes
        with nothing archived — the first live attempt did, its cause unrecorded)."""
        host = httpx.URL(url).host
        sec = ratelimit.is_sec_host(host)
        policy = self.hosts.policy(host)
        if sec and not self.settings.sec_user_agent:
            raise ConfigError(
                f"SEC_USER_AGENT is not set; refusing to request {host} "
                "(set it to 'Name email@example.com' in the environment or .env)"
            )
        req_headers = {
            **policy.headers,
            "User-Agent": self._user_agent(host, sec),
            **(headers or {}),
        }
        request = self._http.build_request(
            method, url, params=params, headers=req_headers, json=json, data=data
        )
        request_sha = sha256(
            f"{request.method} {request.url}\n".encode() + request.read()
        ).hexdigest()
        bucket = self.limiters.for_policy(policy)

        for attempt in range(1, MAX_TRIES + 1):
            if sec and self.sec_block.blocked():
                raise SecBlocked(
                    f"SEC requests blocked for another {self.sec_block.remaining():.0f}s "
                    f"after a 403; not sending {request.url}"
                )
            bucket.acquire()
            try:
                response = self._http.send(request, stream=stream)
            except httpx.TransportError as exc:
                if attempt == MAX_TRIES:
                    raise HttpError(
                        f"{method} {request.url} failed after {MAX_TRIES} tries: {exc!r}"
                    ) from exc
                self._sleep(BACKOFF_BASE * 2 ** (attempt - 1))
                continue

            if response.status_code in _RETRY_STATUSES and attempt < MAX_TRIES:
                backoff = BACKOFF_BASE * 2 ** (attempt - 1)
                retry_after = _retry_after(response)
                response.close()
                self._sleep(retry_after if retry_after is not None else backoff)
                continue

            try:
                raw_id = self._archive(
                    response, source=source, dataset=dataset, request_sha=request_sha,
                    stream=stream,
                )  # fmt: skip
            except httpx.TransportError as exc:
                # only a streamed body is read here: cut mid-body, nothing archived (the raw
                # store drops its spool), so it is a transport failure like any other
                if attempt == MAX_TRIES:
                    raise HttpError(
                        f"{method} {request.url} body cut off after {MAX_TRIES} tries: {exc!r}"
                    ) from exc
                self._sleep(BACKOFF_BASE * 2 ** (attempt - 1))
                continue
            finally:
                if stream:
                    response.close()
            if sec and response.status_code == 403:
                self.sec_block.block()
                raise SecBlocked(
                    f"SEC answered 403 for {request.url}; SEC requests blocked for "
                    f"{SEC_BLOCK_SECONDS:.0f}s",
                    raw_id=raw_id,
                    response=response,
                )
            if response.status_code in _RETRY_STATUSES:
                raise HttpError(
                    f"{method} {request.url} answered {response.status_code} "
                    f"after {MAX_TRIES} tries",
                    raw_id=raw_id,
                    response=response,
                )
            return raw_id, response
        raise AssertionError("unreachable")  # pragma: no cover

    def _user_agent(self, host: str, sec: bool) -> str:
        if sec:
            return self.settings.sec_user_agent
        if host in BLS_WWW_HOSTS:
            email = self.settings.bls_contact_email
            if not email or not _EMAIL.fullmatch(email):
                raise ConfigError(
                    f"BLS_CONTACT_EMAIL must be set to a contact email to request {host} "
                    f"(bls.gov refuses clients without one); got {email!r}"
                )
            return f"usfin/{__version__} ({email})"
        return USFIN_USER_AGENT

    def _archive(
        self,
        response: httpx.Response,
        *,
        source: str,
        dataset: str,
        request_sha: str,
        stream: bool = False,
    ) -> int:
        return raw.save(
            self.con,
            source=source,
            dataset=dataset,
            url=str(response.request.url),
            body=response.iter_bytes(raw.CHUNK_SIZE) if stream else response.content,
            http_status=response.status_code,
            content_type=response.headers.get("Content-Type"),
            request_sha256=request_sha,
            raw_dir=self.settings.raw_dir,
        )
