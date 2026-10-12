"""HTTP client: per-host rate limit, retries, SEC user agent and block window, archive-first.

`Client.get/post(url, source=, dataset=, ...)` returns `(raw_id, response)`. Before sending it
checks the SEC rules (no `SEC_USER_AGENT` → `ConfigError`; inside a block window →
`SecBlocked`), then takes a token from the host's bucket for every attempt. 429, 5xx and
transport errors are retried with exponential backoff (1, 2, 4, 8 s; `Retry-After` honoured),
at most `MAX_TRIES` attempts. The final response, whatever its status, is archived through the
raw store before the call returns or raises. A SEC 403 opens a process-wide block window
(`SEC_BLOCK_SECONDS`) and raises `SecBlocked`; nothing sleeps through it — except a 403 for a
daily-index file of today or a later day (New York date): the S3 store behind www.sec.gov answers
`AccessDenied` (403, `application/xml`) for an object that does not exist yet, so such a 403 means
"not published yet" and is returned to the caller like a 404, with no block (S36; C5 holds for
every other SEC 403, a past day's daily index included). A SEC 403 to a HEAD is returned too, with
no block (S35): SEC refused a HEAD on an Archives file whose GET answered 200 (S36), so a refused
HEAD is a method refusal, not a rate limit; the caller falls back to a GET, which blocks if refused.

`inspect(response)` (optional) sees the final response before its body is read or archived; if it
raises, the response is closed, nothing is archived and the error propagates — used to refuse a
streamed bulk ZIP whose `Content-Length` does not fit the free-disk guard (S35).

www.bls.gov (the release calendar; not api.bls.gov) answers 403 to any User-Agent without a
contact email, and also to one that carries a URL (probed 2026-10-08), so requests to it send
`usfin/<version> (<BLS_CONTACT_EMAIL>)`; without that setting → `ConfigError`, no request.

The two Alpaca hosts (`ALPACA_HOSTS`) get `APCA-API-KEY-ID` / `APCA-API-SECRET-KEY` from Settings
on every request, and no other host does; with either key unset → `ConfigError`, no request. The
headers are never archived: the raw store keeps the redacted URL, body, status and content type,
and the request hash covers method, URL and body only. A 429 without `Retry-After` but with
Alpaca's `X-Ratelimit-Reset` (epoch seconds) waits until that instant (alpaca-market-data S1).

A secret query parameter (`api_key`, FRED/ALFRED's key, S18) never leaves the process except to
its host: `redact` replaces its value with `REDACTED` in the archived URL, the request hash, every
error message and httpx's own `HTTP Request:` log line.
"""

import logging
import re
import threading
import time
from collections.abc import Callable
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from hashlib import sha256
from typing import Any, Self
from zoneinfo import ZoneInfo

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
ALPACA_HOSTS = frozenset({"data.alpaca.markets", "paper-api.alpaca.markets"})
_EMAIL = re.compile(r"[^@\s()<>/]+@[^@\s()<>/]+\.[^@\s()<>/]+")
ET = ZoneInfo("America/New_York")
_SECRET_PARAM = re.compile(r"(?<=[?&]api_key=)[^&#\s\"']+")


def redact(text: object) -> str:
    """`text` (a URL or a message carrying one) with every `api_key=` value replaced."""
    return _SECRET_PARAM.sub("REDACTED", str(text))


class _RedactFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        shown = redact(message)
        if shown != message:
            record.msg, record.args = shown, ()
        return True


logging.getLogger("httpx").addFilter(_RedactFilter())

_DAILY_INDEX_DAY = re.compile(r"^/Archives/edgar/daily-index/\d{4}/QTR[1-4]/[a-z]+\.(\d{8})\.")


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


def _ratelimit_reset(response: httpx.Response) -> float | None:
    """Seconds until Alpaca's `X-Ratelimit-Reset` (epoch seconds), 0 if passed; None if absent."""
    value = (response.headers.get("X-Ratelimit-Reset") or "").strip()
    if not value.isdigit():
        return None
    return max(0.0, int(value) - _wall_now().timestamp())


def unpublished_daily_index(url: httpx.URL | str, today: date) -> bool:
    """True for a www.sec.gov daily-index file (`<name>.YYYYMMDD.<ext>`) of `today` or later: SEC
    answers 403, not 404, while it does not exist yet (master.20261008.idx at 03:47 ET, S32)."""
    url = httpx.URL(str(url))
    m = _DAILY_INDEX_DAY.match(url.path) if url.host == "www.sec.gov" else None
    if m is None:
        return False
    try:
        day = date(int(m[1][:4]), int(m[1][4:6]), int(m[1][6:]))
    except ValueError:
        return False
    return day >= today


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
        inspect: Callable[[httpx.Response], None] | None = None,
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
            **self._alpaca_auth(host),
        }
        request = self._http.build_request(
            method, url, params=params, headers=req_headers, json=json, data=data
        )
        shown = redact(request.url)
        request_sha = sha256(
            f"{request.method} {shown}\n".encode() + request.read()
        ).hexdigest()
        bucket = self.limiters.for_policy(policy)

        for attempt in range(1, MAX_TRIES + 1):
            if sec and self.sec_block.blocked():
                raise SecBlocked(
                    f"SEC requests blocked for another {self.sec_block.remaining():.0f}s "
                    f"after a 403; not sending {shown}"
                )
            bucket.acquire()
            try:
                response = self._http.send(request, stream=stream)
            except httpx.TransportError as exc:
                if attempt == MAX_TRIES:
                    raise HttpError(
                        f"{method} {shown} failed after {MAX_TRIES} tries: {exc!r}"
                    ) from exc
                self._sleep(BACKOFF_BASE * 2 ** (attempt - 1))
                continue

            if response.status_code in _RETRY_STATUSES and attempt < MAX_TRIES:
                backoff = BACKOFF_BASE * 2 ** (attempt - 1)
                retry_after = _retry_after(response)
                if retry_after is None:
                    retry_after = _ratelimit_reset(response)
                response.close()
                self._sleep(retry_after if retry_after is not None else backoff)
                continue

            if inspect is not None:
                try:
                    inspect(response)
                except BaseException:
                    response.close()
                    raise
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
                        f"{method} {shown} body cut off after {MAX_TRIES} tries: {exc!r}"
                    ) from exc
                self._sleep(BACKOFF_BASE * 2 ** (attempt - 1))
                continue
            finally:
                if stream:
                    response.close()
            if sec and response.status_code == 403:
                if request.method == "HEAD":
                    return raw_id, response  # a refused HEAD: the caller falls back to a GET
                if unpublished_daily_index(request.url, _wall_now().astimezone(ET).date()):
                    return raw_id, response  # not published yet: the caller decides, no block
                self.sec_block.block()
                raise SecBlocked(
                    f"SEC answered 403 for {shown}; SEC requests blocked for "
                    f"{SEC_BLOCK_SECONDS:.0f}s",
                    raw_id=raw_id,
                    response=response,
                )
            if response.status_code in _RETRY_STATUSES:
                raise HttpError(
                    f"{method} {shown} answered {response.status_code} "
                    f"after {MAX_TRIES} tries",
                    raw_id=raw_id,
                    response=response,
                )
            return raw_id, response
        raise AssertionError("unreachable")  # pragma: no cover

    def _alpaca_auth(self, host: str) -> dict[str, str]:
        if host not in ALPACA_HOSTS:
            return {}
        missing = [
            name
            for name, value in (
                ("APCA_API_KEY_ID", self.settings.apca_api_key_id),
                ("APCA_API_SECRET_KEY", self.settings.apca_api_secret_key),
            )
            if not value
        ]
        if missing:
            raise ConfigError(
                f"{' and '.join(missing)} not set; refusing to request {host} "
                "(add the Alpaca paper account's keys to the environment or .env)"
            )
        return {
            "APCA-API-KEY-ID": self.settings.apca_api_key_id,
            "APCA-API-SECRET-KEY": self.settings.apca_api_secret_key,
        }

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
            url=redact(response.request.url),
            body=response.iter_bytes(raw.CHUNK_SIZE) if stream else response.content,
            http_status=response.status_code,
            content_type=response.headers.get("Content-Type"),
            request_sha256=request_sha,
            raw_dir=self.settings.raw_dir,
        )
