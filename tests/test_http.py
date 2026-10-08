import gzip
import threading
from dataclasses import replace
from pathlib import Path

import httpx
import pytest
import respx
from typer.testing import CliRunner

from usfin import __version__, http, ratelimit, raw, store
from usfin.cli import app
from usfin.settings import ConfigError, load_settings

REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"
FD = (
    "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/debt_to_penny"
)
SEC = "https://data.sec.gov/submissions/CIK0000320193.json"
SEC_UA = "Test Person test@example.com"


class FakeClock:
    def __init__(self, start: float = 1000.0):
        self.t = start
        self.sleeps: list[float] = []
        self._lock = threading.Lock()

    def __call__(self) -> float:
        with self._lock:
            return self.t

    def sleep(self, seconds: float) -> None:
        with self._lock:
            self.sleeps.append(seconds)
            self.t += seconds


@pytest.fixture
def con(tmp_path):
    with store.session(tmp_path / "usfin.duckdb", wait=0) as c:
        yield c


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def make_client(con, tmp_path, clock, monkeypatch):
    for key in ("SEC_USER_AGENT", "USFIN_CONFIG", "USFIN_RAW"):
        monkeypatch.delenv(key, raising=False)
    base = load_settings(env_file=tmp_path / "no.env")

    def make(sec_user_agent=SEC_UA, sec_block=None):
        settings = replace(
            base,
            raw_dir=tmp_path / "raw",
            config_dir=REPO_CONFIG,
            sec_user_agent=sec_user_agent,
        )
        return http.Client(
            con,
            settings=settings,
            limiters=ratelimit.Limiters(clock=clock, sleep=clock.sleep),
            sec_block=sec_block or http.SecBlock(clock=clock),
            clock=clock,
            sleep=clock.sleep,
        )

    return make


def _row(con, raw_id):
    cur = con.execute("SELECT * FROM raw_response WHERE id = ?", [raw_id])
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, cur.fetchone(), strict=True))


def _rows(con):
    return con.execute("SELECT count(*) FROM raw_response").fetchone()[0]


@respx.mock
def test_success_is_archived_before_return(con, make_client, tmp_path):
    route = respx.get(FD).respond(
        200, json={"data": []}, headers={"Content-Type": "application/json"}
    )
    raw_id, resp = make_client().get(FD, source="fiscal", dataset="fd_debt_to_penny")

    assert route.call_count == 1
    assert resp.status_code == 200
    row = _row(con, raw_id)
    assert row["source"] == "fiscal" and row["dataset"] == "fd_debt_to_penny"
    assert row["url"] == FD and row["http_status"] == 200
    assert row["content_type"].startswith("application/json")
    assert row["bytes"] == len(resp.content)
    assert row["request_sha256"]
    assert gzip.decompress((tmp_path / "raw" / row["blob_path"]).read_bytes()) == resp.content


@respx.mock
def test_params_are_part_of_the_archived_url(con, make_client):
    respx.get(FD).respond(200, json={})
    raw_id, _ = make_client().get(FD, source="fiscal", dataset="d", params={"page[size]": 1})
    assert _row(con, raw_id)["url"] == FD + "?page%5Bsize%5D=1"


@respx.mock
def test_404_is_archived_and_returned_without_retry(con, make_client):
    route = respx.get(FD).respond(404, text="not here")
    raw_id, resp = make_client().get(FD, source="fiscal", dataset="d")
    assert route.call_count == 1
    assert resp.status_code == 404
    assert _row(con, raw_id)["http_status"] == 404


@respx.mock
def test_500_then_success_retries_with_backoff_and_archives_the_final_response(
    con, make_client, clock
):
    route = respx.get(FD).mock(
        side_effect=[httpx.Response(500), httpx.Response(503), httpx.Response(200, text="ok")]
    )
    raw_id, resp = make_client().get(FD, source="fiscal", dataset="d")
    assert route.call_count == 3
    assert resp.text == "ok"
    assert _row(con, raw_id)["http_status"] == 200
    assert _rows(con) == 1
    assert [s for s in clock.sleeps if s >= 1] == [1, 2]


@respx.mock
def test_five_500s_archive_the_last_and_raise(con, make_client):
    route = respx.get(FD).respond(500, text="boom")
    with pytest.raises(http.HttpError) as info:
        make_client().get(FD, source="fiscal", dataset="d")
    assert route.call_count == 5
    assert info.value.raw_id is not None
    assert _row(con, info.value.raw_id)["http_status"] == 500
    assert info.value.response.status_code == 500


@respx.mock
def test_transport_errors_are_retried_then_raise(con, make_client):
    route = respx.get(FD).mock(side_effect=httpx.ConnectError("refused"))
    with pytest.raises(http.HttpError) as info:
        make_client().get(FD, source="fiscal", dataset="d")
    assert route.call_count == 5
    assert info.value.raw_id is None
    assert _rows(con) == 0


@respx.mock
def test_transport_error_then_success(con, make_client):
    route = respx.get(FD).mock(side_effect=[httpx.ReadTimeout("slow"), httpx.Response(200)])
    raw_id, resp = make_client().get(FD, source="fiscal", dataset="d")
    assert route.call_count == 2 and resp.status_code == 200 and raw_id


@respx.mock
def test_429_retry_after_seconds_is_honoured(con, make_client, clock):
    route = respx.get(FD).mock(
        side_effect=[httpx.Response(429, headers={"Retry-After": "7"}), httpx.Response(200)]
    )
    _, resp = make_client().get(FD, source="fiscal", dataset="d")
    assert route.call_count == 2 and resp.status_code == 200
    assert 7 in clock.sleeps


@respx.mock
def test_429_retry_after_http_date_is_honoured(con, make_client, clock, monkeypatch):
    from datetime import UTC, datetime

    monkeypatch.setattr(http, "_wall_now", lambda: datetime(2026, 10, 8, 12, 0, 0, tzinfo=UTC))
    respx.get(FD).mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "Thu, 08 Oct 2026 12:00:30 GMT"}),
            httpx.Response(200),
        ]
    )
    make_client().get(FD, source="fiscal", dataset="d")
    assert 30 in clock.sleeps


@respx.mock
def test_post_sends_json_and_archives(con, make_client):
    url = "https://api.bls.gov/publicAPI/v1/timeseries/data/"
    route = respx.post(url).respond(200, json={"status": "REQUEST_SUCCEEDED"})
    raw_id, _ = make_client().post(
        url, source="bls", dataset="bls_timeseries", json={"seriesid": ["LNS14000000"]}
    )
    assert route.calls.last.request.content == b'{"seriesid":["LNS14000000"]}'
    assert _row(con, raw_id)["dataset"] == "bls_timeseries"


@respx.mock
def test_non_sec_hosts_get_the_usfin_user_agent(make_client):
    route = respx.get(FD).respond(200)
    make_client(sec_user_agent=SEC_UA).get(FD, source="fiscal", dataset="d")
    ua = route.calls.last.request.headers["User-Agent"]
    assert ua == f"usfin/{__version__} (+https://github.com/tomtomtomdev/sec-edgar)"


@respx.mock
def test_sec_user_agent_is_sent_to_sec_hosts(make_client):
    route = respx.get(SEC).respond(200, json={})
    make_client().get(SEC, source="edgar", dataset="edgar_submissions")
    assert route.calls.last.request.headers["User-Agent"] == SEC_UA


@respx.mock
def test_sec_host_without_user_agent_raises_config_error_with_no_request(con, make_client):
    route = respx.get(SEC).respond(200)
    www = respx.get("https://www.sec.gov/files/company_tickers.json").respond(200)
    client = make_client(sec_user_agent=None)
    with pytest.raises(ConfigError, match="SEC_USER_AGENT"):
        client.get(SEC, source="edgar", dataset="edgar_submissions")
    with pytest.raises(ConfigError):
        client.get("https://www.sec.gov/files/company_tickers.json", source="edgar", dataset="t")
    assert route.call_count == 0 and www.call_count == 0
    assert _rows(con) == 0


@respx.mock
def test_sec_403_sets_block_window_and_later_sec_calls_send_nothing(con, make_client, clock):
    block = http.SecBlock(clock=clock)
    route = respx.get(SEC).respond(403, text="Request Rate Threshold Exceeded")
    fd = respx.get(FD).respond(200)
    client = make_client(sec_block=block)

    with pytest.raises(http.SecBlocked) as info:
        client.get(SEC, source="edgar", dataset="edgar_submissions")
    assert route.call_count == 1  # a 403 is not retried
    assert _row(con, info.value.raw_id)["http_status"] == 403
    assert block.remaining() == pytest.approx(600)

    clock.sleep(599)
    with pytest.raises(http.SecBlocked):
        make_client(sec_block=block).get(
            "https://www.sec.gov/files/company_tickers.json", source="edgar", dataset="t"
        )
    assert route.call_count == 1

    # other hosts are not stalled by the block
    client.get(FD, source="fiscal", dataset="d")
    assert fd.call_count == 1

    clock.sleep(2)
    assert not block.blocked()
    route.respond(200, json={})
    client.get(SEC, source="edgar", dataset="edgar_submissions")
    assert route.call_count == 2


@respx.mock
def test_a_sec_403_to_a_head_is_archived_and_returned_with_no_block(con, make_client, clock):
    # S35: SEC refused a HEAD on an Archives file whose GET worked (S36); a refused HEAD is a
    # method refusal, not a rate limit, so the caller decides (a GET 403 still blocks)
    block = http.SecBlock(clock=clock)
    respx.head(SEC).respond(403, text="denied")
    raw_id, resp = make_client(sec_block=block).request(
        "HEAD", SEC, source="edgar", dataset="edgar_bulk_head"
    )
    assert resp.status_code == 403 and _row(con, raw_id)["http_status"] == 403
    assert not block.blocked()


@respx.mock
def test_inspect_refusing_the_final_response_archives_nothing(con, make_client):
    respx.get(SEC).respond(200, content=b"x" * 5000)

    def refuse(response):
        raise RuntimeError(f"too big: {response.headers['Content-Length']}")

    with pytest.raises(RuntimeError, match="too big: 5000"):
        make_client().get(SEC, source="edgar", dataset="d", stream=True, inspect=refuse)
    assert _rows(con) == 0


@respx.mock
def test_non_sec_403_is_returned_not_blocking(con, make_client):
    respx.get(FD).respond(403)
    raw_id, resp = make_client().get(FD, source="fiscal", dataset="d")
    assert resp.status_code == 403 and raw_id


def test_clients_share_the_process_wide_limiters_and_sec_block_by_default(con, tmp_path):
    settings = replace(load_settings(env_file=tmp_path / "no.env"), config_dir=REPO_CONFIG)
    a, b = http.Client(con, settings=settings), http.Client(con, settings=settings)
    assert a.limiters is b.limiters is ratelimit.LIMITERS
    assert a.sec_block is b.sec_block is http.SEC_BLOCK


@respx.mock
def test_requests_take_a_token_from_the_host_bucket(con, make_client, clock):
    respx.get(SEC).respond(200, json={})
    client = make_client()
    for _ in range(9):
        client.get(SEC, source="edgar", dataset="edgar_submissions")
    # 8 rps → nine requests need 8 intervals of 1/8 s of (fake) waiting
    assert sum(clock.sleeps) == pytest.approx(1.0)


@respx.mock
def test_cli_http_get_prints_status_raw_id_and_bytes(tmp_path, monkeypatch):
    monkeypatch.setenv("USFIN_DB", str(tmp_path / "usfin.duckdb"))
    monkeypatch.setenv("USFIN_RAW", str(tmp_path / "raw"))
    monkeypatch.setenv("USFIN_CONFIG", str(REPO_CONFIG))
    monkeypatch.chdir(tmp_path)
    respx.get(FD).respond(200, text="0123456789")
    result = CliRunner().invoke(app, ["http", "get", FD])
    assert result.exit_code == 0, result.output
    assert "status: 200" in result.output
    assert "raw_id: 1" in result.output
    assert "bytes: 10" in result.output
    with (
        store.session(tmp_path / "usfin.duckdb", wait=0) as c,
        raw.open(c, 1, raw_dir=tmp_path / "raw") as f,
    ):
        assert f.read() == b"0123456789"


class _CutStream(httpx.SyncByteStream):
    """A body that breaks off after its first chunk, as a long SEC bulk download can."""

    def __iter__(self):
        yield b"PK\x03\x04partial"
        raise httpx.ReadError("connection reset mid-body")


BULK = "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"


@respx.mock
def test_a_stream_cut_mid_body_is_retried_and_only_the_whole_body_archived(
    con, make_client, tmp_path
):
    route = respx.get(BULK).mock(
        side_effect=[
            httpx.Response(200, stream=_CutStream()),
            httpx.Response(200, content=b"PK\x03\x04whole body"),
        ]
    )
    raw_id, _ = make_client().get(BULK, source="edgar", dataset="bulk_companyfacts", stream=True)
    assert route.call_count == 2
    assert _rows(con) == 1
    with raw.open(con, raw_id, raw_dir=tmp_path / "raw") as fh:
        assert fh.read() == b"PK\x03\x04whole body"
    assert not [p for p in (tmp_path / "raw").rglob(".tmp-*")]


@respx.mock
def test_a_stream_cut_every_time_raises_http_error_and_archives_nothing(
    con, make_client, tmp_path
):
    respx.get(BULK).mock(side_effect=lambda req: httpx.Response(200, stream=_CutStream()))
    with pytest.raises(http.HttpError, match="tries"):
        make_client().get(BULK, source="edgar", dataset="bulk_companyfacts", stream=True)
    assert _rows(con) == 0
    assert not [p for p in (tmp_path / "raw").rglob(".tmp-*")]
