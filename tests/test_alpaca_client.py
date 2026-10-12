"""Alpaca access through the shared HTTP client (plan alpaca-market-data: S1; A1, A10, A12).

Requests to the two Alpaca hosts carry `APCA-API-KEY-ID` / `APCA-API-SECRET-KEY` from Settings
and no other host gets them; with either key unset a request to an Alpaca host raises
`ConfigError` before anything is sent (client, job and `usfin http get`); a 429 with
`X-Ratelimit-Reset` waits until it; the key and secret never reach the raw store, `raw_response`,
logs or error messages; the hosts have their own 3 rps buckets; doctor reports the keys as `info`
set/unset and checks `config/alpaca.yaml`. Fake keys only.
"""

import gzip
import logging
import shutil
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest
import respx
from typer.testing import CliRunner

from usfin import http, jobs, ratelimit, store
from usfin.cli import app
from usfin.settings import ConfigError, load_settings

REPO = Path(__file__).resolve().parents[1]
REPO_CONFIG = REPO / "config"
KEY_ID = "PKFAKEKEYID0123456789"
SECRET = "fakeSecretValue0123456789abcdefghijklmno"
DATA = "https://data.alpaca.markets/v2/stocks/bars?symbols=AAPL&timeframe=1Day"
TRADING = "https://paper-api.alpaca.markets/v2/clock"
FD = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/x"


class FakeClock:
    def __init__(self, start: float = 1000.0):
        self.t = start
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
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
def base(tmp_path, monkeypatch):
    for key in ("APCA_API_KEY_ID", "APCA_API_SECRET_KEY", "USFIN_CONFIG", "USFIN_RAW"):
        monkeypatch.delenv(key, raising=False)
    settings = load_settings(env_file=tmp_path / "no.env")
    return replace(settings, raw_dir=tmp_path / "raw", config_dir=REPO_CONFIG)


@pytest.fixture
def make_client(con, base, clock):
    def make(key_id=KEY_ID, secret=SECRET):
        settings = replace(base, apca_api_key_id=key_id, apca_api_secret_key=secret)
        return http.Client(
            con,
            settings=settings,
            limiters=ratelimit.Limiters(clock=clock, sleep=clock.sleep),
            clock=clock,
            sleep=clock.sleep,
        )

    return make


# --- settings -------------------------------------------------------------------------------


def test_settings_read_the_alpaca_keys_from_env_file(tmp_path, monkeypatch):
    for key in ("APCA_API_KEY_ID", "APCA_API_SECRET_KEY"):
        monkeypatch.delenv(key, raising=False)
    env = tmp_path / ".env"
    env.write_text(f"APCA_API_KEY_ID={KEY_ID}\nAPCA_API_SECRET_KEY={SECRET}\n", "utf-8")
    s = load_settings(env_file=env)
    assert (s.apca_api_key_id, s.apca_api_secret_key) == (KEY_ID, SECRET)
    s = load_settings(env_file=tmp_path / "none.env")
    assert (s.apca_api_key_id, s.apca_api_secret_key) == (None, None)


# --- auth headers ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.parametrize("url", [DATA, TRADING])
def test_requests_to_alpaca_hosts_carry_the_keys(make_client, url):
    route = respx.get(url).respond(200, json={})
    make_client().get(url, source="alpaca", dataset="t")
    headers = route.calls.last.request.headers
    assert headers["APCA-API-KEY-ID"] == KEY_ID
    assert headers["APCA-API-SECRET-KEY"] == SECRET


@respx.mock
def test_other_hosts_never_get_the_keys(make_client):
    route = respx.get(FD).respond(200, json={})
    make_client().get(FD, source="fiscal", dataset="t")
    headers = route.calls.last.request.headers
    assert "APCA-API-KEY-ID" not in headers and "APCA-API-SECRET-KEY" not in headers
    assert KEY_ID not in str(headers) and SECRET not in str(headers)


@respx.mock
@pytest.mark.parametrize(
    "key_id,secret,missing",
    [(None, SECRET, "APCA_API_KEY_ID"), (KEY_ID, None, "APCA_API_SECRET_KEY"), (None, None, "APCA")],
)
def test_an_unset_key_raises_config_error_before_any_request(
    con, make_client, key_id, secret, missing
):
    data = respx.get(DATA).respond(200)
    trading = respx.get(TRADING).respond(200)
    client = make_client(key_id=key_id, secret=secret)
    for url in (DATA, TRADING):
        with pytest.raises(ConfigError, match=missing):
            client.get(url, source="alpaca", dataset="t")
    assert data.call_count == 0 and trading.call_count == 0
    assert con.execute("SELECT count(*) FROM raw_response").fetchone()[0] == 0


def test_a_job_on_an_alpaca_host_without_keys_fails_and_sends_nothing(base, tmp_path):
    settings = replace(base, db_path=tmp_path / "job.duckdb", lock_wait=0)

    def fetch(ctx, key):
        raw_id, _ = ctx.client.get(TRADING, source="alpaca", dataset="alpaca_clock")
        return [raw_id]

    registry = jobs.Registry()
    registry.register(jobs.Job(name="alpaca_probe", keys=("clock",), fetch=fetch, parsers={}))
    with respx.mock(assert_all_called=False) as router:
        route = router.route(host="paper-api.alpaca.markets").respond(200, json={})
        result = jobs.run_job("alpaca_probe", settings=settings, registry=registry)
    assert result.status == "failed"
    assert "ConfigError" in result.error and "APCA_API_KEY_ID" in result.error
    assert not route.called


def _cli_home(tmp_path, monkeypatch, env_text=""):
    for key in ("APCA_API_KEY_ID", "APCA_API_SECRET_KEY"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("USFIN_DB", str(tmp_path / "cli.duckdb"))
    monkeypatch.setenv("USFIN_RAW", str(tmp_path / "raw"))
    monkeypatch.setenv("USFIN_CONFIG", str(REPO_CONFIG))
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(env_text, "utf-8")


def test_http_get_to_alpaca_without_keys_exits_1_and_sends_nothing(tmp_path, monkeypatch):
    _cli_home(tmp_path, monkeypatch)
    with respx.mock(assert_all_called=False) as router:
        route = router.route(host="paper-api.alpaca.markets").respond(200, json={})
        out = CliRunner().invoke(app, ["http", "get", TRADING])
    assert out.exit_code == 1
    assert "APCA_API_KEY_ID" in out.output
    assert not route.called


def test_http_get_to_alpaca_with_keys_is_archived_and_leaks_nothing(tmp_path, monkeypatch):
    _cli_home(
        tmp_path, monkeypatch, f"APCA_API_KEY_ID={KEY_ID}\nAPCA_API_SECRET_KEY={SECRET}\n"
    )
    with respx.mock() as router:
        route = router.get(TRADING).respond(200, json={"is_open": False})
        out = CliRunner().invoke(app, ["http", "get", TRADING, "--source", "alpaca"])
    assert out.exit_code == 0, out.output
    assert "status: 200" in out.output
    assert route.calls.last.request.headers["APCA-API-KEY-ID"] == KEY_ID
    assert KEY_ID not in out.output and SECRET not in out.output


# --- rate limit -----------------------------------------------------------------------------


def test_each_alpaca_host_has_its_own_bucket_under_200_a_minute():
    hosts = ratelimit.load_hosts(REPO_CONFIG)
    data = hosts.policy("data.alpaca.markets")
    trading = hosts.policy("paper-api.alpaca.markets")
    assert (data.bucket, trading.bucket) == ("alpaca_data", "alpaca_trading")
    assert data.rate <= 3 and trading.rate <= 3
    for policy in (data, trading):  # the committed config never holds a key
        assert not any(k.upper().startswith("APCA") for k in policy.headers)


@respx.mock
def test_429_with_x_ratelimit_reset_waits_until_it(make_client, clock, monkeypatch):
    monkeypatch.setattr(http, "_wall_now", lambda: datetime(2026, 10, 12, 14, 0, 0, tzinfo=UTC))
    reset = int(datetime(2026, 10, 12, 14, 0, 42, tzinfo=UTC).timestamp())
    route = respx.get(DATA).mock(
        side_effect=[
            httpx.Response(429, headers={"X-Ratelimit-Limit": "200", "X-Ratelimit-Reset": str(reset)}),
            httpx.Response(200, json={}),
        ]
    )
    _, resp = make_client().get(DATA, source="alpaca", dataset="t")
    assert route.call_count == 2 and resp.status_code == 200
    assert 42 in clock.sleeps


@respx.mock
def test_x_ratelimit_reset_already_passed_retries_without_backoff(make_client, clock, monkeypatch):
    monkeypatch.setattr(http, "_wall_now", lambda: datetime(2026, 10, 12, 14, 0, 0, tzinfo=UTC))
    reset = int(datetime(2026, 10, 12, 13, 59, 0, tzinfo=UTC).timestamp())
    respx.get(DATA).mock(
        side_effect=[
            httpx.Response(429, headers={"X-Ratelimit-Reset": str(reset)}),
            httpx.Response(200, json={}),
        ]
    )
    make_client().get(DATA, source="alpaca", dataset="t")
    # already reset: no wait of its own and no 1 s backoff (the 3 rps bucket still paces it)
    assert 0 in clock.sleeps and 1.0 not in clock.sleeps


# --- secrets never stored or shown ----------------------------------------------------------


def _blobs(root: Path) -> bytes:
    out = b""
    for p in root.rglob("*"):
        if p.is_file():
            data = p.read_bytes()
            out += gzip.decompress(data) if p.suffix == ".gz" else data
    return out


@respx.mock
def test_the_keys_appear_nowhere_after_archived_requests(con, make_client, base, caplog):
    caplog.set_level(logging.DEBUG)
    respx.get(TRADING).respond(200, json={"is_open": True})
    respx.get(DATA).respond(500, text="upstream")
    client = make_client()
    raw_id, _ = client.get(TRADING, source="alpaca", dataset="alpaca_clock")
    with pytest.raises(http.HttpError) as err:
        client.get(DATA, source="alpaca", dataset="alpaca_bars")
    rows = con.execute("SELECT * FROM raw_response").fetchall()
    assert len(rows) == 2
    text = " ".join(str(r) for r in rows) + str(err.value) + caplog.text
    blobs = _blobs(base.raw_dir)
    for secret in (KEY_ID, SECRET):
        assert secret not in text
        assert secret.encode() not in blobs
    # the request hash covers method, URL and body only: other keys, same hash
    other = make_client(key_id="PKOTHER", secret="otherSecret")
    raw_2, _ = other.get(TRADING, source="alpaca", dataset="alpaca_clock")
    shas = con.execute(
        "SELECT request_sha256 FROM raw_response WHERE id IN (?, ?)", [raw_id, raw_2]
    ).fetchall()
    assert shas[0] == shas[1]


# --- config/alpaca.yaml ---------------------------------------------------------------------


def test_alpaca_yaml_loads_with_every_key_of_the_plan():
    from usfin.sources import alpaca

    cfg = alpaca.load_config(REPO_CONFIG)
    assert set(cfg.exchanges) == {"NYSE", "NASDAQ", "AMEX", "ARCA", "BATS"}
    assert cfg.batch_size == 100
    assert cfg.lookback_days == 5
    assert cfg.backfill_start == date(2016, 1, 4)
    assert cfg.new_symbol_cap == 200
    assert cfg.gap_sessions == 60


def test_a_bad_alpaca_yaml_is_a_config_error(tmp_path):
    from usfin.sources import alpaca

    (tmp_path / "alpaca.yaml").write_text("batch_size: 0\n", "utf-8")
    with pytest.raises(ConfigError, match="batch_size"):
        alpaca.load_config(tmp_path)
    (tmp_path / "alpaca.yaml").write_text("backfill_start: not-a-date\n", "utf-8")
    with pytest.raises(ConfigError, match="backfill_start"):
        alpaca.load_config(tmp_path)


# --- doctor ---------------------------------------------------------------------------------


@pytest.fixture
def doctor_home(tmp_path, monkeypatch):
    for key in (
        "APCA_API_KEY_ID", "APCA_API_SECRET_KEY", "SEC_USER_AGENT", "BLS_CONTACT_EMAIL",
        "USFIN_DB", "USFIN_RAW", "USFIN_CONFIG",
    ):  # fmt: skip
        monkeypatch.delenv(key, raising=False)
    shutil.copytree(REPO_CONFIG, tmp_path / "config")
    (tmp_path / "raw").mkdir()
    monkeypatch.chdir(tmp_path)
    return tmp_path


def _line(output: str, name: str) -> str:
    found = [ln for ln in output.splitlines() if f" {name} " in f" {ln} "]
    assert found, f"no line for {name!r} in:\n{output}"
    return found[0]


def test_doctor_reports_the_keys_as_info_set_and_never_their_values(doctor_home):
    (doctor_home / ".env").write_text(
        f"SEC_USER_AGENT=A b@c.test\nBLS_CONTACT_EMAIL=b@c.test\n"
        f"APCA_API_KEY_ID={KEY_ID}\nAPCA_API_SECRET_KEY={SECRET}\n",
        "utf-8",
    )
    out = CliRunner().invoke(app, ["doctor"])
    for name in ("APCA_API_KEY_ID", "APCA_API_SECRET_KEY"):
        line = _line(out.output, name)
        assert line.split()[0] == "info" and " set " in line, line
    assert KEY_ID not in out.output and SECRET not in out.output
    assert _line(out.output, "alpaca.yaml").split()[0] == "ok"


def test_doctor_unset_keys_are_info_and_do_not_fail(doctor_home):
    (doctor_home / ".env").write_text("SEC_USER_AGENT=A b@c.test\nBLS_CONTACT_EMAIL=b@c.test\n")
    out = CliRunner().invoke(app, ["doctor"])
    for name in ("APCA_API_KEY_ID", "APCA_API_SECRET_KEY"):
        line = _line(out.output, name)
        assert line.split()[0] == "info" and "unset" in line, line
    assert not any(ln.startswith("FAIL") and "APCA" in ln for ln in out.output.splitlines())


def test_doctor_fails_on_a_missing_or_broken_alpaca_yaml(doctor_home):
    (doctor_home / ".env").write_text("SEC_USER_AGENT=A b@c.test\nBLS_CONTACT_EMAIL=b@c.test\n")
    (doctor_home / "config" / "alpaca.yaml").write_text("lookback_days: -1\n", "utf-8")
    out = CliRunner().invoke(app, ["doctor"])
    assert out.exit_code == 1
    assert _line(out.output, "alpaca.yaml").startswith("FAIL")
    (doctor_home / "config" / "alpaca.yaml").unlink()
    out = CliRunner().invoke(app, ["doctor"])
    assert "missing" in _line(out.output, "alpaca.yaml")
