"""URL construction, cache skip, retries and missing-file handling (no network)."""

import io
import urllib.error
from datetime import date

import pytest

from mm import download_bybit as dl


def test_urls_and_depth_switch():
    assert dl.ob_depth_for(date(2025, 8, 20)) == 500
    assert dl.ob_depth_for(date(2025, 8, 21)) == 200
    assert dl.ob_url("BTCUSDT", date(2026, 11, 9), 200) == (
        "https://quote-saver.bycsi.com/orderbook/linear/BTCUSDT/2026-11-09_BTCUSDT_ob200.data.zip")
    assert dl.trades_url("BTCUSDT", date(2026, 11, 9)) == (
        "https://public.bybit.com/trading/BTCUSDT/BTCUSDT2026-11-09.csv.gz")


def test_default_range_is_last_7_complete_days():
    assert dl.default_range(date(2026, 11, 10)) == (date(2026, 11, 3), date(2026, 11, 9))


class FakeResp(io.BytesIO):
    def __init__(self, body, ctype="application/octet-stream", length=None):
        super().__init__(body)
        self.headers = {"Content-Type": ctype, "Content-Length": str(len(body) if length is None else length)}

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


def test_cache_skips_existing_file(tmp_path, monkeypatch):
    dest = tmp_path / "f.zip"
    dest.write_bytes(b"done")
    monkeypatch.setattr(dl.urllib.request, "urlopen", lambda *a, **k: pytest.fail("network used"))
    assert dl.fetch("http://x/f.zip", dest) == "cached"


def test_retries_then_succeeds(tmp_path, monkeypatch):
    calls = []

    def urlopen(req, timeout):
        calls.append(1)
        if len(calls) == 1:
            raise urllib.error.URLError("conn reset")
        if len(calls) == 2:
            return FakeResp(b"abc", length=10)  # truncated body
        return FakeResp(b"payload")

    monkeypatch.setattr(dl.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(dl.time, "sleep", lambda s: None)
    dest = tmp_path / "f.zip"
    assert dl.fetch("http://x/f.zip", dest, retries=3) == "downloaded"
    assert dest.read_bytes() == b"payload" and len(calls) == 3
    assert not (tmp_path / "f.zip.part").exists()


def test_404_and_html_are_not_found(tmp_path, monkeypatch):
    def e404(req, timeout):
        raise urllib.error.HTTPError(req.full_url, 404, "nf", {}, None)

    monkeypatch.setattr(dl.urllib.request, "urlopen", e404)
    with pytest.raises(dl.NotFound):
        dl.fetch("http://x/a", tmp_path / "a")
    monkeypatch.setattr(dl.urllib.request, "urlopen", lambda r, timeout: FakeResp(b"<html>", "text/html"))
    with pytest.raises(dl.NotFound):
        dl.fetch("http://x/b", tmp_path / "b")
    assert list(tmp_path.iterdir()) == []


def test_gives_up_after_retries(tmp_path, monkeypatch):
    def boom(req, timeout):
        raise urllib.error.URLError("down")

    monkeypatch.setattr(dl.urllib.request, "urlopen", boom)
    monkeypatch.setattr(dl.time, "sleep", lambda s: None)
    with pytest.raises(IOError, match="after 2 attempts"):
        dl.fetch("http://x/c", tmp_path / "c", retries=2)
