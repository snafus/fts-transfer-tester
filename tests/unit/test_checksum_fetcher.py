"""
Unit tests for fts_framework.checksum.fetcher.

All HTTP calls are mocked via the ``responses`` library.
"""

import base64
import struct

import pytest
import responses as resp_lib
import requests

from fts_framework.checksum.fetcher import (
    fetch_all,
    _fetch_one,
    _parse_digest_header,
    _is_hex_adler32,
    _base64_to_hex,
)
from fts_framework.exceptions import ChecksumFetchError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_session(token="test-token", ssl_verify=True):
    session = requests.Session()
    session.headers["Authorization"] = "Bearer {}".format(token)
    session.verify = ssl_verify
    return session


def _config(workers=4):
    return {"concurrency": {"want_digest_workers": workers}}


def _adler32_as_b64(hex_str):
    """Convert an 8-char hex adler32 to RFC 3230 base64."""
    raw = bytes(bytearray.fromhex(hex_str))
    return base64.b64encode(raw).decode("ascii")


PFN = "https://storage.example.org/data/file001.dat"
HEX_CHECKSUM = "a1b2c3d4"


# ---------------------------------------------------------------------------
# _is_hex_adler32
# ---------------------------------------------------------------------------

class TestIsHexAdler32:
    def test_valid_8_char_hex_lowercase(self):
        assert _is_hex_adler32("a1b2c3d4") is True

    def test_valid_8_char_hex_uppercase(self):
        assert _is_hex_adler32("A1B2C3D4") is True

    def test_valid_8_char_hex_mixed(self):
        assert _is_hex_adler32("A1b2C3d4") is True

    def test_too_short_returns_false(self):
        assert _is_hex_adler32("a1b2c3") is False

    def test_too_long_returns_false(self):
        assert _is_hex_adler32("a1b2c3d4e5") is False

    def test_non_hex_char_returns_false(self):
        assert _is_hex_adler32("g1b2c3d4") is False

    def test_empty_returns_false(self):
        assert _is_hex_adler32("") is False

    def test_base64_string_returns_false(self):
        # base64 of 4 bytes is 8 chars but contains + / =
        assert _is_hex_adler32("obLNpA==") is False


# ---------------------------------------------------------------------------
# _base64_to_hex
# ---------------------------------------------------------------------------

class TestBase64ToHex:
    def test_valid_base64_4_bytes(self):
        raw = bytes([0xa1, 0xb2, 0xc3, 0xd4])
        b64 = base64.b64encode(raw).decode("ascii")
        result = _base64_to_hex(b64)
        assert result == "a1b2c3d4"

    def test_already_padded_input_decodes_correctly(self):
        """Padding normalisation must not corrupt an already-padded value."""
        raw = bytes([0xa1, 0xb2, 0xc3, 0xd4])
        b64 = base64.b64encode(raw).decode("ascii")   # e.g. "obLN pA==" (8 chars with ==)
        result = _base64_to_hex(b64)
        assert result == "a1b2c3d4"

    def test_unpadded_input_decodes_correctly(self):
        """Input without trailing = characters must still decode correctly."""
        raw = bytes([0xa1, 0xb2, 0xc3, 0xd4])
        b64 = base64.b64encode(raw).decode("ascii").rstrip("=")
        result = _base64_to_hex(b64)
        assert result == "a1b2c3d4"

    def test_returns_none_for_wrong_decoded_length(self):
        # 3 bytes → 4 base64 chars → decoded length 3, not 4
        raw = bytes([0xa1, 0xb2, 0xc3])
        b64 = base64.b64encode(raw).decode("ascii")
        assert _base64_to_hex(b64) is None

    def test_returns_none_for_invalid_base64(self):
        assert _base64_to_hex("!!!invalid!!!") is None

    def test_result_is_lowercase(self):
        raw = bytes([0xFF, 0xFF, 0xFF, 0xFF])
        b64 = base64.b64encode(raw).decode("ascii")
        result = _base64_to_hex(b64)
        assert result == "ffffffff"


# ---------------------------------------------------------------------------
# _parse_digest_header
# ---------------------------------------------------------------------------

class TestParseDigestHeader:
    def test_hex_value_returned_lowercase(self):
        result = _parse_digest_header(PFN, "adler32=A1B2C3D4")
        assert result == "a1b2c3d4"

    def test_hex_value_already_lowercase(self):
        assert _parse_digest_header(PFN, "adler32=a1b2c3d4") == "a1b2c3d4"

    def test_base64_value_decoded_to_hex(self):
        b64 = _adler32_as_b64(HEX_CHECKSUM)
        result = _parse_digest_header(PFN, "adler32={}".format(b64))
        assert result == HEX_CHECKSUM

    def test_case_insensitive_prefix(self):
        assert _parse_digest_header(PFN, "ADLER32=a1b2c3d4") == "a1b2c3d4"

    def test_multi_algorithm_header_picks_adler32(self):
        b64 = _adler32_as_b64(HEX_CHECKSUM)
        header = "sha256=AAAA, adler32={}".format(b64)
        result = _parse_digest_header(PFN, header)
        assert result == HEX_CHECKSUM

    def test_no_adler32_in_header_raises(self):
        with pytest.raises(ChecksumFetchError, match="adler32"):
            _parse_digest_header(PFN, "sha256=AAABBBCCC")

    def test_unparseable_value_raises(self):
        with pytest.raises(ChecksumFetchError, match="Cannot parse"):
            _parse_digest_header(PFN, "adler32=!!notvalid!!")

    def test_empty_header_raises(self):
        with pytest.raises(ChecksumFetchError, match="adler32"):
            _parse_digest_header(PFN, "")


# ---------------------------------------------------------------------------
# _fetch_one (mocked HTTP)
# ---------------------------------------------------------------------------

class TestFetchOne:
    @resp_lib.activate
    def test_hex_digest_header(self):
        resp_lib.add(resp_lib.HEAD, PFN, headers={"Digest": "adler32={}".format(HEX_CHECKSUM)}, status=200)
        result = _fetch_one(PFN, _make_session())
        assert result == "adler32:{}".format(HEX_CHECKSUM)

    @resp_lib.activate
    def test_base64_digest_header(self):
        b64 = _adler32_as_b64(HEX_CHECKSUM)
        resp_lib.add(resp_lib.HEAD, PFN, headers={"Digest": "adler32={}".format(b64)}, status=200)
        result = _fetch_one(PFN, _make_session())
        assert result == "adler32:{}".format(HEX_CHECKSUM)

    @resp_lib.activate
    def test_404_raises(self):
        resp_lib.add(resp_lib.HEAD, PFN, status=404)
        with pytest.raises(ChecksumFetchError, match="404"):
            _fetch_one(PFN, _make_session())

    @resp_lib.activate
    def test_500_raises(self):
        resp_lib.add(resp_lib.HEAD, PFN, status=500)
        with pytest.raises(ChecksumFetchError, match="500"):
            _fetch_one(PFN, _make_session())

    @resp_lib.activate
    def test_missing_digest_header_raises(self):
        resp_lib.add(resp_lib.HEAD, PFN, status=200)
        with pytest.raises(ChecksumFetchError, match="no Digest header"):
            _fetch_one(PFN, _make_session())

    def test_connection_error_raises(self):
        with resp_lib.RequestsMock() as rsps:
            rsps.add(resp_lib.HEAD, PFN, body=requests.ConnectionError("timeout"))
            with pytest.raises(ChecksumFetchError, match="HEAD request failed"):
                _fetch_one(PFN, _make_session())

    @resp_lib.activate
    def test_davs_pfn_converted_to_https_for_request(self):
        davs_pfn = "davs://storage.example.org/data/file001.dat"
        https_url = "https://storage.example.org/data/file001.dat"
        resp_lib.add(resp_lib.HEAD, https_url, headers={"Digest": "adler32={}".format(HEX_CHECKSUM)}, status=200)
        result = _fetch_one(davs_pfn, _make_session())
        assert result == "adler32:{}".format(HEX_CHECKSUM)
        assert resp_lib.calls[0].request.url == https_url


# ---------------------------------------------------------------------------
# fetch_all (parallel, mocked HTTP)
# ---------------------------------------------------------------------------

class TestFetchAll:
    @resp_lib.activate
    def test_fetches_all_pfns(self):
        pfns = [
            "https://storage.example.org/data/file{:03d}.dat".format(i)
            for i in range(5)
        ]
        for pfn in pfns:
            resp_lib.add(
                resp_lib.HEAD, pfn,
                headers={"Digest": "adler32={}".format(HEX_CHECKSUM)},
                status=200,
            )
        result = fetch_all(pfns, _make_session(), _config())
        assert len(result) == 5
        for pfn in pfns:
            assert result[pfn] == "adler32:{}".format(HEX_CHECKSUM)

    @resp_lib.activate
    def test_one_failure_raises_with_failing_pfn(self):
        pfns = [
            "https://storage.example.org/data/good.dat",
            "https://storage.example.org/data/bad.dat",
        ]
        resp_lib.add(
            resp_lib.HEAD, pfns[0],
            headers={"Digest": "adler32={}".format(HEX_CHECKSUM)},
            status=200,
        )
        resp_lib.add(resp_lib.HEAD, pfns[1], status=404)
        with pytest.raises(ChecksumFetchError, match="404"):
            fetch_all(pfns, _make_session(), _config(workers=2))

    @resp_lib.activate
    def test_serial_single_worker(self):
        """workers=1 (serial) must produce identical results to workers>1."""
        resp_lib.add(
            resp_lib.HEAD, PFN,
            headers={"Digest": "adler32={}".format(HEX_CHECKSUM)},
            status=200,
        )
        result = fetch_all([PFN], _make_session(), _config(workers=1))
        assert result[PFN] == "adler32:{}".format(HEX_CHECKSUM)

    def test_empty_pfn_list_returns_empty_dict(self):
        result = fetch_all([], _make_session(), _config())
        assert result == {}

    @resp_lib.activate
    def test_single_pfn(self):
        resp_lib.add(
            resp_lib.HEAD, PFN,
            headers={"Digest": "adler32={}".format(HEX_CHECKSUM)},
            status=200,
        )
        result = fetch_all([PFN], _make_session(), _config())
        assert result[PFN] == "adler32:{}".format(HEX_CHECKSUM)
