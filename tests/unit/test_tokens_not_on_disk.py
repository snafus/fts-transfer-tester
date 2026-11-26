"""Tokens must never appear in any file under ``runs/<run_id>/``.

Drives the real payload builder, persistence layer and resume path (only the
HTTP calls are faked) and then greps every file the run directory holds.
"""

import json
import os

import pytest

import fts_framework.runner as runner_mod
from fts_framework.fts.submission import build_payload, restore_storage_tokens
from fts_framework.persistence import store
from fts_framework.resume import controller as resume

RUN_ID = "run-tokens"
_SECRETS = {
    "fts_submit":  "SECRET_fts_submit_eyJhbGciOiJSUzI1NiJ9.aaa.bbb",
    "source_read": "SECRET_source_read_eyJhbGciOiJSUzI1NiJ9.ccc.ddd",
    "dest_write":  "SECRET_dest_write_eyJhbGciOiJSUzI1NiJ9.eee.fff",
}


def _config(destination_naming="sequential"):
    return {
        "run": {"test_label": "tokens", "run_id": RUN_ID},
        "fts": {"endpoint": "https://fts.example.org:8446", "ssl_verify": True},
        "tokens": dict(_SECRETS),
        "transfer": {
            "chunk_size": 2,
            "checksum_algorithm": "adler32",
            "verify_checksum": "both",
            "destination_naming": destination_naming,
            "nostreams": 4,
        },
        "submission": {"scan_window_s": 300},
        "retry": {"fts_retry_max": 2},
    }


def _mapping(n=3):
    return [
        ("https://src.example.org/data/f{}".format(i),
         "https://dst.example.org/data/tokens/testfile_{:06d}".format(i))
        for i in range(n)
    ]


def _files_holding_secrets(run_dir):
    """Return {relative path: [role, ...]} for files containing a token."""
    hits = {}
    scanned = 0
    for root, _, names in os.walk(run_dir):
        for name in names:
            path = os.path.join(root, name)
            scanned += 1
            with open(path, "rb") as fh:
                data = fh.read()
            roles = [r for r, tok in _SECRETS.items() if tok.encode("ascii") in data]
            if roles:
                hits[os.path.relpath(path, run_dir)] = roles
    assert scanned, "nothing was written under {}".format(run_dir)
    return hits


class _PostRecorder(object):
    """Stands in for FTSClient: empty pre-scan, then accepts every POST."""

    def __init__(self):
        self.posted = []

    def get(self, path, **kwargs):
        return []

    def post(self, path, payload=None, **kwargs):
        self.posted.append(payload)
        return _Response({"job_id": "job-{}".format(len(self.posted))})


class _Response(object):
    status_code = 200

    def __init__(self, body):
        self._body = body

    def json(self):
        return self._body


@pytest.fixture
def submitted(tmp_path, monkeypatch):
    """Run the real submit loop for a fresh campaign; return what was POSTed."""
    runs_dir = str(tmp_path)
    config = _config()
    posted = []

    def _fake_submit(client, payload, cfg, run_id, chunk_index, retry_round):
        posted.append(payload)
        return "job-{}".format(chunk_index)

    monkeypatch.setattr(runner_mod, "submit_with_500_recovery", _fake_submit)
    store.init_run_directory(RUN_ID, config, runs_dir=runs_dir)
    store.write_manifest(RUN_ID, _mapping(), config, runs_dir=runs_dir)
    subjobs = runner_mod._submit_chunks(
        _mapping(), {}, config, RUN_ID, 0, None, runs_dir,
    )
    store.update_manifest(RUN_ID, subjobs, runs_dir=runs_dir)
    return runs_dir, config, posted


def test_fresh_submission_leaves_no_token_on_disk(submitted):
    runs_dir, _, posted = submitted
    # The tokens did go out on the wire...
    assert len(posted) == 2
    for payload in posted:
        for entry in payload["files"]:
            assert entry["source_tokens"] == [_SECRETS["source_read"]]
            assert entry["destination_tokens"] == [_SECRETS["dest_write"]]
    # ...and nowhere under the run directory.
    assert _files_holding_secrets(os.path.join(runs_dir, RUN_ID)) == {}


def test_persisted_payload_differs_from_posted_only_in_tokens(submitted):
    runs_dir, config, posted = submitted
    payload_dir = os.path.join(runs_dir, RUN_ID, "submitted_payloads")
    for chunk_index, sent in enumerate(posted):
        name = "chunk_{:04d}_r0.json".format(chunk_index)
        with open(os.path.join(payload_dir, name)) as fh:
            on_disk = json.load(fh)
        assert on_disk != sent
        assert restore_storage_tokens(on_disk, config) == sent


@pytest.mark.parametrize("destination_naming", ["sequential", "uuid", "date"])
def test_resume_repost_sends_tokens_and_leaves_none_on_disk(
        tmp_path, destination_naming):
    """Crash between persisting a payload and POSTing it, then resume."""
    runs_dir = str(tmp_path)
    config = _config(destination_naming)
    store.init_run_directory(RUN_ID, config, runs_dir=runs_dir)
    store.write_manifest(RUN_ID, _mapping(2), config, runs_dir=runs_dir)
    payload = build_payload(_mapping(2), {}, config, RUN_ID, 0, 0)
    store.write_payload(RUN_ID, 0, 0, payload, runs_dir=runs_dir)

    client = _PostRecorder()
    subjobs = resume.load(RUN_ID, client, config, runs_dir=runs_dir)

    assert [s["job_id"] for s in subjobs] == ["job-1"]
    assert len(client.posted) == 1
    for entry in client.posted[0]["files"]:
        assert entry["source_tokens"] == [_SECRETS["source_read"]]
        assert entry["destination_tokens"] == [_SECRETS["dest_write"]]
    assert client.posted[0]["params"] == payload["params"]
    assert _files_holding_secrets(os.path.join(runs_dir, RUN_ID)) == {}


def test_resume_uses_tokens_refreshed_since_the_original_attempt(tmp_path):
    runs_dir = str(tmp_path)
    config = _config()
    store.init_run_directory(RUN_ID, config, runs_dir=runs_dir)
    store.write_manifest(RUN_ID, _mapping(2), config, runs_dir=runs_dir)
    store.write_payload(
        RUN_ID, 0, 0, build_payload(_mapping(2), {}, config, RUN_ID, 0, 0),
        runs_dir=runs_dir,
    )

    config["tokens"]["source_read"] = "fresh_source_token"
    config["tokens"]["dest_write"] = "fresh_dest_token"
    client = _PostRecorder()
    resume.load(RUN_ID, client, config, runs_dir=runs_dir)

    for entry in client.posted[0]["files"]:
        assert entry["source_tokens"] == ["fresh_source_token"]
        assert entry["destination_tokens"] == ["fresh_dest_token"]
