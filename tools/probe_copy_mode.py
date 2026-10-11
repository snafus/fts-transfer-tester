#!/usr/bin/env python3
"""
One-off probe: submit one 1-file FTS3 job per HTTP-TPC copy mode and report
what FTS3 returns.  Answers, before any framework change:

  1. Does FTS3 keep ``?copy_mode=<mode>`` on the destination URL it reports?
  2. Does each mode succeed against this source/destination pair?
  3. What does a failure reason look like (for later parsing)?

Uses the framework's config loader and sessions, so token/OIDC and x509
configs both work.  Tokens are never printed.  Raw responses are saved under
--out (default: probe_copy_mode_<timestamp>/).

Usage:
  python probe_copy_mode.py CONFIG SOURCE_URL [--x509-proxy PATH]
                            [--modes pull push] [--timeout 3600] [--cleanup]
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime

from fts_framework.config import loader
from fts_framework.fts.client import FTSClient, build_role_session

_TERMINAL = ("FINISHED", "FAILED", "FINISHEDDIRTY", "CANCELED")


def _save(out_dir, name, data):
    with open(os.path.join(out_dir, name), "w") as fh:
        json.dump(data, fh, indent=2, sort_keys=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("config")
    ap.add_argument("source_url", help="one small source file (https:// or davs://)")
    ap.add_argument("--x509-proxy", default=None)
    ap.add_argument("--modes", nargs="+", default=["pull", "push"],
                    choices=["pull", "push"])
    ap.add_argument("--timeout", type=int, default=3600)
    ap.add_argument("--cleanup", action="store_true",
                    help="DELETE the probe destination files afterwards")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    config = loader.load(args.config, x509_proxy=args.x509_proxy)
    endpoint = config["fts"]["endpoint"]
    client = FTSClient(endpoint, build_role_session(config, "fts_submit"))
    tokens = config.get("tokens") or {}
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = args.out or "probe_copy_mode_{}".format(stamp)
    os.makedirs(out_dir)

    dst_prefix = config["transfer"].get("dst_prefix")
    if not dst_prefix:
        sys.exit("config must set transfer.dst_prefix")
    dst_base = "{}/copy_mode_probe".format(dst_prefix.rstrip("/"))

    print("auth mode : {}".format((config.get("auth") or {}).get("mode", "token")))
    print("whoami    : {}".format(client.get("/whoami").get("user_dn")))

    # Report source size so a huge file is noticed before waiting on it.
    try:
        head = build_role_session(config, "source_read").head(
            args.source_url, allow_redirects=True, timeout=30)
        print("source    : HTTP {} size={}".format(
            head.status_code, head.headers.get("Content-Length", "?")))
    except Exception as exc:
        print("source    : HEAD failed ({})".format(exc))

    jobs = {}
    for mode in args.modes:
        clean_dst = "{}/probe_{}_{}".format(dst_base, mode, stamp)
        entry = {
            "sources": [args.source_url],
            "destinations": ["{}?copy_mode={}".format(clean_dst, mode)],
        }
        if tokens.get("source_read"):
            entry["source_tokens"] = [tokens["source_read"]]
        if tokens.get("dest_write"):
            entry["destination_tokens"] = [tokens["dest_write"]]
        payload = {
            "files": [entry],
            "params": {
                "overwrite": True,
                "verify_checksum": "none",
                "retry": 0,  # one attempt: the reason reflects this mode only
                "priority": config["transfer"].get("priority", 3),
                "activity": config["transfer"].get("activity") or "default",
                "job_metadata": {"probe": "copy_mode", "copy_mode": mode},
            },
        }
        resp = client.post("/jobs", payload)
        if resp.status_code not in (200, 201):
            print("[{}] submit FAILED: HTTP {} {}".format(
                mode, resp.status_code, resp.text[:500]))
            continue
        job_id = resp.json()["job_id"]
        jobs[mode] = (job_id, clean_dst)
        print("[{}] submitted job {}".format(mode, job_id))

    deadline = time.time() + args.timeout
    pending = dict(jobs)
    while pending and time.time() < deadline:
        time.sleep(15)
        for mode, (job_id, _) in list(pending.items()):
            state = client.get("/jobs/{}".format(job_id)).get("job_state")
            if state in _TERMINAL:
                print("[{}] job {} -> {}".format(mode, job_id, state))
                del pending[mode]
    for mode in pending:
        print("[{}] still running after --timeout; results below are partial".format(mode))

    print()
    for mode, (job_id, clean_dst) in jobs.items():
        job = client.get("/jobs/{}".format(job_id))
        files = client.get("/jobs/{}/files".format(job_id))
        _save(out_dir, "{}_job.json".format(mode), job)
        _save(out_dir, "{}_files.json".format(mode), files)
        f = files[0] if files else {}
        dest_surl = f.get("dest_surl", "")
        print("=== {} (job {}) ===".format(mode, job_id))
        print("  file_state     : {}".format(f.get("file_state")))
        print("  dest_surl      : {}".format(dest_surl))
        print("  query echoed   : {}".format(
            "YES" if "copy_mode={}".format(mode) in dest_surl else "NO"))
        print("  throughput     : {} MiB/s   tx_duration: {} s".format(
            f.get("throughput"), f.get("tx_duration")))
        print("  transfer_host  : {}".format(f.get("transfer_host")))
        print("  reason         : {}".format(f.get("reason") or "-"))
        log_keys = sorted(k for k in f if "log" in k.lower())
        print("  log fields     : {}".format(
            ", ".join("{}={}".format(k, f.get(k)) for k in log_keys) or "none"))

        if args.cleanup and f.get("file_state") == "FINISHED":
            r = build_role_session(config, "dest_write").delete(clean_dst, timeout=60)
            print("  cleanup DELETE : HTTP {}".format(r.status_code))
        print()

    print("Raw responses saved in {}/".format(out_dir))


if __name__ == "__main__":
    main()
