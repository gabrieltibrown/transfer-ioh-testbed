"""Fetch per-case VitalDB Parquet files into the local (gitignored) data cache.

VitalDB serves one Parquet file per case at ``https://api.vitaldb.net/<caseid>.parquet``
in the same row-per-block shape DWC recordings are expected to arrive in, so the
replay path reads Parquet for both sources.

Only the manifest is committed. The Parquet files themselves stay under ``data/``,
which is gitignored: see the data governance rules in CLAUDE.md.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import sys
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

API = "https://api.vitaldb.net"

# Tracks a case must carry to be usable for IOH work: the arterial waveform,
# the MAP numeric that defines hypotension, and an ECG lead.
REQUIRED_TRACKS = ("SNUADC/ART", "Solar8000/ART_MBP", "SNUADC/ECG_II")


def _fetch(url: str, timeout: int = 300) -> bytes:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        body = r.read()
    # The API gzips responses regardless of Accept-Encoding.
    return gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body


def eligible_caseids() -> list[int]:
    """Case ids carrying every track in REQUIRED_TRACKS, ascending."""
    text = _fetch(f"{API}/trks").decode("utf-8", "replace")
    tracks_by_case: dict[str, set[str]] = defaultdict(set)
    for row in csv.DictReader(io.StringIO(text, newline="")):
        tracks_by_case[row["caseid"]].add(row["tname"])
    ok = [cid for cid, names in tracks_by_case.items() if set(REQUIRED_TRACKS) <= names]
    return sorted(int(c) for c in ok)


def fetch_case(caseid: int, dest: Path) -> dict:
    """Download one case Parquet unless already cached. Returns its manifest entry."""
    path = dest / f"{caseid}.parquet"
    if not path.exists():
        path.write_bytes(_fetch(f"{API}/{caseid}.parquet"))
    raw = path.read_bytes()
    return {
        "caseid": caseid,
        "file": path.name,
        "bytes": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("-n", "--count", type=int, default=50, help="cases to fetch")
    ap.add_argument("--data-dir", type=Path, default=Path("data/vitaldb"))
    ap.add_argument(
        "--manifest",
        type=Path,
        default=Path("src/ioh_testbed/replay/manifest.json"),
        help="committed record of exactly which cases back the experiments",
    )
    args = ap.parse_args()

    args.data_dir.mkdir(parents=True, exist_ok=True)
    caseids = eligible_caseids()
    print(f"eligible cases (have {', '.join(REQUIRED_TRACKS)}): {len(caseids)}", file=sys.stderr)

    selected = caseids[: args.count]
    entries = []
    for i, caseid in enumerate(selected, 1):
        entry = fetch_case(caseid, args.data_dir)
        entries.append(entry)
        print(
            f"[{i}/{len(selected)}] case {caseid}: {entry['bytes'] / 1e6:.1f} MB",
            file=sys.stderr,
        )

    manifest = {
        "source": "VitalDB open dataset (Lee et al. 2022)",
        "api": API,
        "fetched_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "required_tracks": list(REQUIRED_TRACKS),
        "eligible_case_count": len(caseids),
        "selection": "lowest N eligible case ids, ascending, for determinism",
        "cases": entries,
        "total_bytes": sum(e["bytes"] for e in entries),
    }
    args.manifest.write_text(json.dumps(manifest, indent=2) + "\n")
    print(
        f"\n{len(entries)} cases, {manifest['total_bytes'] / 1e9:.2f} GB -> {args.data_dir}",
        file=sys.stderr,
    )
    print(f"manifest -> {args.manifest}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
