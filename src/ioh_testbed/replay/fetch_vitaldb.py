"""Fetch per-case VitalDB Parquet files into the local (gitignored) data cache.

VitalDB serves one Parquet file per case at ``https://api.vitaldb.net/<caseid>.parquet``
in the same row-per-block shape DWC recordings are expected to arrive in, so the
replay path reads Parquet for both sources.

Eligibility is decided on Parquet *content*, not on the ``/trks`` catalogue: the
catalogue lists tracks for some cases whose files contain no such data (cases 3,
14 and 32 at the time of writing). A case is accepted only if its file holds
waveform rows for every track in ``REQUIRED_WAVES`` and numeric rows for every
track in ``REQUIRED_NUMERICS``.

Only the manifest is committed. The Parquet files stay under ``data/``, which is
gitignored: see the data governance rules in CLAUDE.md.
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
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

API = "https://api.vitaldb.net"

# What a case must actually contain to be usable for IOH work: the arterial
# waveform, an ECG lead, and the MAP numeric that defines hypotension.
REQUIRED_WAVES = ("SNUADC/ART", "SNUADC/ECG_II")
REQUIRED_NUMERICS = ("Solar8000/ART_MBP",)


def _fetch(url: str, timeout: int = 300) -> bytes:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        body = r.read()
    # The API gzips responses regardless of Accept-Encoding.
    return gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body


def catalogue_candidates() -> list[int]:
    """Case ids whose catalogue entry lists the required tracks, ascending.

    This is only a cheap pre-filter. Acceptance is decided by ``inspect_case``.
    """
    text = _fetch(f"{API}/trks").decode("utf-8", "replace")
    tracks_by_case: dict[str, set[str]] = defaultdict(set)
    for row in csv.DictReader(io.StringIO(text, newline="")):
        tracks_by_case[row["caseid"]].add(row["tname"])
    want = set(REQUIRED_WAVES) | set(REQUIRED_NUMERICS)
    return sorted(int(c) for c, names in tracks_by_case.items() if want <= names)


def inspect_case(path: Path) -> dict:
    """Summarise what a case file really contains, reading only the light columns."""
    t = pq.read_table(path, columns=["dt", "dname", "tname", "srate", "nval"])
    key = pc.binary_join_element_wise(
        pc.cast(t["dname"], "string"), pc.cast(t["tname"], "string"), "/"
    )
    is_wave = pc.is_valid(t["srate"])
    is_num = pc.and_(pc.is_valid(t["nval"]), pc.invert(is_wave))
    waves = sorted(set(pc.filter(key, is_wave).to_pylist()))
    nums = sorted(set(pc.filter(key, is_num).to_pylist()))
    dt_min, dt_max = pc.min(t["dt"]).as_py(), pc.max(t["dt"]).as_py()
    return {
        "duration_s": round(dt_max - dt_min, 1),
        "wave_tracks": waves,
        "n_wave_tracks": len(waves),
        "n_numeric_tracks": len(nums),
        "has_required_waves": all(w in waves for w in REQUIRED_WAVES),
        "has_required_numerics": all(n in nums for n in REQUIRED_NUMERICS),
    }


def fetch_case(caseid: int, dest: Path) -> Path:
    path = dest / f"{caseid}.parquet"
    if not path.exists():
        path.write_bytes(_fetch(f"{API}/{caseid}.parquet"))
    return path


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("-n", "--count", type=int, default=50, help="accepted cases wanted")
    ap.add_argument("--data-dir", type=Path, default=Path("data/vitaldb"))
    ap.add_argument(
        "--manifest",
        type=Path,
        default=Path("src/ioh_testbed/replay/manifest.json"),
        help="committed record of exactly which cases back the experiments",
    )
    args = ap.parse_args()
    args.data_dir.mkdir(parents=True, exist_ok=True)

    candidates = catalogue_candidates()
    print(f"catalogue candidates: {len(candidates)}", file=sys.stderr)

    accepted: list[dict] = []
    rejected: list[dict] = []
    for caseid in candidates:
        if len(accepted) >= args.count:
            break
        path = fetch_case(caseid, args.data_dir)
        info = inspect_case(path)
        if not (info["has_required_waves"] and info["has_required_numerics"]):
            path.unlink()  # keep the cache honest: only accepted cases on disk
            rejected.append({"caseid": caseid, **{k: info[k] for k in ("has_required_waves", "has_required_numerics")}})
            print(f"  reject case {caseid}: catalogue lists required tracks, file lacks them", file=sys.stderr)
            continue
        raw = path.read_bytes()
        accepted.append({
            "caseid": caseid,
            "file": path.name,
            "bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
            **{k: v for k, v in info.items() if not k.startswith("has_")},
        })
        print(f"  [{len(accepted)}/{args.count}] case {caseid}: {len(raw) / 1e6:.1f} MB, "
              f"{info['duration_s'] / 3600:.2f} h, {info['n_wave_tracks']} wave / {info['n_numeric_tracks']} numeric tracks",
              file=sys.stderr)

    manifest = {
        "source": "VitalDB open dataset (Lee et al. 2022)",
        "api": API,
        "fetched_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "required_waves": list(REQUIRED_WAVES),
        "required_numerics": list(REQUIRED_NUMERICS),
        "selection": "lowest catalogue-eligible case ids, ascending, accepted only if the "
                     "Parquet content holds the required tracks",
        "catalogue_candidate_count": len(candidates),
        "rejected": rejected,
        "cases": accepted,
        "total_bytes": sum(c["bytes"] for c in accepted),
    }
    args.manifest.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"\n{len(accepted)} accepted, {len(rejected)} rejected, "
          f"{manifest['total_bytes'] / 1e9:.2f} GB -> {args.data_dir}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
