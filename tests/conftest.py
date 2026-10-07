"""Shared fixtures. Tests never read data/ (gitignored); they build VitalDB-shaped
Parquet in a temp dir so the suite runs anywhere."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

REPO = Path(__file__).resolve().parents[1]
VITALDB_DIR = REPO / "data" / "vitaldb"


def write_vitaldb_case(path: Path, waves=None, numerics=None) -> Path:
    """Write a Parquet file with VitalDB's row-per-block schema.

    waves:    {"SNUADC/ART": {"srate": 500, "gain": 0.98, "bias": 31861.0, "unit": "mmHg",
                              "blocks": [(dt, [int or None, ...]) , ...]}}
              A block list value of None writes a null array (whole block missing).
              If "gain" is absent the samples are written to fvals (float), as BIS does.
    numerics: {"Solar8000/HR": {"unit": "/min", "points": [(dt, value), ...]}}
    """
    dt, dname, tname, unit, ivals, fvals, nval, sval, srate, gain, bias = ([] for _ in range(11))

    def row(_dt, _dname, _tname, _unit, _ivals=None, _fvals=None, _nval=None, _srate=None, _gain=None, _bias=None):
        dt.append(_dt); dname.append(_dname); tname.append(_tname); unit.append(_unit)
        ivals.append(_ivals); fvals.append(_fvals); nval.append(_nval); sval.append(None)
        srate.append(_srate); gain.append(_gain); bias.append(_bias)

    for key, spec in (waves or {}).items():
        d, t = key.split("/")
        as_float = "gain" not in spec
        for block_dt, samples in spec["blocks"]:
            if as_float:
                row(block_dt, d, t, spec.get("unit"), _fvals=samples, _srate=float(spec["srate"]))
            else:
                row(block_dt, d, t, spec.get("unit"), _ivals=samples, _srate=float(spec["srate"]),
                    _gain=float(spec["gain"]), _bias=float(spec["bias"]))
    for key, spec in (numerics or {}).items():
        d, t = key.split("/")
        for p_dt, v in spec["points"]:
            row(p_dt, d, t, spec.get("unit"), _nval=float(v))

    table = pa.table({
        "dt": pa.array(dt, pa.float64()),
        "dname": pa.array(dname, pa.string()),
        "tname": pa.array(tname, pa.string()),
        "unit": pa.array(unit, pa.string()),
        "ivals": pa.array(ivals, pa.list_(pa.int16())),
        "fvals": pa.array(fvals, pa.list_(pa.float32())),
        "nval": pa.array(nval, pa.float64()),
        "sval": pa.array(sval, pa.string()),
        "srate": pa.array(srate, pa.float64()),
        "gain": pa.array(gain, pa.float64()),
        "bias": pa.array(bias, pa.float64()),
    })
    pq.write_table(table, path, compression="zstd")
    return path


@pytest.fixture
def vitaldb_case_factory(tmp_path):
    def make(name="case.parquet", **kw) -> Path:
        return write_vitaldb_case(tmp_path / name, **kw)
    return make


def blocks(t0: float, srate: float, n_blocks: int, block_len: int | None = None, value=1000, start_dt_step=1.0):
    """Contiguous 1 s blocks of a constant value, like VitalDB stores them."""
    n = block_len or int(srate)
    return [(t0 + i * start_dt_step, [value] * n) for i in range(n_blocks)]


@pytest.fixture
def real_case_path():
    """Path to cached VitalDB case 1, or skip. Only for tests marked ``vitaldb``."""
    p = VITALDB_DIR / "1.parquet"
    if not p.exists():
        pytest.skip("cached VitalDB data not present under data/vitaldb")
    return p
