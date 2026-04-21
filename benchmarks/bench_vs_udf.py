"""Reproducible benchmark: daft-h3 native functions vs daft UDFs wrapping h3-py.

Runs two fair-comparison tables — UInt64 cell inputs (maximum throughput) and
Utf8 hex-string inputs (typical persistence format). Run via ``make bench``.
"""

from __future__ import annotations

import math
import statistics
import time
from dataclasses import dataclass
from typing import Any, Callable

import daft
import h3.api.basic_int as h3i
import h3.api.basic_str as h3s
import numpy as np
from daft import Series, col
from daft.session import Session

import daft_h3

N_ROWS = 1_000_000
RES = 7
PARENT_RES = 5
K = 1
SEED = 42
WARMUP = 2
TRIALS = 5


def _gen_data() -> dict[str, list[Any]]:
    rng = np.random.default_rng(SEED)
    lats = rng.uniform(-89.0, 89.0, N_ROWS).tolist()
    lngs = rng.uniform(-179.0, 179.0, N_ROWS).tolist()
    cells = [h3i.latlng_to_cell(la, ln, RES) for la, ln in zip(lats, lngs)]
    hexes = [h3i.int_to_str(c) for c in cells]
    neighbors = [h3i.grid_ring(c, 1)[0] for c in cells]
    neighbors_hex = [h3i.int_to_str(c) for c in neighbors]
    return {
        "lat": lats,
        "lng": lngs,
        "cell": cells,
        "hex": hexes,
        "cell_b": neighbors,
        "hex_b": neighbors_hex,
    }


# ── UInt64-path UDFs ────────────────────────────────────────────────


@daft.func.batch(return_dtype=daft.DataType.uint64())
def udf_latlng_to_cell(lat: Series, lng: Series) -> list[int]:
    return [
        h3i.latlng_to_cell(la.as_py(), ln.as_py(), RES)
        for la, ln in zip(lat.to_arrow(), lng.to_arrow())
    ]


@daft.func.batch(return_dtype=daft.DataType.float64())
def udf_cell_to_lat(cell: Series) -> list[float]:
    return [h3i.cell_to_latlng(c.as_py())[0] for c in cell.to_arrow()]


@daft.func.batch(return_dtype=daft.DataType.uint64())
def udf_cell_parent(cell: Series) -> list[int]:
    return [h3i.cell_to_parent(c.as_py(), PARENT_RES) for c in cell.to_arrow()]


@daft.func.batch(return_dtype=daft.DataType.uint8())
def udf_cell_resolution(cell: Series) -> list[int]:
    return [h3i.get_resolution(c.as_py()) for c in cell.to_arrow()]


@daft.func.batch(return_dtype=daft.DataType.int32())
def udf_grid_distance(a: Series, b: Series) -> list[int]:
    return [
        h3i.grid_distance(x.as_py(), y.as_py())
        for x, y in zip(a.to_arrow(), b.to_arrow())
    ]


@daft.func.batch(return_dtype=daft.DataType.list(daft.DataType.uint64()))
def udf_grid_disk(cell: Series) -> list[list[int]]:
    return [list(h3i.grid_disk(c.as_py(), K)) for c in cell.to_arrow()]


@daft.func.batch(return_dtype=daft.DataType.list(daft.DataType.uint64()))
def udf_grid_ring(cell: Series) -> list[list[int]]:
    return [list(h3i.grid_ring(c.as_py(), K)) for c in cell.to_arrow()]


@daft.func.batch(return_dtype=daft.DataType.list(daft.DataType.uint64()))
def udf_grid_disk_k3(cell: Series) -> list[list[int]]:
    return [list(h3i.grid_disk(c.as_py(), 3)) for c in cell.to_arrow()]


@daft.func.batch(return_dtype=daft.DataType.list(daft.DataType.uint64()))
def udf_grid_disk_k5(cell: Series) -> list[list[int]]:
    return [list(h3i.grid_disk(c.as_py(), 5)) for c in cell.to_arrow()]


# ── Utf8-path UDFs ──────────────────────────────────────────────────


@daft.func.batch(return_dtype=daft.DataType.float64())
def udf_cell_to_lat_str(hex: Series) -> list[float]:
    return [h3s.cell_to_latlng(h.as_py())[0] for h in hex.to_arrow()]


@daft.func.batch(return_dtype=daft.DataType.string())
def udf_cell_parent_str(hex: Series) -> list[str]:
    return [h3s.cell_to_parent(h.as_py(), PARENT_RES) for h in hex.to_arrow()]


@daft.func.batch(return_dtype=daft.DataType.uint8())
def udf_cell_resolution_str(hex: Series) -> list[int]:
    return [h3s.get_resolution(h.as_py()) for h in hex.to_arrow()]


@daft.func.batch(return_dtype=daft.DataType.uint64())
def udf_str_to_cell(hex: Series) -> list[int]:
    return [h3s.str_to_int(h.as_py()) for h in hex.to_arrow()]


@daft.func.batch(return_dtype=daft.DataType.int32())
def udf_grid_distance_str(a: Series, b: Series) -> list[int]:
    return [
        h3s.grid_distance(x.as_py(), y.as_py())
        for x, y in zip(a.to_arrow(), b.to_arrow())
    ]


@daft.func.batch(return_dtype=daft.DataType.list(daft.DataType.string()))
def udf_grid_disk_str(hex: Series) -> list[list[str]]:
    return [list(h3s.grid_disk(h.as_py(), K)) for h in hex.to_arrow()]


@daft.func.batch(return_dtype=daft.DataType.list(daft.DataType.string()))
def udf_grid_ring_str(hex: Series) -> list[list[str]]:
    return [list(h3s.grid_ring(h.as_py(), K)) for h in hex.to_arrow()]


@daft.func.batch(return_dtype=daft.DataType.list(daft.DataType.string()))
def udf_grid_disk_str_k3(hex: Series) -> list[list[str]]:
    return [list(h3s.grid_disk(h.as_py(), 3)) for h in hex.to_arrow()]


@daft.func.batch(return_dtype=daft.DataType.list(daft.DataType.string()))
def udf_grid_disk_str_k5(hex: Series) -> list[list[str]]:
    return [list(h3s.grid_disk(h.as_py(), 5)) for h in hex.to_arrow()]


@dataclass
class Case:
    name: str
    native: Callable[[daft.DataFrame], daft.DataFrame]
    udf: Callable[[daft.DataFrame], daft.DataFrame]


CASES_UINT64: list[Case] = [
    Case(
        "latlng_to_cell",
        lambda df: df.select(daft_h3.h3_latlng_to_cell(col("lat"), col("lng"), RES)),
        lambda df: df.select(udf_latlng_to_cell(col("lat"), col("lng"))),
    ),
    Case(
        "cell_to_lat",
        lambda df: df.select(daft_h3.h3_cell_to_lat(col("cell"))),
        lambda df: df.select(udf_cell_to_lat(col("cell"))),
    ),
    Case(
        "cell_parent",
        lambda df: df.select(daft_h3.h3_cell_parent(col("cell"), PARENT_RES)),
        lambda df: df.select(udf_cell_parent(col("cell"))),
    ),
    Case(
        "cell_resolution",
        lambda df: df.select(daft_h3.h3_cell_resolution(col("cell"))),
        lambda df: df.select(udf_cell_resolution(col("cell"))),
    ),
    Case(
        "grid_distance",
        lambda df: df.select(daft_h3.h3_grid_distance(col("cell"), col("cell_b"))),
        lambda df: df.select(udf_grid_distance(col("cell"), col("cell_b"))),
    ),
    Case(
        "grid_disk (k=1)",
        lambda df: df.select(daft_h3.h3_grid_disk(col("cell"), K)),
        lambda df: df.select(udf_grid_disk(col("cell"))),
    ),
    Case(
        "grid_disk (k=3)",
        lambda df: df.select(daft_h3.h3_grid_disk(col("cell"), 3)),
        lambda df: df.select(udf_grid_disk_k3(col("cell"))),
    ),
    Case(
        "grid_disk (k=5)",
        lambda df: df.select(daft_h3.h3_grid_disk(col("cell"), 5)),
        lambda df: df.select(udf_grid_disk_k5(col("cell"))),
    ),
    Case(
        "grid_ring (k=1)",
        lambda df: df.select(daft_h3.h3_grid_ring(col("cell"), K)),
        lambda df: df.select(udf_grid_ring(col("cell"))),
    ),
]


CASES_STR: list[Case] = [
    Case(
        "cell_to_lat",
        lambda df: df.select(daft_h3.h3_cell_to_lat(col("hex"))),
        lambda df: df.select(udf_cell_to_lat_str(col("hex"))),
    ),
    Case(
        "cell_parent",
        lambda df: df.select(daft_h3.h3_cell_parent(col("hex"), PARENT_RES)),
        lambda df: df.select(udf_cell_parent_str(col("hex"))),
    ),
    Case(
        "cell_resolution",
        lambda df: df.select(daft_h3.h3_cell_resolution(col("hex"))),
        lambda df: df.select(udf_cell_resolution_str(col("hex"))),
    ),
    Case(
        "str_to_cell",
        lambda df: df.select(daft_h3.h3_str_to_cell(col("hex"))),
        lambda df: df.select(udf_str_to_cell(col("hex"))),
    ),
    Case(
        "grid_distance",
        lambda df: df.select(daft_h3.h3_grid_distance(col("hex"), col("hex_b"))),
        lambda df: df.select(udf_grid_distance_str(col("hex"), col("hex_b"))),
    ),
    Case(
        "grid_disk (k=1)",
        lambda df: df.select(daft_h3.h3_grid_disk(col("hex"), K)),
        lambda df: df.select(udf_grid_disk_str(col("hex"))),
    ),
    Case(
        "grid_disk (k=3)",
        lambda df: df.select(daft_h3.h3_grid_disk(col("hex"), 3)),
        lambda df: df.select(udf_grid_disk_str_k3(col("hex"))),
    ),
    Case(
        "grid_disk (k=5)",
        lambda df: df.select(daft_h3.h3_grid_disk(col("hex"), 5)),
        lambda df: df.select(udf_grid_disk_str_k5(col("hex"))),
    ),
    Case(
        "grid_ring (k=1)",
        lambda df: df.select(daft_h3.h3_grid_ring(col("hex"), K)),
        lambda df: df.select(udf_grid_ring_str(col("hex"))),
    ),
]


def _time_ms(fn: Callable[[], daft.DataFrame]) -> float:
    for _ in range(WARMUP):
        fn().collect()
    times = []
    for _ in range(TRIALS):
        t0 = time.perf_counter()
        fn().collect()
        times.append(time.perf_counter() - t0)
    return statistics.median(times) * 1000


def _equal(native_val: Any, udf_val: Any) -> bool:
    if native_val is None or udf_val is None:
        return native_val is None and udf_val is None
    if isinstance(native_val, list):
        return sorted(native_val) == sorted(udf_val)
    if isinstance(native_val, float):
        return math.isclose(native_val, udf_val, rel_tol=1e-9, abs_tol=1e-12)
    return bool(native_val == udf_val)


def _verify_equality(case: Case, df: daft.DataFrame) -> None:
    native_col = next(iter(case.native(df).to_pydict().values()))
    udf_col = next(iter(case.udf(df).to_pydict().values()))
    if len(native_col) != len(udf_col):
        raise AssertionError(f"{case.name}: length {len(native_col)} vs {len(udf_col)}")
    for i, (nv, uv) in enumerate(zip(native_col, udf_col)):
        if not _equal(nv, uv):
            raise AssertionError(f"{case.name}: row {i}: {nv!r} != {uv!r}")


def _print_table(title: str, cases: list[Case], df: daft.DataFrame) -> None:
    print(f"\n### {title}\n")
    print("| Function | daft-h3 | UDF (h3-py) | Speedup |")
    print("|---|---|---|---|")
    for case in cases:
        _verify_equality(case, df)
        native_ms = _time_ms(lambda: case.native(df))
        udf_ms = _time_ms(lambda: case.udf(df))
        speedup = udf_ms / native_ms if native_ms > 0 else float("inf")
        print(
            f"| `{case.name}` | {native_ms:.0f}ms | {udf_ms:,.0f}ms | **{speedup:.1f}x** |"
        )


def main() -> None:
    with Session() as sess:
        sess.load_extension(daft_h3)
        print(f"Generating {N_ROWS:,} rows (seed={SEED})...")
        u64 = daft.DataType.uint64()
        input_data = _gen_data()
        df = (
            daft.from_pydict(input_data)  # type: ignore[arg-type]
            .with_column("cell", col("cell").cast(u64))
            .with_column("cell_b", col("cell_b").cast(u64))
        )

        print(f"\nBenchmarking (warmup={WARMUP}, trials={TRIALS}, median ms):")
        _print_table("UInt64 inputs", CASES_UINT64, df)
        _print_table("Utf8 (hex-string) inputs", CASES_STR, df)


if __name__ == "__main__":
    main()
