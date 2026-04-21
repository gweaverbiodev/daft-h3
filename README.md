# daft-h3

[![CI](https://github.com/gweaverbiodev/daft-h3/actions/workflows/pr-test-suite.yml/badge.svg)](https://github.com/gweaverbiodev/daft-h3/actions) [![PyPI](https://img.shields.io/pypi/v/daft-h3.svg)](https://pypi.org/project/daft-h3) [![Latest Tag](https://img.shields.io/github/v/tag/gweaverbiodev/daft-h3?label=latest&logo=GitHub)](https://github.com/gweaverbiodev/daft-h3/tags)


Native [H3](https://h3geo.org/) geospatial indexing functions for [Daft](https://github.com/Eventual-Inc/Daft).

## Installation

```bash
pip install daft-h3
```

## Usage

```python
import daft
import daft_h3
from daft import col

daft.load_extension(daft_h3)

df = daft.from_pydict({"lat": [37.7749, 48.8566], "lng": [-122.4194, 2.3522]})
df = df.select(daft_h3.h3_latlng_to_cell(col("lat"), col("lng"), 7).alias("cell"))
df = df.select(daft_h3.h3_cell_to_str(col("cell")).alias("hex"))
df.show()
```

All cell-input functions accept both UInt64 and Utf8 (hex string) columns, so you can operate directly on string H3 data without an explicit conversion step. Functions that return cell indices preserve the input type: string in, string out.

For maximum throughput, convert to UInt64 once at the top of a pipeline with `h3_str_to_cell` and operate on integers throughout.

`h3_cell_to_str` also accepts a list column of cells and returns `List(Utf8)`, so you can compute in UInt64 and persist as strings (useful for Iceberg/Trino/Spark, which lack unsigned types and will reinterpret UInt64 cell IDs as negative signed values):

```python
df = df.select(
    h3_cell_to_str(h3_grid_disk(col("cell"), 1)).alias("neighbors_hex")
)
```

## Functions

| Function | Input | Output |
|---|---|---|
| `h3_latlng_to_cell` | lat (f64), lng (f64), resolution (0-15) | UInt64 |
| `h3_cell_to_lat` | cell (UInt64 or Utf8) | Float64 |
| `h3_cell_to_lng` | cell (UInt64 or Utf8) | Float64 |
| `h3_cell_to_str` | cell or List of cells (UInt64 or Utf8) | Utf8 or List(Utf8) |
| `h3_str_to_cell` | hex (Utf8) | UInt64 |
| `h3_cell_resolution` | cell (UInt64 or Utf8) | UInt8 |
| `h3_cell_is_valid` | cell (UInt64 or Utf8) | Boolean |
| `h3_cell_parent` | cell (UInt64 or Utf8), resolution (0-15) | same as input |
| `h3_grid_distance` | a (UInt64 or Utf8), b (UInt64 or Utf8) | Int32 |
| `h3_grid_disk` | cell (UInt64 or Utf8), k (≥ 0) | List(UInt64 or Utf8) |
| `h3_grid_ring` | cell (UInt64 or Utf8), k (≥ 0) | List(UInt64 or Utf8) |

Invalid cell indices produce null. Resolution is validated at plan time.

## Performance

Benchmarked against wrapping the `h3` Python library in a `@daft.func.batch` UDF (1M rows, median of 5 trials after 2 warmup runs). Reproduce with `make bench`.

### UInt64 inputs

| Function | daft-h3 | UDF (h3-py) | Speedup |
|---|---|---|---|
| `latlng_to_cell` | 46ms | 1,024ms | **22.5x** |
| `cell_to_lat` | 36ms | 649ms | **18.0x** |
| `cell_parent` | 4ms | 378ms | **96.5x** |
| `cell_resolution` | 4ms | 322ms | **71.7x** |
| `grid_distance` | 17ms | 712ms | **42.7x** |
| `grid_disk (k=1)` | 79ms | 3,851ms | **48.5x** |
| `grid_disk (k=3)` | 226ms | 9,898ms | **43.7x** |
| `grid_disk (k=5)` | 381ms | 20,633ms | **54.2x** |
| `grid_ring (k=1)` | 52ms | 3,589ms | **69.3x** |

### Utf8 (hex-string) inputs

| Function | daft-h3 | UDF (h3-py) | Speedup |
|---|---|---|---|
| `cell_to_lat` | 38ms | 853ms | **22.4x** |
| `cell_parent` | 22ms | 740ms | **33.8x** |
| `cell_resolution` | 5ms | 555ms | **103.1x** |
| `str_to_cell` | 5ms | 536ms | **98.5x** |
| `grid_distance` | 21ms | 1,174ms | **55.8x** |
| `grid_disk (k=1)` | 122ms | 6,408ms | **52.6x** |
| `grid_disk (k=3)` | 487ms | 17,974ms | **36.9x** |
| `grid_disk (k=5)` | 1204ms | 37,988ms | **31.6x** |
| `grid_ring (k=1)` | 105ms | 5,905ms | **56.4x** |

## Development

```bash
git clone https://github.com/gweaverbiodev/daft-h3.git
cd daft-h3
uv sync --extra dev --extra test
make install-hooks
```

Requires Rust (stable), Python >= 3.10, and [uv](https://docs.astral.sh/uv/).

```bash
make install-hooks  # Install pre-commit git hooks
make develop        # Install the extension in editable mode (uv pip install -e)
make check          # Fast Rust type-check (cargo check)
make build          # Release build (cargo build --release)
make lint           # Check formatting and types
make format         # Auto-fix and format
make test           # Run tests
```
