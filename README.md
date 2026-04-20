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

## Functions

| Function | Input | Output |
|---|---|---|
| `h3_latlng_to_cell` | lat (f64), lng (f64), resolution (0-15) | UInt64 |
| `h3_cell_to_lat` | cell (UInt64 or Utf8) | Float64 |
| `h3_cell_to_lng` | cell (UInt64 or Utf8) | Float64 |
| `h3_cell_to_str` | cell (UInt64 or Utf8) | Utf8 |
| `h3_str_to_cell` | hex (Utf8) | UInt64 |
| `h3_cell_resolution` | cell (UInt64 or Utf8) | UInt8 |
| `h3_cell_is_valid` | cell (UInt64 or Utf8) | Boolean |
| `h3_cell_parent` | cell (UInt64 or Utf8), resolution (0-15) | same as input |
| `h3_grid_distance` | a (UInt64 or Utf8), b (UInt64 or Utf8) | Int32 |

Invalid cell indices produce null. Resolution is validated at plan time.

## Performance

Benchmarked against wrapping the `h3` Python library in a `@daft.func.batch` UDF (1M rows, hex string columns, Apple M-series):

| Function | daft-h3 | UDF (h3-py) | Speedup |
|---|---|---|---|
| `latlng_to_cell` | 416ms | 1,407ms | **3.4x** |
| `cell_to_lat` | 242ms | 1,011ms | **4.2x** |
| `cell_parent` | 75ms | 1,072ms | **14.4x** |
| `cell_resolution` | 47ms | 767ms | **16.4x** |
| `str_to_cell` | 46ms | 756ms | **16.6x** |

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
