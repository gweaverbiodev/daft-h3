"""H3 Geospatial Indexing Functions for Daft.

Functions for [H3](https://h3geo.org/), Uber's hierarchical hexagonal
geospatial indexing system.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import daft

if TYPE_CHECKING:
    from daft.expressions import Expression


def h3_latlng_to_cell(lat: Expression, lng: Expression, resolution: int) -> Expression:
    """Converts latitude/longitude coordinates to an H3 cell index at the given resolution (0-15)."""
    if resolution < 0 or resolution > 15:
        raise ValueError("h3_latlng_to_cell: resolution must be between 0 and 15")

    return daft.get_function(
        "h3_latlng_to_cell", lat, lng, daft.lit(resolution).cast(daft.DataType.uint8())
    )


def h3_cell_to_lat(cell: Expression) -> Expression:
    """Returns the latitude of the center of an H3 cell.

    Args:
        cell: H3 cell index (UInt64 or Utf8 hex string).
    """
    return daft.get_function("h3_cell_to_lat", cell)


def h3_cell_to_lng(cell: Expression) -> Expression:
    """Returns the longitude of the center of an H3 cell.

    Args:
        cell: H3 cell index (UInt64 or Utf8 hex string).
    """
    return daft.get_function("h3_cell_to_lng", cell)


def h3_cell_to_str(cell: Expression) -> Expression:
    """Converts an H3 cell index to its hex string representation.

    Accepts either a scalar cell column (UInt64, Utf8, LargeUtf8) or a list
    column of cells (List[UInt64], List[Utf8], List[LargeUtf8]); returns
    Utf8 or List[Utf8] respectively. The list form lets pipelines compute in
    UInt64 and persist the output as strings without an element-wise loop.

    Args:
        cell: H3 cell index or list of cell indices.
    """
    return daft.get_function("h3_cell_to_str", cell)


def h3_str_to_cell(hex: Expression) -> Expression:
    """Converts an H3 hex string to a cell index (UInt64).

    For maximum throughput, convert to UInt64 once at the top of a pipeline
    and operate on integers throughout, rather than passing hex strings to
    every function call.
    """
    return daft.get_function("h3_str_to_cell", hex)


def h3_cell_resolution(cell: Expression) -> Expression:
    """Returns the resolution (0-15) of an H3 cell index.

    Args:
        cell: H3 cell index (UInt64 or Utf8 hex string).
    """
    return daft.get_function("h3_cell_resolution", cell)


def h3_cell_is_valid(cell: Expression) -> Expression:
    """Returns true if the value is a valid H3 cell index.

    Args:
        cell: H3 cell index (UInt64 or Utf8 hex string).
    """
    return daft.get_function("h3_cell_is_valid", cell)


def h3_cell_parent(cell: Expression, resolution: int) -> Expression:
    """Returns the parent cell index at the given resolution.

    The output type matches the input type: string in, string out.

    Args:
        cell: H3 cell index (UInt64 or Utf8 hex string).
        resolution: Target resolution (0-15). Must be coarser (lower) than the cell's resolution.
    """
    if resolution < 0 or resolution > 15:
        raise ValueError("h3_cell_parent: resolution must be between 0 and 15")

    return daft.get_function(
        "h3_cell_parent", cell, daft.lit(resolution).cast(daft.DataType.uint8())
    )


def h3_grid_distance(a: Expression, b: Expression) -> Expression:
    """Returns the grid distance (number of H3 cells) between two cells.

    Returns null if cells are not comparable (different resolutions or pentagonal distortion).

    Args:
        a: H3 cell index (UInt64 or Utf8 hex string).
        b: H3 cell index (UInt64 or Utf8 hex string).
    """
    return daft.get_function("h3_grid_distance", a, b)


def h3_grid_disk(cell: Expression, k: int) -> Expression:
    """Returns all H3 cells within grid distance ``k`` of ``cell`` (inclusive).

    Returns list of cell indices; the list element type mirrors the input
    (UInt64 in → List[UInt64]; Utf8 in → List[Utf8]). For k=0 this is a
    single-element list containing ``cell`` itself; for k>=1 it includes
    ``cell`` plus all neighbors out to distance k.

    Args:
        cell: H3 cell index (UInt64 or Utf8 hex string).
        k: Grid distance radius (non-negative).
    """
    if k < 0:
        raise ValueError("h3_grid_disk: k cannot be negative")

    return daft.get_function(
        "h3_grid_disk", cell, daft.lit(k).cast(daft.DataType.uint32())
    )
