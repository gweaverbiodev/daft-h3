from __future__ import annotations

from typing import Generator

import daft
import pytest
from daft import col
from daft.session import Session

import daft_h3
from daft_h3 import (
    h3_cell_is_valid,
    h3_cell_parent,
    h3_cell_resolution,
    h3_cell_to_lat,
    h3_cell_to_lng,
    h3_cell_to_str,
    h3_grid_disk,
    h3_grid_distance,
    h3_grid_ring,
    h3_latlng_to_cell,
    h3_str_to_cell,
)

SF_LAT = 37.7749
SF_LNG = -122.4194
SF_RES7_HEX = "872830828ffffff"


@pytest.fixture(autouse=True)
def h3_session() -> Generator[Session, None, None]:
    sess = Session()
    sess.load_extension(daft_h3)
    with sess:
        yield sess


class TestH3LatLngToCell:
    def test_basic(self) -> None:
        df = daft.from_pydict({"lat": [SF_LAT], "lng": [SF_LNG]})
        result = (
            df.select(h3_latlng_to_cell(col("lat"), col("lng"), 7))
            .collect()
            .to_pydict()
        )
        assert result["h3_latlng_to_cell"][0] is not None

    def test_null_handling(self) -> None:
        df = daft.from_pydict({"lat": [SF_LAT, None], "lng": [SF_LNG, -122.0]})
        result = (
            df.select(h3_latlng_to_cell(col("lat"), col("lng"), 7))
            .collect()
            .to_pydict()
        )
        assert result["h3_latlng_to_cell"][0] is not None
        assert result["h3_latlng_to_cell"][1] is None


class TestH3CellToLatLng:
    def test_roundtrip_uint64(self) -> None:
        df = daft.from_pydict({"lat": [SF_LAT], "lng": [SF_LNG]})
        df = df.select(
            h3_latlng_to_cell(col("lat"), col("lng"), 7).alias("cell")
        ).collect()
        result = (
            df.select(
                h3_cell_to_lat(col("cell")).alias("out_lat"),
                h3_cell_to_lng(col("cell")).alias("out_lng"),
            )
            .collect()
            .to_pydict()
        )
        assert pytest.approx(result["out_lat"][0], abs=0.01) == SF_LAT
        assert pytest.approx(result["out_lng"][0], abs=0.01) == SF_LNG

    def test_from_string(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        result = (
            df.select(
                h3_cell_to_lat(col("hex")).alias("lat"),
                h3_cell_to_lng(col("hex")).alias("lng"),
            )
            .collect()
            .to_pydict()
        )
        assert pytest.approx(result["lat"][0], abs=0.01) == SF_LAT
        assert pytest.approx(result["lng"][0], abs=0.01) == SF_LNG


class TestH3CellStr:
    def test_roundtrip(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        df = df.select(h3_str_to_cell(col("hex")).alias("cell")).collect()
        df = df.select(h3_cell_to_str(col("cell")).alias("back")).collect()
        assert df.to_pydict()["back"][0] == SF_RES7_HEX

    def test_invalid(self) -> None:
        df = daft.from_pydict({"hex": ["not_a_cell", None]})
        result = df.select(h3_str_to_cell(col("hex"))).collect().to_pydict()
        assert all(v is None for v in result["h3_str_to_cell"])


class TestH3CellInfo:
    def test_resolution(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        result = (
            df.select(h3_cell_resolution(col("hex")).alias("res")).collect().to_pydict()
        )
        assert result["res"][0] == 7

    def test_is_valid(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX, "not_valid", None]})
        result = df.select(h3_cell_is_valid(col("hex"))).collect().to_pydict()
        assert result["h3_cell_is_valid"][0] is True
        assert result["h3_cell_is_valid"][1] is False
        assert result["h3_cell_is_valid"][2] is None


class TestH3CellParent:
    def test_parent_string_preserves_type(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        result = df.select(h3_cell_parent(col("hex"), 5)).collect().to_pydict()
        parent = result["h3_cell_parent"][0]
        assert isinstance(parent, str)

    def test_parent_correct_resolution(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        df = df.select(h3_cell_parent(col("hex"), 5).alias("parent")).collect()
        result = (
            df.select(h3_cell_resolution(col("parent")).alias("res"))
            .collect()
            .to_pydict()
        )
        assert result["res"][0] == 5


class TestH3GridDistance:
    def test_same_cell(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        result = (
            df.select(h3_grid_distance(col("hex"), col("hex")).alias("dist"))
            .collect()
            .to_pydict()
        )
        assert result["dist"][0] == 0


class TestH3GridDisk:
    def test_k0_returns_self(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        df = df.select(h3_str_to_cell(col("hex")).alias("cell")).collect()
        result = (
            df.select(h3_grid_disk(col("cell"), 0).alias("disk")).collect().to_pydict()
        )
        disk = result["disk"][0]
        assert len(disk) == 1
        assert disk[0] == df.to_pydict()["cell"][0]

    def test_k1_uint64_input_returns_uint64_list(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        df = df.select(h3_str_to_cell(col("hex")).alias("cell")).collect()
        result = (
            df.select(h3_grid_disk(col("cell"), 1).alias("disk")).collect().to_pydict()
        )
        disk = result["disk"][0]
        # A non-pentagon cell has 6 neighbors, plus itself = 7.
        assert len(disk) == 7
        assert all(isinstance(c, int) for c in disk)
        # All returned cells should be valid and at resolution 7.
        res = (
            daft.from_pydict({"cell": disk})
            .select(col("cell").cast(daft.DataType.uint64()))
            .select(h3_cell_resolution(col("cell")).alias("r"))
            .collect()
            .to_pydict()["r"]
        )
        assert all(r == 7 for r in res)

    def test_k1_string_input_returns_string_list(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        result = (
            df.select(h3_grid_disk(col("hex"), 1).alias("disk")).collect().to_pydict()
        )
        disk = result["disk"][0]
        assert len(disk) == 7
        assert all(isinstance(c, str) for c in disk)
        # The center cell itself should be part of its k=1 disk.
        assert SF_RES7_HEX in disk
        # All returned cells should be valid and at resolution 7.
        res = (
            daft.from_pydict({"cell": disk})
            .select(h3_cell_resolution(col("cell")).alias("r"))
            .collect()
            .to_pydict()["r"]
        )
        assert all(r == 7 for r in res)

    def test_null_handling(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX, None]})
        result = (
            df.select(h3_grid_disk(col("hex"), 1).alias("disk")).collect().to_pydict()
        )
        assert result["disk"][0] is not None
        assert len(result["disk"][0]) == 7
        assert result["disk"][1] is None


class TestH3GridRing:
    def test_k0_returns_self(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        df = df.select(h3_str_to_cell(col("hex")).alias("cell")).collect()
        result = (
            df.select(h3_grid_ring(col("cell"), 0).alias("ring")).collect().to_pydict()
        )
        ring = result["ring"][0]
        assert len(ring) == 1
        assert ring[0] == df.to_pydict()["cell"][0]

    def test_k1_uint64_input_returns_six_neighbors(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        df = df.select(h3_str_to_cell(col("hex")).alias("cell")).collect()
        center = df.to_pydict()["cell"][0]
        result = (
            df.select(h3_grid_ring(col("cell"), 1).alias("ring")).collect().to_pydict()
        )
        ring = result["ring"][0]
        # A non-pentagon cell has exactly 6 neighbors at distance 1 (no center).
        assert len(ring) == 6
        assert all(isinstance(c, int) for c in ring)
        assert center not in ring

    def test_k1_string_input_returns_six_neighbor_strings(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        result = (
            df.select(h3_grid_ring(col("hex"), 1).alias("ring")).collect().to_pydict()
        )
        ring = result["ring"][0]
        assert len(ring) == 6
        assert all(isinstance(c, str) for c in ring)
        # The center cell itself is NOT part of the ring.
        assert SF_RES7_HEX not in ring
        # All returned cells should be at resolution 7.
        res = (
            daft.from_pydict({"cell": ring})
            .select(h3_cell_resolution(col("cell")).alias("r"))
            .collect()
            .to_pydict()["r"]
        )
        assert all(r == 7 for r in res)

    def test_null_handling(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX, None]})
        result = (
            df.select(h3_grid_ring(col("hex"), 1).alias("ring")).collect().to_pydict()
        )
        assert result["ring"][0] is not None
        assert len(result["ring"][0]) == 6
        assert result["ring"][1] is None
