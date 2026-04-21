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
# Parent of SF_RES7_HEX at resolution 5 (deterministic per H3 spec).
SF_RES5_PARENT_HEX = "85283083fffffff"
# k=1 disk of SF_RES7_HEX: center + 6 neighbors (non-pentagon cell).
SF_RES7_DISK_K1 = {
    "872830828ffffff",
    "872830829ffffff",
    "87283082affffff",
    "87283082bffffff",
    "87283082cffffff",
    "87283082dffffff",
    "87283082effffff",
}
# Base cell 4 at res 0 is one of the 12 icosahedron-vertex pentagons.
PENTAGON_RES0_HEX = "8009fffffffffff"
# k=1 disk of a pentagon: center + 5 neighbors.
PENTAGON_DISK_K1 = {
    "8001fffffffffff",
    "8007fffffffffff",
    "8009fffffffffff",
    "8011fffffffffff",
    "8019fffffffffff",
    "801ffffffffffff",
}


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
            df.select(
                h3_cell_to_str(h3_latlng_to_cell(col("lat"), col("lng"), 7)).alias(
                    "hex"
                )
            )
            .collect()
            .to_pydict()
        )
        assert result["hex"][0] == SF_RES7_HEX

    def test_null_handling(self) -> None:
        df = daft.from_pydict({"lat": [SF_LAT, None], "lng": [SF_LNG, -122.0]})
        result = (
            df.select(
                h3_cell_to_str(h3_latlng_to_cell(col("lat"), col("lng"), 7)).alias(
                    "hex"
                )
            )
            .collect()
            .to_pydict()
        )
        assert result["hex"][0] == SF_RES7_HEX
        assert result["hex"][1] is None


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

    def test_list_uint64_to_list_str(self) -> None:
        # UInt64 compute path + string output: the persistence-friendly pattern.
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        df = df.select(h3_str_to_cell(col("hex")).alias("cell")).collect()
        result = (
            df.select(h3_cell_to_str(h3_grid_disk(col("cell"), 1)).alias("disk"))
            .collect()
            .to_pydict()
        )
        disk = result["disk"][0]
        assert all(isinstance(c, str) for c in disk)
        assert set(disk) == SF_RES7_DISK_K1

    def test_list_str_to_list_str_passthrough(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        result = (
            df.select(h3_cell_to_str(h3_grid_disk(col("hex"), 1)).alias("disk"))
            .collect()
            .to_pydict()
        )
        assert set(result["disk"][0]) == SF_RES7_DISK_K1

    def test_list_null_row_preserved(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX, None]})
        result = (
            df.select(h3_cell_to_str(h3_grid_disk(col("hex"), 1)).alias("disk"))
            .collect()
            .to_pydict()
        )
        assert set(result["disk"][0]) == SF_RES7_DISK_K1
        assert result["disk"][1] is None


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
        assert parent == SF_RES5_PARENT_HEX

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
        assert all(isinstance(c, int) for c in disk)
        # Convert uint64 cells back to hex and compare against the known disk.
        disk_hex = (
            daft.from_pydict({"cell": disk})
            .select(col("cell").cast(daft.DataType.uint64()))
            .select(h3_cell_to_str(col("cell")).alias("h"))
            .collect()
            .to_pydict()["h"]
        )
        assert set(disk_hex) == SF_RES7_DISK_K1

    def test_k1_string_input_returns_string_list(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        result = (
            df.select(h3_grid_disk(col("hex"), 1).alias("disk")).collect().to_pydict()
        )
        disk = result["disk"][0]
        assert all(isinstance(c, str) for c in disk)
        assert set(disk) == SF_RES7_DISK_K1

    def test_null_handling(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX, None]})
        result = (
            df.select(h3_grid_disk(col("hex"), 1).alias("disk")).collect().to_pydict()
        )
        assert result["disk"][0] is not None
        assert len(result["disk"][0]) == 7
        assert result["disk"][1] is None

    def test_uint64_disk_to_str_for_persistence(self) -> None:
        # Compute in UInt64 (fast), convert output list to strings for
        # Iceberg/Trino/Spark-safe persistence, all in one query.
        df = daft.from_pydict({"lat": [SF_LAT], "lng": [SF_LNG]})
        df = df.select(
            h3_latlng_to_cell(col("lat"), col("lng"), 7).alias("cell")
        ).collect()
        result = (
            df.select(h3_cell_to_str(h3_grid_disk(col("cell"), 1)).alias("disk"))
            .collect()
            .to_pydict()
        )
        disk = result["disk"][0]
        assert all(isinstance(c, str) for c in disk)
        assert set(disk) == SF_RES7_DISK_K1

    def test_invalid_input_returns_null(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX, "not_a_cell"]})
        result = (
            df.select(h3_grid_disk(col("hex"), 1).alias("disk")).collect().to_pydict()
        )
        assert len(result["disk"][0]) == 7
        assert result["disk"][1] is None

    def test_pentagon_disk_has_six_cells(self) -> None:
        df = daft.from_pydict({"hex": [PENTAGON_RES0_HEX]})
        result = (
            df.select(h3_grid_disk(col("hex"), 1).alias("disk")).collect().to_pydict()
        )
        # Pentagon has 5 neighbors (not 6), so disk(k=1) = self + 5 = 6.
        assert set(result["disk"][0]) == PENTAGON_DISK_K1

    def test_multi_row_batch(self) -> None:
        df = daft.from_pydict({"lat": [SF_LAT, 0.0], "lng": [SF_LNG, 0.0]})
        df = df.select(
            h3_latlng_to_cell(col("lat"), col("lng"), 7).alias("cell")
        ).collect()
        cells = df.to_pydict()["cell"]
        result = (
            df.select(h3_grid_disk(col("cell"), 1).alias("disk")).collect().to_pydict()
        )
        assert len(result["disk"]) == 2
        assert all(len(d) == 7 for d in result["disk"])
        assert cells[0] in result["disk"][0]
        assert cells[1] in result["disk"][1]

    def test_empty_batch_returns_empty(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX]}).where(
            col("hex") == daft.lit("no_match")
        )
        result = (
            df.select(h3_grid_disk(col("hex"), 1).alias("disk")).collect().to_pydict()
        )
        assert result["disk"] == []


class TestH3GridRing:
    def test_k0_returns_self(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        result = (
            df.select(h3_grid_ring(col("hex"), 0).alias("ring")).collect().to_pydict()
        )
        ring = result["ring"][0]
        assert ring == [SF_RES7_HEX]

    def test_k1_equals_disk_minus_center(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        result = (
            df.select(h3_grid_ring(col("hex"), 1).alias("ring")).collect().to_pydict()
        )
        ring = result["ring"][0]
        assert all(isinstance(c, str) for c in ring)
        assert set(ring) == SF_RES7_DISK_K1 - {SF_RES7_HEX}

    def test_k1_uint64_input_returns_uint64_list(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        df = df.select(h3_str_to_cell(col("hex")).alias("cell")).collect()
        result = (
            df.select(h3_grid_ring(col("cell"), 1).alias("ring")).collect().to_pydict()
        )
        ring = result["ring"][0]
        assert all(isinstance(c, int) for c in ring)
        assert len(ring) == 6

    def test_null_handling(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX, None]})
        result = (
            df.select(h3_grid_ring(col("hex"), 1).alias("ring")).collect().to_pydict()
        )
        assert len(result["ring"][0]) == 6
        assert result["ring"][1] is None

    def test_pentagon_ring_has_five_cells(self) -> None:
        df = daft.from_pydict({"hex": [PENTAGON_RES0_HEX]})
        result = (
            df.select(h3_grid_ring(col("hex"), 1).alias("ring")).collect().to_pydict()
        )
        # Pentagon has 5 neighbors, so ring(k=1) = 5.
        assert set(result["ring"][0]) == PENTAGON_DISK_K1 - {PENTAGON_RES0_HEX}

    def test_empty_batch_returns_empty(self) -> None:
        df = daft.from_pydict({"hex": [SF_RES7_HEX]}).where(
            col("hex") == daft.lit("no_match")
        )
        result = (
            df.select(h3_grid_ring(col("hex"), 1).alias("ring")).collect().to_pydict()
        )
        assert result["ring"] == []


class TestValidation:
    def test_latlng_to_cell_rejects_negative_resolution(self) -> None:
        with pytest.raises(ValueError, match="resolution must be between 0 and 15"):
            h3_latlng_to_cell(col("lat"), col("lng"), -1)

    def test_latlng_to_cell_rejects_out_of_range_resolution(self) -> None:
        with pytest.raises(ValueError, match="resolution must be between 0 and 15"):
            h3_latlng_to_cell(col("lat"), col("lng"), 16)

    def test_cell_parent_rejects_negative_resolution(self) -> None:
        with pytest.raises(ValueError, match="resolution must be between 0 and 15"):
            h3_cell_parent(col("cell"), -1)

    def test_cell_parent_rejects_out_of_range_resolution(self) -> None:
        with pytest.raises(ValueError, match="resolution must be between 0 and 15"):
            h3_cell_parent(col("cell"), 16)

    def test_grid_disk_rejects_negative_k(self) -> None:
        with pytest.raises(ValueError, match="k cannot be negative"):
            h3_grid_disk(col("cell"), -1)

    def test_grid_ring_rejects_negative_k(self) -> None:
        with pytest.raises(ValueError, match="k cannot be negative"):
            h3_grid_ring(col("cell"), -1)
