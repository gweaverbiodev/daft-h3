from __future__ import annotations

import pytest

import daft
import daft_h3
from daft import col
from daft.session import Session
from daft_h3 import (
    h3_cell_is_valid,
    h3_cell_parent,
    h3_cell_resolution,
    h3_cell_to_lat,
    h3_cell_to_lng,
    h3_cell_to_str,
    h3_grid_distance,
    h3_latlng_to_cell,
    h3_str_to_cell,
)

SF_LAT = 37.7749
SF_LNG = -122.4194
SF_RES7_HEX = "872830828ffffff"


@pytest.fixture(autouse=True)
def h3_session():
    sess = Session()
    sess.load_extension(daft_h3)
    with sess:
        yield sess


class TestH3LatLngToCell:
    def test_basic(self):
        df = daft.from_pydict({"lat": [SF_LAT], "lng": [SF_LNG]})
        result = df.select(h3_latlng_to_cell(col("lat"), col("lng"), 7)).collect().to_pydict()
        assert result["h3_latlng_to_cell"][0] is not None

    def test_null_handling(self):
        df = daft.from_pydict({"lat": [SF_LAT, None], "lng": [SF_LNG, -122.0]})
        result = df.select(h3_latlng_to_cell(col("lat"), col("lng"), 7)).collect().to_pydict()
        assert result["h3_latlng_to_cell"][0] is not None
        assert result["h3_latlng_to_cell"][1] is None


class TestH3CellToLatLng:
    def test_roundtrip_uint64(self):
        df = daft.from_pydict({"lat": [SF_LAT], "lng": [SF_LNG]})
        df = df.select(h3_latlng_to_cell(col("lat"), col("lng"), 7).alias("cell")).collect()
        result = df.select(
            h3_cell_to_lat(col("cell")).alias("out_lat"),
            h3_cell_to_lng(col("cell")).alias("out_lng"),
        ).collect().to_pydict()
        assert pytest.approx(result["out_lat"][0], abs=0.01) == SF_LAT
        assert pytest.approx(result["out_lng"][0], abs=0.01) == SF_LNG

    def test_from_string(self):
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        result = df.select(
            h3_cell_to_lat(col("hex")).alias("lat"),
            h3_cell_to_lng(col("hex")).alias("lng"),
        ).collect().to_pydict()
        assert pytest.approx(result["lat"][0], abs=0.01) == SF_LAT
        assert pytest.approx(result["lng"][0], abs=0.01) == SF_LNG


class TestH3CellStr:
    def test_roundtrip(self):
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        df = df.select(h3_str_to_cell(col("hex")).alias("cell")).collect()
        df = df.select(h3_cell_to_str(col("cell")).alias("back")).collect()
        assert df.to_pydict()["back"][0] == SF_RES7_HEX

    def test_invalid(self):
        df = daft.from_pydict({"hex": ["not_a_cell", None]})
        result = df.select(h3_str_to_cell(col("hex"))).collect().to_pydict()
        assert all(v is None for v in result["h3_str_to_cell"])


class TestH3CellInfo:
    def test_resolution(self):
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        result = df.select(h3_cell_resolution(col("hex")).alias("res")).collect().to_pydict()
        assert result["res"][0] == 7

    def test_is_valid(self):
        df = daft.from_pydict({"hex": [SF_RES7_HEX, "not_valid", None]})
        result = df.select(h3_cell_is_valid(col("hex"))).collect().to_pydict()
        assert result["h3_cell_is_valid"][0] is True
        assert result["h3_cell_is_valid"][1] is False
        assert result["h3_cell_is_valid"][2] is None


class TestH3CellParent:
    def test_parent_string_preserves_type(self):
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        result = df.select(h3_cell_parent(col("hex"), 5)).collect().to_pydict()
        parent = result["h3_cell_parent"][0]
        assert isinstance(parent, str)

    def test_parent_correct_resolution(self):
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        df = df.select(h3_cell_parent(col("hex"), 5).alias("parent")).collect()
        result = df.select(h3_cell_resolution(col("parent")).alias("res")).collect().to_pydict()
        assert result["res"][0] == 5


class TestH3GridDistance:
    def test_same_cell(self):
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        result = df.select(
            h3_grid_distance(col("hex"), col("hex")).alias("dist")
        ).collect().to_pydict()
        assert result["dist"][0] == 0