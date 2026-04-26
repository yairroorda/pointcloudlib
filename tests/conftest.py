import geopandas as gpd
import pytest
from shapely.geometry import Polygon


@pytest.fixture
def dummy_polygon_wgs84() -> Polygon:
    return Polygon(
        [
            (4.8950, 52.3702),
            (4.8960, 52.3702),
            (4.8960, 52.3710),
            (4.8950, 52.3710),
            (4.8950, 52.3702),
        ]
    )


@pytest.fixture
def dummy_polygon_rdnew() -> Polygon:
    return Polygon(
        [
            (121000.0, 487000.0),
            (121100.0, 487000.0),
            (121100.0, 487100.0),
            (121000.0, 487100.0),
            (121000.0, 487000.0),
        ]
    )


@pytest.fixture
def dummy_aoi_gdf(dummy_polygon_wgs84: Polygon) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(geometry=[dummy_polygon_wgs84], crs="EPSG:4326")
