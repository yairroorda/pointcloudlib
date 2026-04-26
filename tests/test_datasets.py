from pathlib import Path

import geopandas as gpd

from cloudfetch.datasets import CanElevation


def test_can_elevation_get_index_extracts_urls(dummy_aoi_gdf, tmp_path: Path, monkeypatch) -> None:
    provider = CanElevation(data_dir=tmp_path)

    monkeypatch.setattr(provider, "_download_index", lambda: tmp_path / "nrcan_tile_index.gpkg")

    fake_index = gpd.GeoDataFrame(
        {
            "URL": ["https://fake.laz", "https://fake2.laz"],
            "Year": [2020, 2019],
        },
        geometry=[dummy_aoi_gdf.geometry.iloc[0], dummy_aoi_gdf.geometry.iloc[0]],
        crs="EPSG:4617",
    )

    monkeypatch.setattr(gpd, "read_file", lambda *args, **kwargs: fake_index)

    urls = provider.get_index(dummy_aoi_gdf)

    assert urls == ["https://fake.laz", "https://fake2.laz"]
