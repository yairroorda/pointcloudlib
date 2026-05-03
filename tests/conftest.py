import geopandas as gpd
import pytest
from shapely.geometry import Polygon


@pytest.fixture
def dummy_polygon_wgs84() -> Polygon:
    return Polygon([
        (4.8950, 52.3702),
        (4.8960, 52.3702),
        (4.8960, 52.3710),
        (4.8950, 52.3710),
        (4.8950, 52.3702),
    ])


@pytest.fixture
def dummy_polygon_rdnew() -> Polygon:
    return Polygon([
        (121000.0, 487000.0),
        (121100.0, 487000.0),
        (121100.0, 487100.0),
        (121000.0, 487100.0),
        (121000.0, 487000.0),
    ])


@pytest.fixture
def dummy_aoi_gdf(dummy_polygon_wgs84: Polygon) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(geometry=[dummy_polygon_wgs84], crs="EPSG:4326")


# Tkinter mock classes for headless testing
class MockTk:
    """Mock Tkinter Tk root window."""

    def __init__(self):
        self.children = {}
        self.tk = self

    def title(self, name):
        pass

    def geometry(self, spec):
        pass

    def pack(self, **kw):
        pass

    def mainloop(self):
        pass

    def destroy(self):
        pass


class MockFrame:
    """Mock Tkinter Frame widget."""

    def __init__(self, master, **kwargs):
        self.master = master

    def pack(self, **kw):
        pass


class MockLabel:
    """Mock Tkinter Label widget."""

    def __init__(self, master, **kwargs):
        self.text = kwargs.get("text", "")

    def pack(self, **kw):
        pass


class MockMapWidget:
    """Mock TkinterMapView map widget."""

    def __init__(self):
        self.on_click = None

    def pack(self, **kw):
        pass

    def set_position(self, lat, lon):
        pass

    def set_zoom(self, level):
        pass

    def add_left_click_map_command(self, fn):
        self.on_click = fn


@pytest.fixture
def mock_tkinter(monkeypatch):
    """Fixture to mock Tkinter and map widget for headless testing."""

    import tkinter as tk

    # Disable X11 display requirement for headless CI
    monkeypatch.setenv("DISPLAY", "")

    monkeypatch.setattr(tk, "Tk", MockTk)
    monkeypatch.setattr(tk, "Frame", MockFrame)
    monkeypatch.setattr(tk, "Label", MockLabel)
