from __future__ import annotations

import os
import threading
from collections import OrderedDict
from io import BytesIO
from pathlib import Path

import mercantile
import numpy as np
import xarray as xr
from PIL import Image

from .app import App



TILE_SIZE = 256
DILATION_RADIUS = 1
MAX_CACHE_LIMIT = 512

RAIN_BINS = np.array([0.0, 5.0, 10.0, 20.0, 30.0, 40.0, 50.0, np.inf], dtype=np.float32)
RECURRENCE_BINS = np.array([1, 2, 3, 4, 5, 6, 8, np.inf], dtype=np.float32)

COLORS_RGBA = np.array(
    [
        (0, 0, 255, 230),      # blue
        (0, 100, 0, 255),      # dark green
        (255, 255, 0, 255),    # yellow
        (255, 165, 0, 255),    # orange
        (255, 99, 71, 255),    # tomato
        (178, 34, 34, 255),    # firebrick
        (139, 0, 0, 255),      # dark red
    ],
    dtype=np.uint8,
)

_INIT_LOCK = threading.Lock()
_RECURRENCE_LOCK = threading.Lock()

_DS: xr.Dataset | None = None
_RAIN = None
_LON_FLAT: np.ndarray | None = None
_LAT_FLAT: np.ndarray | None = None
_TIMES_ISO: list[str] | None = None
_NT: int | None = None

_VALID_TIME_INDICES: np.ndarray | None = None
_VALID_TIMES_ISO: list[str] | None = None

_RECURRENCE_COUNTS: np.ndarray | None = None
_RECURRENCE_MAX_COUNT: int | None = None

_WEST: float | None = None
_EAST: float | None = None
_SOUTH: float | None = None
_NORTH: float | None = None

_TRANSPARENT_TILE_BYTES: bytes | None = None
_MAX_CACHE: OrderedDict[int, tuple[float | None, float | None, float | None]] = OrderedDict()

def get_zarr_path():
    app_workspace_path = App.get_app_workspace().path
    DEFAULT_ZARR_PATH = os.path.join(app_workspace_path, "unifed", "mrms_single_basin_stable.zarr")
    ZARR_PATH = os.environ.get("MRMS_ZARR_PATH", DEFAULT_ZARR_PATH)
    return Path(ZARR_PATH)

def _build_transparent_tile() -> bytes:
    image = Image.fromarray(np.zeros((TILE_SIZE, TILE_SIZE, 4), dtype=np.uint8), mode="RGBA")
    buffer = BytesIO()
    image.save(buffer, format="PNG", optimize=True)
    return buffer.getvalue()


def _dilate_grid(grid: np.ndarray) -> np.ndarray:
    if DILATION_RADIUS != 1:
        return grid

    dilated = np.maximum.reduce(
        [
            grid,
            np.roll(grid, 1, axis=0),
            np.roll(grid, -1, axis=0),
            np.roll(grid, 1, axis=1),
            np.roll(grid, -1, axis=1),
            np.roll(np.roll(grid, 1, axis=0), 1, axis=1),
            np.roll(np.roll(grid, 1, axis=0), -1, axis=1),
            np.roll(np.roll(grid, -1, axis=0), 1, axis=1),
            np.roll(np.roll(grid, -1, axis=0), -1, axis=1),
        ]
    )

    dilated[0, :] = -1
    dilated[-1, :] = -1
    dilated[:, 0] = -1
    dilated[:, -1] = -1
    return dilated


def _init_once() -> None:
    global _DS, _RAIN, _LON_FLAT, _LAT_FLAT, _TIMES_ISO, _NT
    global _VALID_TIME_INDICES, _VALID_TIMES_ISO
    global _WEST, _EAST, _SOUTH, _NORTH, _TRANSPARENT_TILE_BYTES

    if _DS is not None:
        return

    with _INIT_LOCK:
        if _DS is not None:
            return

        zarr_path = get_zarr_path()
        if not zarr_path.exists():
            raise FileNotFoundError(f"MRMS Zarr not found: {zarr_path}")

        try:
            dataset = xr.open_zarr(str(zarr_path), consolidated=True)
        except Exception:
            dataset = xr.open_zarr(str(zarr_path), consolidated=False)

        required = {"rain", "lon", "lat", "time"}
        missing = required.difference(set(dataset.variables))
        if missing:
            raise KeyError(
                f"Zarr is missing required variables: {sorted(missing)}. "
                f"Available variables: {sorted(dataset.variables)}"
            )

        rain = dataset["rain"]
        lon_flat = np.asarray(dataset["lon"].values, dtype=np.float32).reshape(-1)
        lat_flat = np.asarray(dataset["lat"].values, dtype=np.float32).reshape(-1)

        times = dataset["time"].values
        times_iso = [str(np.datetime64(t)) for t in times]
        nt = int(dataset.sizes["time"])

        spatial_dims = tuple(dim for dim in rain.dims if dim != "time")
        max_per_time = np.asarray(
            rain.max(dim=spatial_dims, skipna=True).values,
            dtype=np.float32,
        ).reshape(-1)
        valid_mask = np.isfinite(max_per_time) & (max_per_time > 0)
        valid_time_indices = np.flatnonzero(valid_mask).astype(np.int32)
        valid_times_iso = [times_iso[i] for i in valid_time_indices.tolist()]

        finite_mask = np.isfinite(lon_flat) & np.isfinite(lat_flat)
        if finite_mask.any():
            lon_ok = lon_flat[finite_mask]
            lat_ok = lat_flat[finite_mask]
            west = float(lon_ok.min())
            east = float(lon_ok.max())
            south = float(lat_ok.min())
            north = float(lat_ok.max())
        else:
            west = east = south = north = None

        _DS = dataset
        _RAIN = rain
        _LON_FLAT = lon_flat
        _LAT_FLAT = lat_flat
        _TIMES_ISO = times_iso
        _NT = nt

        _VALID_TIME_INDICES = valid_time_indices
        _VALID_TIMES_ISO = valid_times_iso

        _WEST = west
        _EAST = east
        _SOUTH = south
        _NORTH = north

        _TRANSPARENT_TILE_BYTES = _build_transparent_tile()


def _transparent_png_bytes() -> bytes:
    _init_once()
    return _TRANSPARENT_TILE_BYTES  # type: ignore[return-value]


def _rain_time_to_1d(time_index: int) -> np.ndarray:
    _init_once()

    array_1d = np.asarray(_RAIN.isel(time=time_index).values, dtype=np.float32).reshape(-1)
    expected_size = _LON_FLAT.size  # type: ignore[union-attr]

    if array_1d.size != expected_size:
        raise ValueError(
            f"Grid mismatch: rain(t) has {array_1d.size} values but lon/lat have {expected_size} values."
        )

    return array_1d


def _render_flat_values_to_tile(
    values_1d: np.ndarray,
    bins: np.ndarray,
    z: int,
    x: int,
    y: int,
) -> bytes:
    _init_once()

    bounds = mercantile.bounds(x, y, z)
    west, south, east, north = bounds.west, bounds.south, bounds.east, bounds.north

    lon = _LON_FLAT
    lat = _LAT_FLAT

    spatial_mask = (lon >= west) & (lon <= east) & (lat >= south) & (lat <= north)
    if not spatial_mask.any():
        return _transparent_png_bytes()

    valid_mask = spatial_mask & np.isfinite(values_1d) & (values_1d > 0)
    if not valid_mask.any():
        return _transparent_png_bytes()

    lon_t = lon[valid_mask]
    lat_t = lat[valid_mask]
    values_t = values_1d[valid_mask]

    px = (lon_t - west) / (east - west) * (TILE_SIZE - 1)
    py = (north - lat_t) / (north - south) * (TILE_SIZE - 1)

    ix = np.clip(px.astype(np.int32), 0, TILE_SIZE - 1)
    iy = np.clip(py.astype(np.int32), 0, TILE_SIZE - 1)

    bin_index = np.clip(
        np.digitize(values_t, bins, right=False) - 1,
        0,
        len(COLORS_RGBA) - 1,
    ).astype(np.int16)

    grid = np.full((TILE_SIZE, TILE_SIZE), -1, dtype=np.int16)
    flat_index = iy * TILE_SIZE + ix
    np.maximum.at(grid.reshape(-1), flat_index, bin_index)

    rgba = np.zeros((TILE_SIZE, TILE_SIZE, 4), dtype=np.uint8)
    active = _dilate_grid(grid) >= 0
    dilated_grid = _dilate_grid(grid)
    rgba[active] = COLORS_RGBA[dilated_grid[active]]

    image = Image.fromarray(rgba, mode="RGBA")
    buffer = BytesIO()
    image.save(buffer, format="PNG", optimize=True)
    return buffer.getvalue()


def _compute_recurrence_counts() -> tuple[np.ndarray, int]:
    _init_once()

    valid_time_indices = _VALID_TIME_INDICES
    pixel_count = _LON_FLAT.size  # type: ignore[union-attr]

    if valid_time_indices.size == 0:
        counts = np.zeros(pixel_count, dtype=np.int32)
        return counts, 0

    rain_valid = np.asarray(
        _RAIN.isel(time=valid_time_indices).values,
        dtype=np.float32,
    ).reshape(valid_time_indices.size, pixel_count)

    positive = np.isfinite(rain_valid) & (rain_valid > 0)
    masked = np.where(positive, rain_valid, -np.inf)
    max_pixel_index_per_time = np.argmax(masked, axis=1)

    counts = np.bincount(max_pixel_index_per_time, minlength=pixel_count).astype(np.int32)
    max_count = int(counts.max(initial=0))
    return counts, max_count


def _get_recurrence_counts() -> np.ndarray:
    global _RECURRENCE_COUNTS, _RECURRENCE_MAX_COUNT

    _init_once()
    if _RECURRENCE_COUNTS is not None:
        return _RECURRENCE_COUNTS

    with _RECURRENCE_LOCK:
        if _RECURRENCE_COUNTS is None:
            counts, max_count = _compute_recurrence_counts()
            _RECURRENCE_COUNTS = counts
            _RECURRENCE_MAX_COUNT = max_count

    return _RECURRENCE_COUNTS


def get_mrms_meta() -> dict[str, object]:
    _init_once()
    recurrence_counts = _get_recurrence_counts()

    return {
        "nt": int(_NT),
        "times_iso": _TIMES_ISO,
        "valid_time_indices": _VALID_TIME_INDICES.tolist(),
        "valid_times_iso": _VALID_TIMES_ISO,
        "zarr_path": get_zarr_path(),
        "n_pixels": int(_LON_FLAT.size),
        "west": _WEST,
        "east": _EAST,
        "south": _SOUTH,
        "north": _NORTH,
        "recurrence_max_count": int(recurrence_counts.max(initial=0)),
        "n_valid_times": int(_VALID_TIME_INDICES.size),
    }


def render_tile_png(time_index: int, z: int, x: int, y: int) -> bytes:
    _init_once()

    if time_index < 0 or time_index >= int(_NT):
        return _transparent_png_bytes()

    values_1d = _rain_time_to_1d(time_index)
    return _render_flat_values_to_tile(values_1d=values_1d, bins=RAIN_BINS, z=z, x=x, y=y)


def render_recurrence_tile_png(z: int, x: int, y: int) -> bytes:
    counts = _get_recurrence_counts().astype(np.float32, copy=False)
    return _render_flat_values_to_tile(values_1d=counts, bins=RECURRENCE_BINS, z=z, x=x, y=y)


def value_at_latlon(time_index: int, lon: float, lat: float) -> float | None:
    _init_once()

    if time_index < 0 or time_index >= int(_NT):
        return None

    values_1d = _rain_time_to_1d(time_index)
    dx = _LON_FLAT - float(lon)
    dy = _LAT_FLAT - float(lat)
    nearest_index = int(np.argmin(dx * dx + dy * dy))

    value = values_1d[nearest_index]
    return float(value) if np.isfinite(value) else None


def recurrence_at_latlon(lon: float, lat: float) -> int:
    _init_once()

    counts = _get_recurrence_counts()
    dx = _LON_FLAT - float(lon)
    dy = _LAT_FLAT - float(lat)
    nearest_index = int(np.argmin(dx * dx + dy * dy))
    return int(counts[nearest_index])


def max_pixel_at_time(time_index: int) -> tuple[float | None, float | None, float | None]:
    _init_once()

    ti = int(time_index)
    if ti < 0 or ti >= int(_NT):
        return (None, None, None)

    cached = _MAX_CACHE.get(ti)
    if cached is not None:
        _MAX_CACHE.move_to_end(ti)
        return cached

    values_1d = _rain_time_to_1d(ti)
    valid = np.isfinite(values_1d) & (values_1d > 0)

    if valid.any():
        masked = np.where(valid, values_1d, -np.inf)
        max_index = int(np.argmax(masked))
        result = (
            float(_LON_FLAT[max_index]),
            float(_LAT_FLAT[max_index]),
            float(values_1d[max_index]),
        )
    else:
        result = (None, None, None)

    _MAX_CACHE[ti] = result
    _MAX_CACHE.move_to_end(ti)

    while len(_MAX_CACHE) > MAX_CACHE_LIMIT:
        _MAX_CACHE.popitem(last=False)

    return result