from __future__ import annotations

from datetime import datetime
from pathlib import Path
import shutil
from time import perf_counter

from ..utils.s3_utils import download_flood_alert_inputs

from mrms_usgs_events.ews.state_rain import build_current_state_rain_npz
from mrms_usgs_events.ews.current_alerts import compute_current_alerts_for_state
from mrms_usgs_events.ews.tethys_outputs import export_basin_alerts_geojson




def get_times_from_run_id(run_id: str) -> tuple[str, str]:
    """
    Get start/end datetimes from a run identifier.
    """
    try:
        dates = run_id.split("_")
        start_str = "_".join(dates[0:2])
        end_str = "_".join(dates[2:4])
        start_dt = datetime.strptime(start_str, "%Y%m%d_%H%M%S")
        end_dt = datetime.strptime(end_str, "%Y%m%d_%H%M%S")
        return start_dt.isoformat(), end_dt.isoformat()
    except Exception as e:
        raise ValueError(f"Invalid run_id format: {run_id}") from e

def build_run_id(start_dt: str, end_dt: str) -> str:
    """
    Build a stable run identifier from start/end datetimes.
    """
    start_fmt = datetime.fromisoformat(start_dt).strftime("%Y%m%d_%H%M%S")
    end_fmt = datetime.fromisoformat(end_dt).strftime("%Y%m%d_%H%M%S")

    return f"{start_fmt}_{end_fmt}"


def build_run_directory(
    base_dir: Path,
    state: str,
    start_dt: str,
    end_dt: str,
) -> Path:
    """
    Build the output directory for a Flood Alert run.
    """
    run_id = build_run_id(start_dt, end_dt)

    run_dir = (
        Path(base_dir)
        / "flood_alert_runs"
        / state.upper()
        / run_id
    )

    run_dir.mkdir(parents=True, exist_ok=True)

    return run_dir

def run_flood_alert_pipeline(
    *,
    base_dir: Path,
    state: str,
    start: str,
    end: str,
    workers: int = 4,
) -> dict:
    state = state.upper()
    base_dir = Path(base_dir)

    run_id = build_run_id(start, end)
    run_dir = build_run_directory(base_dir, state, start, end)

    print("=" * 100, flush=True)
    print("TETHYS FLOOD ALERT PIPELINE", flush=True)
    print("=" * 100, flush=True)
    print(f"state   : {state}", flush=True)
    print(f"start   : {start}", flush=True)
    print(f"end     : {end}", flush=True)
    print(f"workers : {workers}", flush=True)
    print(f"base_dir: {base_dir}", flush=True)
    print(f"run_id  : {run_id}", flush=True)
    print(f"run_dir : {run_dir}", flush=True)

    total_t0 = perf_counter()

    t0 = perf_counter()
    inputs = download_flood_alert_inputs(
        base_dir=base_dir,
        state=state,
        workers=workers,
    )
    print(f"[TIME] download inputs: {perf_counter() - t0:.2f} sec", flush=True)

    t1 = perf_counter()
    current_rain_npz = base_dir / "current_rain" / f"{state}_{run_id}_current_rain.npz"

    build_current_state_rain_npz(
        state=state,
        state_mask_fp=inputs["state_mask_fp"],
        out_npz=current_rain_npz,
        base_dir=base_dir,
        start=start,
        end=end,
        workers=workers,
    )
    print(f"[TIME] build current rain: {perf_counter() - t1:.2f} sec", flush=True)

    t2 = perf_counter()

    efficient_event_reference_fp = (
    base_dir
    / "hydro_history"
    / "state_efficient_event_reference"
    / f"{state}_efficient_event_reference.npz"

    )

    alerts_result = compute_current_alerts_for_state(
        state=state,
        current_rain_npz=current_rain_npz,
        state_basin_index_npz=inputs["state_basin_index_fp"],
        pixel_event_index_npz=inputs["pixel_event_index_fp"],
        efficient_event_reference_npz=efficient_event_reference_fp,
        out_dir=base_dir / "ews_alerts",
        max_pixels_per_basin_output=250,
        workers=workers,
    )


    print(f"[TIME] compute alerts: {perf_counter() - t2:.2f} sec", flush=True)

    t3 = perf_counter()

    alerts_dir = base_dir / "ews_alerts" / state
    basin_alerts_parquet = alerts_dir / "basin_alerts.parquet"
    pixel_alerts_parquet = alerts_dir / "pixel_alerts.parquet"

    basin_geojson = run_dir / "basin_alerts.geojson"
    basin_csv = run_dir / "basin_alerts.csv"
    pixel_csv = run_dir / "pixel_alerts.csv"

    export_basin_alerts_geojson(
    state=state,
    base_dir=base_dir,
    basin_alerts_parquet=basin_alerts_parquet,
    out_geojson=basin_geojson,
    relevant_levels=["SEVERE", "WARNING"],
    max_features=300,
    )

    # Keep lightweight copies for the run folder.
    # Pixel GeoJSON is intentionally NOT generated here.
    if (alerts_dir / "basin_alerts.csv").exists():
        shutil.copy2(alerts_dir / "basin_alerts.csv", basin_csv)

    if (alerts_dir / "pixel_alerts.csv").exists():
        shutil.copy2(alerts_dir / "pixel_alerts.csv", pixel_csv)

    print(f"[TIME] export basin geojson only: {perf_counter() - t3:.2f} sec", flush=True)
    print(f"[TIME] total pipeline: {perf_counter() - total_t0:.2f} sec", flush=True)

    return {
        "state": state,
        "start": start,
        "end": end,
        "workers": workers,
        "base_dir": str(base_dir),
        "run_id": run_id,
        "run_dir": str(run_dir),
        "current_rain_npz": str(current_rain_npz),
        "inputs": {k: str(v) for k, v in inputs.items()},
        "alerts": {k: str(v) for k, v in alerts_result.items()},
        "exports": {
            "basin_geojson": str(basin_geojson),
            "basin_csv": str(basin_csv),
            "pixel_alerts_parquet": str(pixel_alerts_parquet),
            "pixel_csv": str(pixel_csv),
            "out_dir": str(run_dir),
        },
    }

def run_flood_alert_pipeline_background(base_dir, state, start_dt, end_dt, workers, lock_fp, done_fp):
    print(f"[BG] start; lock={lock_fp} done={done_fp}", flush=True)
    print(f"[BG] lock exists at entry: {lock_fp.exists()}", flush=True)
    try:
        result = run_flood_alert_pipeline(
            base_dir=base_dir,
            state=state,
            start=start_dt,
            end=end_dt,
            workers=workers,
        )
        print(f"[BG] pipeline run_dir={result['run_dir']}", flush=True)
        print(f"[BG] done_fp dir ={done_fp.parent}", flush=True)
        done_fp.write_text("done\n", encoding="utf-8")
        print(f"[BG] wrote done: {done_fp.exists()}", flush=True)


    except Exception as e:
        print(f"Flood alert pipeline failed: {e}")
    finally:
        try:
            lock_fp.unlink(missing_ok=True)
        except Exception as e:
            print(f"Failed to remove lock file: {e}")