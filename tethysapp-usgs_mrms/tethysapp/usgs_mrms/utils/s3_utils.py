import boto3
import os
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from ..app import App

MAX_WORKERS = 4


def get_bucket():
    key = App.get_custom_setting("s3_key")
    secret = App.get_custom_setting("s3_secret")
    region_name = App.get_custom_setting("s3_region")
    bucket_name = App.get_custom_setting("bucket_name")
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        region_name=region_name,
    )
    return s3.Bucket(bucket_name)

def download_s3_file_if_missing(*, s3_key: str, local_fp: Path) -> Path:
    local_fp = Path(local_fp)
    local_fp.parent.mkdir(parents=True, exist_ok=True)

    if local_fp.exists() and local_fp.stat().st_size > 0:
        print(f"[SKIP] exists: {local_fp}", flush=True)
        return local_fp

    bucket = get_bucket()
    print(f"[DOWNLOAD] s3://{bucket.name}/{s3_key} -> {local_fp}", flush=True)
    bucket.download_file(s3_key, str(local_fp))
    return local_fp

def _download_one_json(obj_key: str, local_fp: Path) -> Path:
    local_fp = Path(local_fp)
    local_fp.parent.mkdir(parents=True, exist_ok=True)

    if local_fp.exists() and local_fp.stat().st_size > 0:
        return local_fp

    bucket = get_bucket()
    bucket.download_file(obj_key, str(local_fp))
    return local_fp

def _download_one_file(obj_key, local_path):
    bucket = get_bucket()
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    bucket.download_file(obj_key, local_path)


def _download_files_parallel(download_jobs, max_workers=MAX_WORKERS):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(_download_one_file, obj_key, local_path)
            for obj_key, local_path in download_jobs
        ]

        for future in as_completed(futures):
            future.result()


def download_basin_geojson_files(state_name, destination_path):
    bucket = get_bucket()
    prefix = f"basins_json/{state_name}"
    dest = f"{destination_path}/basin_json_downloaded_files"
    state_dest = f"{dest}/{state_name}"

    os.makedirs(dest, exist_ok=True)

    if os.path.exists(state_dest):
        return

    objects = [
        obj for obj in bucket.objects.filter(Prefix=prefix)
        if obj.key.endswith(".json")
    ]

    if not objects:
        raise FileNotFoundError(
            f"No basin JSON files found in S3 for state {state_name} with prefix {prefix}"
        )

    if len(os.listdir(dest)) >= 5:
        oldest_dir = min(
            os.listdir(dest),
            key=lambda f: os.path.getctime(os.path.join(dest, f)),
        )
        shutil.rmtree(os.path.join(dest, oldest_dir))

    os.makedirs(state_dest, exist_ok=True)

    download_jobs = [
        (
            obj.key,
            os.path.join(state_dest, os.path.basename(obj.key)),
        )
        for obj in objects
    ]

    _download_files_parallel(download_jobs)


def download_zarr_file(state_name, gage_id, destination_path):
    dest = f"{destination_path}/zarr_files"
    print("This is the download util file destination: ", dest)
    os.makedirs(dest, exist_ok=True)

    done_path = os.path.join(dest, f".{gage_id}.done")
    running_path = os.path.join(dest, f".{gage_id}.running")
    try:
        first_folder = gage_id[:2]
        second_folder = gage_id[:4]
        bucket = get_bucket()

        zarr_prefix = f"rain_zarr/{state_name}/{first_folder}/{second_folder}/{gage_id}.zarr"

        local_zarr_path = os.path.join(dest, f"{gage_id}.zarr")

        if os.path.exists(local_zarr_path):
            Path(done_path).touch()
            return
        
        existing_files = [f for f in os.listdir(dest) if f.endswith(".zarr")]

        if len(existing_files) >= 5:
            oldest_file = min(
                existing_files,
                key=lambda f: os.path.getctime(os.path.join(dest, f)),
            )
            # Get rid of the .zarr file and any .done, .running files left behind.
            stem = Path(oldest_file).stem
            for f in os.listdir(dest):
                if Path(f).stem == stem:
                    if os.path.isdir(os.path.join(dest, f)):
                        shutil.rmtree(os.path.join(dest, f))
                    else:
                        os.remove(os.path.join(dest, f))
            


        objects = list(bucket.objects.filter(Prefix=zarr_prefix))

        if len(objects) == 0:
            raise FileNotFoundError(
                f"No Zarr files found in S3 for {gage_id} with prefix {zarr_prefix}"
            )

        download_jobs = []

        for obj in objects:
            relative_path = os.path.relpath(obj.key, os.path.dirname(zarr_prefix))
            local_file_path = os.path.join(dest, relative_path)
            download_jobs.append((obj.key, local_file_path))

        _download_files_parallel(download_jobs)
        print("Reached after downloading Zarr files...")
        Path(done_path).touch()
    
    except Exception as e:
        print(f"Failed to download Zarr file for {gage_id}: {e}", flush=True)
        
    finally:
        if os.path.exists(running_path):
            os.remove(running_path)
    
def download_s3_prefix_jsons(
    *,
    s3_prefix: str,
    local_dir: Path,
    workers: int = 4,
) -> list[Path]:
    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)

    bucket = get_bucket()
    objects = [obj for obj in bucket.objects.filter(Prefix=s3_prefix) if obj.key.endswith(".json")]

    if not objects:
        raise FileNotFoundError(f"No JSON files found in s3://{bucket.name}/{s3_prefix}")

    tasks = []
    downloaded: list[Path] = []

    for obj in objects:
        local_fp = local_dir / Path(obj.key).name

        if local_fp.exists() and local_fp.stat().st_size > 0:
            downloaded.append(local_fp)
        else:
            tasks.append((obj.key, local_fp))

    print(
        f"[BASIN JSON] prefix=s3://{bucket.name}/{s3_prefix} "
        f"total={len(objects)} existing={len(downloaded)} to_download={len(tasks)} workers={workers}",
        flush=True,
    )

    if not tasks:
        return downloaded

    workers = max(1, min(int(workers), len(tasks)))

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_download_one_json, obj_key, local_fp): (obj_key, local_fp)
            for obj_key, local_fp in tasks
        }

        for n, future in enumerate(as_completed(futures), start=1):
            obj_key, local_fp = futures[future]
            try:
                fp = future.result()
                downloaded.append(fp)

                if n % 25 == 0 or n == len(tasks):
                    print(f"[BASIN JSON] downloaded {n}/{len(tasks)}", flush=True)

            except Exception as e:
                raise RuntimeError(f"Failed downloading s3://{bucket.name}/{obj_key} -> {local_fp}: {e}") from e

    return downloaded

def download_flood_alert_inputs(
    *,
    base_dir: Path,
    state: str,
    workers: int = 4,
) -> dict[str, Path]:
    base_dir = Path(base_dir)
    state = state.upper()

    state_mask_fp = download_s3_file_if_missing(
        s3_key=f"state_masks/{state}_mrms_mask.npz",
        local_fp=base_dir / "state_masks" / f"{state}_mrms_mask.npz",
    )

    state_basin_index_fp = download_s3_file_if_missing(
        s3_key=f"state_basin_index/{state}_state_basin_index.npz",
        local_fp=base_dir / "state_basin_index" / f"{state}_state_basin_index.npz",
    )

    hydro_history_s3_prefix = "experiments/hydro_history_3mm_all_stage"

    pixel_event_index_fp = download_s3_file_if_missing(
        s3_key=f"{hydro_history_s3_prefix}/state_pixel_event_index/{state}_pixel_event_index.npz",
        local_fp=base_dir / "hydro_history" / "state_pixel_event_index" / f"{state}_pixel_event_index.npz",
    )

    efficient_event_reference_fp = download_s3_file_if_missing(
    s3_key=f"{hydro_history_s3_prefix}/state_efficient_event_reference/{state}_efficient_event_reference.npz",
    local_fp=base_dir / "hydro_history" / "state_efficient_event_reference" / f"{state}_efficient_event_reference.npz",
    )

    basin_json_base_dir = base_dir / "basins_json"
    basin_state_dir = basin_json_base_dir / state

    if not basin_json_base_dir.exists():
        os.makedirs(basin_json_base_dir, exist_ok=True)

    if len(os.listdir(basin_json_base_dir)) >= 5:
        oldest_dir = min(
            os.listdir(basin_json_base_dir),
            key=lambda f: os.path.getctime(os.path.join(basin_json_base_dir, f)),
        )
        shutil.rmtree(os.path.join(basin_json_base_dir, oldest_dir))

    download_s3_prefix_jsons(
        s3_prefix=f"basins_json/{state}/",
        local_dir=basin_state_dir,
        workers=workers,
    )

    return {
        "state_mask_fp": state_mask_fp,
        "state_basin_index_fp": state_basin_index_fp,
        "pixel_event_index_fp": pixel_event_index_fp,
        "efficient_event_reference_fp": efficient_event_reference_fp,
        "basin_json_dir": basin_state_dir,
    }