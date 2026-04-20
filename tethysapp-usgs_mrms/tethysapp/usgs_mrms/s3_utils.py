import boto3
import os
import shutil
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "../../.env"))

def get_bucket():
    key = os.getenv("KEY")
    secret = os.getenv("SECRET")
    s3 = boto3.resource("s3", aws_access_key_id=key, aws_secret_access_key=secret, region_name="us-east-1")
    bucket = s3.Bucket("tgf-mentorship-gonzalo")
    return bucket

def download_basin_geojson_files(state_name, destination_path):
    bucket = get_bucket()
    prefix = f'basins_json/{state_name}'
    dest = f"{destination_path}/basin_json_downloaded_files"

    os.makedirs(dest, exist_ok=True)
    if not os.path.exists(f'{dest}/{state_name}'):
        objects = [obj for obj in bucket.objects.filter(Prefix=prefix) if obj.key.endswith('.json')]
        if not objects:
            raise FileNotFoundError(f"No basin JSON files found in S3 for state {state_name} with prefix {prefix}")
        if len(os.listdir(dest)) == 5:
            oldest_dir = min(os.listdir(dest), key=lambda f: os.path.getctime(os.path.join(dest, f)))
            shutil.rmtree(os.path.join(dest, oldest_dir))
        os.makedirs(f'{dest}/{state_name}', exist_ok=True)
        for obj in objects:
            local_path = os.path.join(f'{dest}/{state_name}', os.path.basename(obj.key))
            bucket.download_file(obj.key, local_path)

def download_zarr_file(state_name, gage_id, destination_path):
    first_folder = gage_id[:2]
    second_folder = gage_id[:4]
    bucket = get_bucket()
    zarr_prefix = f"rain_zarr/{state_name}/{first_folder}/{second_folder}/{gage_id}.zarr"
    
    dest = f"{destination_path}/zarr_files"
    local_zarr_path = os.path.join(dest, f"{gage_id}.zarr")

    os.makedirs(dest, exist_ok=True)
    if not os.path.exists(local_zarr_path):
        existing_files = os.listdir(dest)
        if len(existing_files) == 5:
            oldest_file = min(existing_files, key=lambda f: os.path.getctime(os.path.join(dest, f)))
            shutil.rmtree(os.path.join(dest, oldest_file))
        objects = list(bucket.objects.filter(Prefix=zarr_prefix))
        if len(objects) == 0:
            raise FileNotFoundError(f"No Zarr files found in S3 for {gage_id} with prefix {zarr_prefix}")

        for obj in objects:
            relative_path = os.path.relpath(obj.key, os.path.dirname(zarr_prefix))
            local_file_path = os.path.join(dest, relative_path)
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            bucket.download_file(obj.key, local_file_path)
