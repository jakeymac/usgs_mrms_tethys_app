import boto3
import os
from dotenv import load_dotenv

load_dotenv(".env")

def get_bucket():
    key = os.getenv("KEY")
    secret = os.getenv("SECRET")
    s3 = boto3.resource("s3", aws_access_key_id=key, aws_secret_access_key=secret, region_name="us-east-1")
    bucket = s3.Bucket("tgf-mentorship-gonzalo")
    return bucket

def download_basin_geojson(state_name):
    bucket = get_bucket()
    if not os.path.exists(f'tethysapp/usgs_mrms/json_data/{state_name}'):
        if len(os.listdir(f'tethysapp/usgs_mrms/json_data/')) == 5:
            oldest_file = min(os.listdir(f'tethysapp/usgs_mrms/json_data/'), key=lambda f: os.path.getctime(os.path.join(f'tethysapp/usgs_mrms/json_data/', f)))
            os.remove(os.path.join(f'tethysapp/usgs_mrms/json_data/', oldest_file))
        os.makedirs(f'tethysapp/usgs_mrms/json_data/{state_name}', exist_ok=True)
        for obj in bucket.objects.filter(Prefix=f'basins_json/{state_name}'):
            if obj.key.endswith('.json'):
                local_path = os.path.join(f'tethysapp/usgs_mrms/json_data/{state_name}', os.path.basename(obj.key))
                bucket.download_file(obj.key, local_path)
