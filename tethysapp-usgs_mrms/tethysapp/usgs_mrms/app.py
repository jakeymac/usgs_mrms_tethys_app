from tethys_sdk.base import TethysAppBase
from tethys_sdk.app_settings import CustomSetting, SecretCustomSetting


class App(TethysAppBase):
    """
    Tethys app class for USGS-MRMS Flood Explorer.
    """
    name = 'USGS-MRMS Flood Explorer'
    description = ''
    package = 'usgs_mrms'  # WARNING: Do not change this value
    index = 'home'
    icon = f'{package}/images/icon.gif'
    root_url = 'usgs-mrms'
    color = '#c23616'
    tags = ''
    enable_feedback = False
    feedback_emails = []

    def custom_settings(self):
        bucket_name = CustomSetting(
            name="bucket_name",
            description="Name of the S3 bucket where MRMS data is stored.",
            required=True,
        )
        s3_region = CustomSetting(
            name="s3_region",
            description="AWS region where the S3 bucket is located (e.g., 'us-east-1').",
            required=True,
        )
        s3_key = SecretCustomSetting(
            name='s3_key',
            description='AWS Access Key ID for accessing MRMS data on S3.',
            required=True,
        )
        s3_secret = SecretCustomSetting(
            name='s3_secret',
            description='AWS Secret Access Key for accessing MRMS data on S3.',
            required=True,
        )
        return (bucket_name, s3_region, s3_key, s3_secret)