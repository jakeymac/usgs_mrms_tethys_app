from tethys_sdk.base import TethysAppBase


class App(TethysAppBase):
    """
    Tethys app class for USGS MRMS Events.
    """
    name = 'USGS MRMS Events'
    description = ''
    package = 'usgs_mrms'  # WARNING: Do not change this value
    index = 'home'
    icon = f'{package}/images/icon.gif'
    root_url = 'usgs-mrms'
    color = '#c23616'
    tags = ''
    enable_feedback = False
    feedback_emails = []
