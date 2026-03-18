from tethys_sdk.base import TethysAppBase


class MrmsViewer(TethysAppBase):
    """
    Tethys app for exploring MRMS rainfall through time on an interactive Leaflet map.
    """

    name = "USGS-MRMS-EVENTS"
    index = "home"
    icon = "gizmo_showcase/images/gizmos.png"
    package = "gizmo_showcase"
    root_url = "gizmo-showcase"
    color = "#0F4C81"
    description = (
        "Interactive Leaflet viewer for MRMS rainfall time steps with automatic "
        "maximum-pixel tracking."
    )
    tags = "MRMS, rainfall, hydrology, Leaflet, radar"
    enable_feedback = False
    feedback_emails = []