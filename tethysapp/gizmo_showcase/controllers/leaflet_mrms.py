from __future__ import annotations

import json

from django.shortcuts import render
from tethys_sdk.routing import controller

from ..mrms_tiles import get_mrms_meta


@controller(
    name="leaflet_mrms",
    url="gizmo-showcase/leaflet-mrms",
    login_required=False,
)
def leaflet_mrms(request):
    meta = get_mrms_meta()

    valid_time_indices = meta["valid_time_indices"]
    valid_times_iso = meta["valid_times_iso"]
    valid_count = len(valid_time_indices)

    slider_t0 = valid_count // 2 if valid_count else 0
    slider_max = max(valid_count - 1, 0)

    context = {
        "tile_url_template": "/apps/gizmo-showcase/mrms/tiles/{t}/{z}/{x}/{y}",
        "value_url": "/apps/gizmo-showcase/mrms/value_at",
        "max_pixel_url": "/apps/gizmo-showcase/mrms/max_pixel",
        "recurrence_tile_url_template": "/apps/gizmo-showcase/mrms/recurrence/tiles/{z}/{x}/{y}",
        "recurrence_value_url": "/apps/gizmo-showcase/mrms/recurrence/value_at",
        "slider_t0": slider_t0,
        "slider_max": slider_max,
        "valid_time_indices_json": json.dumps(valid_time_indices),
        "valid_times_iso_json": json.dumps(valid_times_iso),
        "west": meta["west"],
        "south": meta["south"],
        "east": meta["east"],
        "north": meta["north"],
        "recurrence_max_count": meta["recurrence_max_count"],
        "n_valid_times": meta["n_valid_times"],
    }

    return render(request, "gizmo_showcase/leaflet_mrms.html", context)


