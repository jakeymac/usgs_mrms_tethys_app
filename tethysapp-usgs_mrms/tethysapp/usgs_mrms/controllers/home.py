import os
import json

from tethys_sdk.routing import controller
from tethys_sdk.layouts import MapLayout
from tethys_sdk.gizmos import MapView, MVLayer
from ..app import App
from ..s3_utils import download_basin_geojson, download_zarr_file
from ..mrms_tiles import get_mrms_meta

@controller(name="home")
def home(request):
    return App.render(request, "home.html")

def create_basin_json(state):
    app_media_path = App.get_app_media().path
    download_basin_geojson(state.upper(), app_media_path)

    features = []
    folder_path = os.path.join(app_media_path, "basin_json", state.upper())
    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            filepath = os.path.join(folder_path, filename)

            with open(filepath, "r") as f:
                data = json.load(f)
                features.append(
                    {"type": "Feature", "geometry": data["geometry"], "properties": data["properties"]}
                )

    geojson_object = {
        "type": "FeatureCollection",
        'crs': {
            'type': 'name',
            'properties': {
            'name': 'EPSG:4326'
            }
        },
        "features": features,
    }

    return geojson_object

@controller(name="state_basin", url="basin/{state}/")
class StateBasinMapLayout(MapLayout):
    app = App
    base_template = 'usgs_mrms/base.html'
    map_title = f'My Map Layout for state'
    map_subtitle = 'Subtitle'
    basemaps = [
        'OpenStreetMap',
        'ESRI'
    ]

    show_properties_popup = True

    def compose_layers(self, request, map_view, *args, **kwargs):
        state = kwargs.get("state").capitalize()
        basin_geojson = create_basin_json(state)
        basin_layer = self.build_geojson_layer(
            basin_geojson, 
            layer_name=f"basins",
            layer_title=f"{state} Basins",
            layer_variable='basins',
            visible=True,
            selectable=True,
            plottable=True,
        )

        map_view.layers.append(basin_layer)

        # Add layer to layer group
        layer_groups = [
            self.build_layer_group(
                id='basins-layer-group',
                display_name='Basins',
                layer_control='radio',  # 'radio' or 'check'
                layers=[
                    basin_layer,
                ],
            ),
        ]

        return layer_groups
    
@controller(
    name="zarr_viewer",
    url="basin/{state}/{gage_id}",
    login_required=False,
)
def leaflet_mrms(request, state, gage_id):
    download_zarr_file(state.upper(), gage_id, App.get_app_media().path)
    meta = get_mrms_meta(gage_id)

    valid_time_indices = meta["valid_time_indices"]
    valid_times_iso = meta["valid_times_iso"]
    valid_count = len(valid_time_indices)

    slider_t0 = valid_count // 2 if valid_count else 0
    slider_max = max(valid_count - 1, 0)

    context = {
        "tile_url_template": f"/apps/usgs-mrms/mrms/tiles/{gage_id}/{{t}}/{{z}}/{{x}}/{{y}}",
        "value_url": f"/apps/usgs-mrms/mrms/value_at/{gage_id}",
        "max_pixel_url": f"/apps/usgs-mrms/mrms/max_pixel/{gage_id}",
        "recurrence_tile_url_template": f"/apps/usgs-mrms/mrms/recurrence/tiles/{gage_id}/{{z}}/{{x}}/{{y}}",
        "recurrence_value_url": f"/apps/usgs-mrms/mrms/recurrence/value_at/{gage_id}",
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
        "gage_id": gage_id,
    }

    return App.render(request, "leaflet_mrms.html", context)

# def state_basin(request, state):
#     state = state.capitalize()
#     print("Before creating basin layer...")
#     basin_layer = create_basin_layer(state)
#     print("After creating basin layer...")

#     map_view = MapView(
#         height="500px",
#         width="100%",
#         controls=["ZoomControl", "ScaleControl"],
#         basemap="OpenStreetMap",
#         layers=[basin_layer],
#     )

#     print("Building context...")
#     context = {
#         "state": state,
#         "map_view": map_view,
#     }

#     return App.render(request, "state_basin.html", context)