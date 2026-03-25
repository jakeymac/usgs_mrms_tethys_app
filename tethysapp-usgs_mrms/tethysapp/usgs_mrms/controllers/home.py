import os
import json

from tethys_sdk.routing import controller
from tethys_sdk.layouts import MapLayout
from tethys_sdk.gizmos import MapView, MVLayer
from ..app import App
from ..s3_utils import download_basin_geojson

@controller(name="home")
def home(request):
    return App.render(request, "home.html")

def create_basin_json(state):
    download_basin_geojson(state.upper())

    features = []
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    folder_path = os.path.join(BASE_DIR, "..", "json_data", state.upper())
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
    
@controller(name="zarr_viewer", url="basin/{state}/{gage_id}")
def view_zarr_page(request, state, gage_id):
    return App.render(request, "zarr_viewer.html", {"state": state, "gage_id": gage_id})


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