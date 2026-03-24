import os
import json

from tethys_sdk.routing import controller
from tethys_sdk.gizmos import MapView, MVLayer
from ..app import App
from ..s3_utils import download_basin_geojson

@controller(name="home")
def home(request):
    return App.render(request, "home.html")

def create_basin_layer(state):
    download_basin_geojson(state.upper())

    features = []
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    folder_path = os.path.join(BASE_DIR, "..", "json_data", state.upper())
    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            filepath = os.path.join(folder_path, filename)

            with open(filepath, "r") as f:
                features.append(
                    {"type": "Feature", "geometry": json.load(f)["geometry"]}
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

    basin_layer = MVLayer(
        source="GeoJSON",
        options=geojson_object,
        legend_title=f"{state} Basins",
    )

    return basin_layer

@controller(name="state_basin", url="basin/{state}")
def state_basin(request, state):
    state = state.capitalize()
    basin_layer = create_basin_layer(state)
    

    map_view = MapView(
        height="500px",
        width="100%",
        controls=["ZoomControl", "ScaleControl"],
        basemap="OpenStreetMap",
        layers=[basin_layer],
    )

    context = {
        "state": state,
        "map_view": map_view,
    }

    return App.render(request, "state_basin.html", context)