import os
import json
from pathlib import Path
import shutil

from django.http import JsonResponse
from django.shortcuts import redirect
from tethys_sdk.routing import controller
from tethys_sdk.layouts import MapLayout
from tethys_sdk.gizmos import MVView
from ..app import App
from ..s3_utils import download_basin_geojson_files, download_zarr_file
from ..mrms_tiles import get_mrms_meta

@controller(name="home")
def home(request):
    return App.render(request, "home.html")

@controller(name="download_basin", url="download_basin/{state}/")
def download_basin_page(request, state):
    breakpoint()
    state = state.title()
    return App.render(request, "downloading.html", {"state": state})

@controller(name="do_download_basin_endpoint", url="do_download_basin/{state}/", app_media=True)
def do_download_basin(request, state, app_media):
    breakpoint()
    state = state.upper()
    try:
        download_basin_geojson_files(state, app_media.path)
        features = []
        folder_path = os.path.join(app_media.path, "basin_json_downloaded_files", state.upper())
        for filename in os.listdir(folder_path):
            if filename.endswith(".json"):
                filepath = os.path.join(folder_path, filename)

                with open(filepath, "r") as f:
                    data = json.load(f)
                    features.append(
                        {"type": "Feature", "geometry": data["geometry"], "properties": data["properties"]}
                    )

        # Sort largest-first so smaller basins render on top and remain selectable
        features.sort(key=lambda f: calculate_basin_area(f.get("geometry")), reverse=True)

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

        generated_json_folder_path = os.path.join(App.get_app_media().path, "generated_basin_json")
        if not os.path.exists(generated_json_folder_path):
            os.makedirs(generated_json_folder_path)
        
        generated_json_file_path = os.path.join(App.get_app_media().path, "generated_basin_json", f"{state.upper()}.json")

        with open(generated_json_file_path, "w") as f:
            json.dump(geojson_object, f)

        # Delete the downloaded basin JSON files after generating the consolidated JSON file
        shutil.rmtree(os.path.join(App.get_app_media().path, "basin_json_downloaded_files", state.upper()))

        return JsonResponse({"status": "success"})
    
    except FileNotFoundError as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=404)

    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=500)

@controller(name="download_zarr", url="download_zarr/{state}/{gage_id}/")
def download_zarr(request, state, gage_id):
    state = state.title()
    return App.render(request, "downloading.html", {"state": state, "gage_id": gage_id})

@controller(name="do_download_zarr_endpoint", url="do_download_zarr/{state}/{gage_id}/", app_media=True)
def do_download_zarr(request, state, gage_id, app_media):
    state = state.upper()
    try:
        download_zarr_file(state, gage_id, app_media.path)
        return JsonResponse({"status": "success"})
    
    except FileNotFoundError as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=404)

    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=500)

def calculate_ring_area(ring):
    area = 0.0
    n = len(ring)
    for i in range(n):
        x1, y1 = ring[i][0], ring[i][1]
        x2, y2 = ring[(i + 1) % n][0], ring[(i + 1) % n][1]
        area += x1 * y2 - x2 * y1
    return abs(area) / 2.0


def calculate_basin_area(geometry):
    if not geometry:
        return 0.0
    gtype = geometry.get("type")
    coords = geometry.get("coordinates", [])
    if gtype == "Polygon" and coords:
        outer = calculate_ring_area(coords[0])
        holes = sum(calculate_ring_area(r) for r in coords[1:])
        return max(outer - holes, 0.0)
    if gtype == "MultiPolygon":
        total = 0.0
        for poly in coords:
            if not poly:
                continue
            outer = calculate_ring_area(poly[0])
            holes = sum(calculate_ring_area(r) for r in poly[1:])
            total += max(outer - holes, 0.0)
        return total
    return 0.0
    

def get_basin_json(state):
    generated_json_file_path = os.path.join(App.get_app_media().path, "generated_basin_json", f"{state.upper()}.json")

    if not os.path.isfile(generated_json_file_path):
        return redirect("usgs_mrms:download_basin", state=state)
    with open(generated_json_file_path, "r") as f:
        return json.load(f)


@controller(name="state_basin", url="basin/{state}/", app_media=True)
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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get(self, request, state, app_media, *args, **kwargs):
        self.basin_json = get_basin_json(state)
        
        self.state = state.upper()
        return super().get(request, state=state, app_media=app_media, *args, **kwargs)  

    def build_map_extent_and_view(self, request, *args, **kwargs):
        # Retreive state map extent from JSON file
        state_extents_file = Path(__file__).parent / "../state_map_extents/state_extents.json"
        state_extents_json = json.load(state_extents_file.open())
        self.map_extent = state_extents_json.get(self.state, [-180, -90, 180, 90])
        self.map_center = [(self.map_extent[1] + self.map_extent[3]) / 2, 
                           (self.map_extent[0] + self.map_extent[2]) / 2]

        map_view =MVView(
            extent=self.map_extent,
            zoom=6
        )
        return map_view, self.map_center

    def compose_layers(self, request, map_view, app_media, *args, **kwargs):
        state = kwargs.get("state").capitalize()

        basin_layer = self.build_geojson_layer(
            self.basin_json, 
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
    app_media=True
)
def leaflet_mrms(request, state, gage_id, app_media):
    app_media_path = app_media.path
    zarr_path = os.path.join(app_media_path, "zarr_files", f"{gage_id}.zarr")

    if not os.path.exists(zarr_path):
        return redirect("usgs_mrms:download_zarr", state=state, gage_id=gage_id)

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