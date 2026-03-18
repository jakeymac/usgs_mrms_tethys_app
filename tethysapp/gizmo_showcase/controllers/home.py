from django.shortcuts import render
from tethys_sdk.routing import controller


@controller(name="home", url="gizmo-showcase")
def home(request):
    return render(request, "gizmo_showcase/home.html", {})
