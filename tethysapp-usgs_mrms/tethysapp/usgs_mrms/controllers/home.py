from tethys_sdk.routing import controller
from ..app import App

@controller(name="home", url="/")
def home(request):
    return App.render(request, "home.html")
