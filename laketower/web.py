from pathlib import Path

import pydantic_settings
from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from laketower.config import Config, load_yaml_config


class Settings(pydantic_settings.BaseSettings):
    laketower_config_path: Path


templates = Jinja2Templates(directory=Path(__file__).parent / "templates")


router = APIRouter()


@router.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    config: Config = request.app.state.config
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"tables": config.tables},
    )


def create_app() -> FastAPI:
    settings = Settings()  # type: ignore[call-arg]
    config = load_yaml_config(settings.laketower_config_path)

    app = FastAPI(title="laketower")
    app.mount(
        "/static",
        StaticFiles(directory=Path(__file__).parent / "static"),
        name="static",
    )
    app.include_router(router)
    app.state.config = config

    return app
