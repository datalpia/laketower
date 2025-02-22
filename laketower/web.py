from pathlib import Path

import pydantic_settings
from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from laketower.config import Config, load_yaml_config
from laketower.tables import execute_query, generate_table_query, load_table


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


@router.get("/tables/{table_id}", response_class=HTMLResponse)
def get_table_index(request: Request, table_id: str) -> HTMLResponse:
    config: Config = request.app.state.config
    table_config = next(
        filter(lambda table_config: table_config.name == table_id, config.tables)
    )
    table = load_table(table_config)

    return templates.TemplateResponse(
        request=request,
        name="tables/index.html",
        context={
            "tables": config.tables,
            "table_id": table_id,
            "table_metadata": table.metadata(),
            "table_schema": table.schema(),
        },
    )


@router.get("/tables/{table_id}/history", response_class=HTMLResponse)
def get_table_history(request: Request, table_id: str) -> HTMLResponse:
    config: Config = request.app.state.config
    table_config = next(
        filter(lambda table_config: table_config.name == table_id, config.tables)
    )
    table = load_table(table_config)

    return templates.TemplateResponse(
        request=request,
        name="tables/history.html",
        context={
            "tables": config.tables,
            "table_id": table_id,
            "table_history": table.history(),
        },
    )


@router.get("/tables/{table_id}/view", response_class=HTMLResponse)
def get_table_view(request: Request, table_id: str) -> HTMLResponse:
    config: Config = request.app.state.config
    table_config = next(
        filter(lambda table_config: table_config.name == table_id, config.tables)
    )
    table = load_table(table_config)
    table_name = table_config.name
    table_dataset = table.dataset()
    sql_query = generate_table_query(table_name)
    results = execute_query({table_name: table_dataset}, sql_query)

    return templates.TemplateResponse(
        request=request,
        name="tables/view.html",
        context={
            "tables": config.tables,
            "table_id": table_id,
            "table_results": results,
        },
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
