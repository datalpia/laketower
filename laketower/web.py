import urllib.parse
from pathlib import Path
from typing import Annotated, Optional

import pydantic_settings
from fastapi import APIRouter, FastAPI, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from laketower.config import Config, load_yaml_config
from laketower.tables import (
    DEFAULT_LIMIT,
    execute_query,
    generate_table_query,
    load_table,
)


class Settings(pydantic_settings.BaseSettings):
    laketower_config_path: Path


def current_path_with_args(request: Request, args: list[tuple[str, str]]) -> str:
    keys_to_update = set(arg[0] for arg in args)
    query_params = request.query_params.multi_items()
    new_query_params = list(
        filter(lambda param: param[0] not in keys_to_update, query_params)
    )
    new_query_params.extend((k, v) for k, v in args if v is not None)
    query_string = urllib.parse.urlencode(new_query_params)
    return f"{request.url.path}?{query_string}"


templates = Jinja2Templates(directory=Path(__file__).parent / "templates")
templates.env.filters["current_path_with_args"] = current_path_with_args

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    config: Config = request.app.state.config
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"tables": config.tables},
    )


@router.get("/tables/query", response_class=HTMLResponse)
def get_tables_query(request: Request, sql: str) -> HTMLResponse:
    config: Config = request.app.state.config
    tables_dataset = {
        table_config.name: load_table(table_config).dataset()
        for table_config in config.tables
    }
    results = execute_query(tables_dataset, sql)

    return templates.TemplateResponse(
        request=request,
        name="tables/query.html",
        context={
            "tables": config.tables,
            "table_results": results,
            "sql_query": sql,
        },
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
def get_table_view(
    request: Request,
    table_id: str,
    limit: Optional[int] = None,
    cols: Annotated[Optional[list[str]], Query()] = None,
    sort_asc: Optional[str] = None,
    sort_desc: Optional[str] = None,
    version: Optional[int] = None,
) -> HTMLResponse:
    config: Config = request.app.state.config
    table_config = next(
        filter(lambda table_config: table_config.name == table_id, config.tables)
    )
    table = load_table(table_config)
    table_name = table_config.name
    table_metadata = table.metadata()
    table_dataset = table.dataset(version=version)
    sql_query = generate_table_query(
        table_name, limit=limit, cols=cols, sort_asc=sort_asc, sort_desc=sort_desc
    )
    results = execute_query({table_name: table_dataset}, sql_query)

    return templates.TemplateResponse(
        request=request,
        name="tables/view.html",
        context={
            "tables": config.tables,
            "table_id": table_id,
            "table_metadata": table_metadata,
            "table_results": results,
            "sql_query": sql_query,
            "default_limit": DEFAULT_LIMIT,
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
