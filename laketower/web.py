import io
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import bleach
import markdown
import pyarrow.csv as pacsv
import pydantic_settings
from fastapi import APIRouter, FastAPI, File, Form, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from laketower import __about__
from laketower.config import Config, load_yaml_config
from laketower.tables import (
    DEFAULT_LIMIT,
    ImportFileFormatEnum,
    ImportModeEnum,
    execute_query,
    extract_query_parameter_names,
    generate_table_statistics_query,
    generate_table_query,
    import_file_to_table,
    limit_query,
    load_datasets,
    load_table,
)


class Settings(pydantic_settings.BaseSettings):
    laketower_config_path: Path


@dataclass(frozen=True)
class AppMetadata:
    app_name: str
    app_version: str


def current_path_with_args(request: Request, args: list[tuple[str, str]]) -> str:
    keys_to_update = set(arg[0] for arg in args)
    query_params = request.query_params.multi_items()
    new_query_params = list(
        filter(lambda param: param[0] not in keys_to_update, query_params)
    )
    new_query_params.extend((k, v) for k, v in args if v is not None)
    query_string = urllib.parse.urlencode(new_query_params)
    return f"{request.url.path}?{query_string}"


def render_markdown(md_text: str) -> str:
    return bleach.clean(
        markdown.markdown(md_text), tags=bleach.sanitizer.ALLOWED_TAGS | {"p"}
    )


templates = Jinja2Templates(directory=Path(__file__).parent / "templates")
templates.env.filters["current_path_with_args"] = current_path_with_args
templates.env.filters["render_markdown"] = render_markdown

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    app_metadata: AppMetadata = request.app.state.app_metadata
    config: Config = request.app.state.config
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "app_metadata": app_metadata,
            "tables": config.tables,
            "queries": config.queries,
        },
    )


@router.get("/tables/query", response_class=HTMLResponse)
def get_tables_query(request: Request, sql: str) -> HTMLResponse:
    app_metadata: AppMetadata = request.app.state.app_metadata
    config: Config = request.app.state.config
    tables_dataset = load_datasets(config.tables)
    sql_schema = {
        table_name: dataset.schema.names
        for table_name, dataset in tables_dataset.items()
    }

    try:
        sql_param_names = extract_query_parameter_names(sql)
        sql_params = {
            name: request.query_params.get(name) or "" for name in sql_param_names
        }
    except ValueError:
        sql_params = {}

    try:
        sql_query = limit_query(sql, config.settings.max_query_rows + 1)

        start_time = time.perf_counter()
        results = execute_query(tables_dataset, sql_query, sql_params=sql_params)
        execution_time_ms = (time.perf_counter() - start_time) * 1000

        truncated = results.num_rows > config.settings.max_query_rows
        results = results.slice(
            0, min(results.num_rows, config.settings.max_query_rows)
        )
        error = None
    except ValueError as e:
        error = {"message": str(e)}
        results = None
        truncated = False
        execution_time_ms = None

    return templates.TemplateResponse(
        request=request,
        name="tables/query.html",
        context={
            "app_metadata": app_metadata,
            "tables": config.tables,
            "queries": config.queries,
            "table_results": results,
            "truncated_results": truncated,
            "execution_time_ms": execution_time_ms,
            "sql_query": sql,
            "sql_schema": sql_schema,
            "sql_params": sql_params,
            "error": error,
        },
    )


@router.get("/tables/query/csv")
def export_tables_query_csv(request: Request, sql: str) -> Response:
    config: Config = request.app.state.config
    tables_dataset = load_datasets(config.tables)

    results = execute_query(tables_dataset, sql)
    csv_content = io.BytesIO()
    pacsv.write_csv(
        results, csv_content, pacsv.WriteOptions(include_header=True, delimiter=",")
    )

    return Response(
        content=csv_content.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=query_results.csv"},
    )


@router.get("/tables/{table_id}", response_class=HTMLResponse)
def get_table_index(request: Request, table_id: str) -> HTMLResponse:
    app_metadata: AppMetadata = request.app.state.app_metadata
    config: Config = request.app.state.config
    table_config = next(
        filter(lambda table_config: table_config.name == table_id, config.tables)
    )
    try:
        table = load_table(table_config)
        table_metadata = table.metadata()
        table_schema = table.schema()
        error = None
    except ValueError as e:
        error = {"message": str(e)}
        table_metadata = None
        table_schema = None

    return templates.TemplateResponse(
        request=request,
        name="tables/index.html",
        context={
            "app_metadata": app_metadata,
            "tables": config.tables,
            "queries": config.queries,
            "table_id": table_id,
            "table_metadata": table_metadata,
            "table_schema": table_schema,
            "error": error,
        },
    )


@router.get("/tables/{table_id}/history", response_class=HTMLResponse)
def get_table_history(request: Request, table_id: str) -> HTMLResponse:
    app_metadata: AppMetadata = request.app.state.app_metadata
    config: Config = request.app.state.config
    table_config = next(
        filter(lambda table_config: table_config.name == table_id, config.tables)
    )
    try:
        table = load_table(table_config)
        table_history = table.history()
        error = None
    except ValueError as e:
        error = {"message": str(e)}
        table_history = None

    return templates.TemplateResponse(
        request=request,
        name="tables/history.html",
        context={
            "app_metadata": app_metadata,
            "tables": config.tables,
            "queries": config.queries,
            "table_id": table_id,
            "table_history": table_history,
            "error": error,
        },
    )


@router.get("/tables/{table_id}/statistics", response_class=HTMLResponse)
def get_table_statistics(
    request: Request,
    table_id: str,
    version: int | None = None,
) -> HTMLResponse:
    app_metadata: AppMetadata = request.app.state.app_metadata
    config: Config = request.app.state.config
    table_config = next(
        filter(lambda table_config: table_config.name == table_id, config.tables)
    )
    try:
        table = load_table(table_config)
        table_name = table_config.name
        table_metadata = table.metadata()
        table_dataset = table.dataset(version=version)
        sql_query = generate_table_statistics_query(table_name)
        query_results = execute_query({table_name: table_dataset}, sql_query)
        error = None
    except ValueError as e:
        error = {"message": str(e)}
        table_metadata = None
        query_results = None

    return templates.TemplateResponse(
        request=request,
        name="tables/statistics.html",
        context={
            "app_metadata": app_metadata,
            "tables": config.tables,
            "queries": config.queries,
            "table_id": table_id,
            "table_metadata": table_metadata,
            "table_results": query_results,
            "error": error,
        },
    )


@router.get("/tables/{table_id}/view", response_class=HTMLResponse)
def get_table_view(
    request: Request,
    table_id: str,
    limit: int | None = None,
    cols: Annotated[list[str] | None, Query()] = None,
    sort_asc: str | None = None,
    sort_desc: str | None = None,
    version: int | None = None,
) -> HTMLResponse:
    app_metadata: AppMetadata = request.app.state.app_metadata
    config: Config = request.app.state.config
    table_config = next(
        filter(lambda table_config: table_config.name == table_id, config.tables)
    )
    try:
        table = load_table(table_config)
        table_name = table_config.name
        table_metadata = table.metadata()
        table_dataset = table.dataset(version=version)
        sql_query = generate_table_query(
            table_name, limit=limit, cols=cols, sort_asc=sort_asc, sort_desc=sort_desc
        )
        results = execute_query({table_name: table_dataset}, sql_query)
        error = None
    except ValueError as e:
        error = {"message": str(e)}
        table_metadata = None
        sql_query = None
        results = None

    return templates.TemplateResponse(
        request=request,
        name="tables/view.html",
        context={
            "app_metadata": app_metadata,
            "tables": config.tables,
            "queries": config.queries,
            "table_id": table_id,
            "table_metadata": table_metadata,
            "table_results": results,
            "sql_query": sql_query,
            "default_limit": DEFAULT_LIMIT,
            "error": error,
        },
    )


@router.get("/tables/{table_id}/import", response_class=HTMLResponse)
def get_table_import(
    request: Request,
    table_id: str,
) -> HTMLResponse:
    app_metadata: AppMetadata = request.app.state.app_metadata
    config: Config = request.app.state.config
    table_config = next(
        filter(lambda table_config: table_config.name == table_id, config.tables)
    )
    try:
        table = load_table(table_config)
        table_metadata = table.metadata()
        message = None
    except ValueError as e:
        message = {"type": "error", "body": str(e)}
        table_metadata = None

    return templates.TemplateResponse(
        request=request,
        name="tables/import.html",
        context={
            "app_metadata": app_metadata,
            "tables": config.tables,
            "queries": config.queries,
            "table_id": table_id,
            "table_metadata": table_metadata,
            "message": message,
        },
    )


@router.post("/tables/{table_id}/import", response_class=HTMLResponse)
def post_table_import(
    request: Request,
    table_id: str,
    input_file: Annotated[UploadFile, File()],
    mode: Annotated[ImportModeEnum, Form()],
    file_format: Annotated[ImportFileFormatEnum, Form()],
    delimiter: Annotated[str, Form()],
    encoding: Annotated[str, Form()],
) -> HTMLResponse:
    app_metadata: AppMetadata = request.app.state.app_metadata
    config: Config = request.app.state.config
    table_config = next(
        filter(lambda table_config: table_config.name == table_id, config.tables)
    )
    try:
        table = load_table(table_config)
        table_metadata = table.metadata()
        rows_imported = import_file_to_table(
            table_config, input_file.file, mode, file_format, delimiter, encoding
        )
        message = {
            "type": "success",
            "body": f"Successfully imported {rows_imported} rows",
        }
    except Exception as e:
        message = {"type": "error", "body": str(e)}
        table_metadata = None

    return templates.TemplateResponse(
        request=request,
        name="tables/import.html",
        context={
            "app_metadata": app_metadata,
            "tables": config.tables,
            "queries": config.queries,
            "table_id": table_id,
            "table_metadata": table_metadata,
            "message": message,
        },
    )


@router.get("/queries/{query_id}/view", response_class=HTMLResponse)
def get_query_view(request: Request, query_id: str) -> Response:
    app_metadata: AppMetadata = request.app.state.app_metadata
    config: Config = request.app.state.config
    query_config = next(
        filter(lambda query_config: query_config.name == query_id, config.queries)
    )

    if (
        len(request.query_params.keys()) == 0
        and len(query_config.parameters.keys()) > 0
    ):
        default_parameters = {k: v.default for k, v in query_config.parameters.items()}
        url = request.url_for("get_query_view", query_id=query_id)
        query_params = urllib.parse.urlencode(default_parameters)
        return RedirectResponse(f"{url}?{query_params}")

    tables_dataset = load_datasets(config.tables)
    sql_param_names = extract_query_parameter_names(query_config.sql)
    sql_params = {
        name: request.query_params.get(name)
        or (
            query_param.default
            if (query_param := query_config.parameters.get(name))
            else None
        )
        or ""
        for name in sql_param_names
    }

    try:
        sql_query = limit_query(query_config.sql, config.settings.max_query_rows + 1)

        start_time = time.perf_counter()
        results = execute_query(tables_dataset, sql_query, sql_params=sql_params)
        execution_time_ms = (time.perf_counter() - start_time) * 1000

        truncated = results.num_rows > config.settings.max_query_rows
        results = results.slice(
            0, min(results.num_rows, config.settings.max_query_rows)
        )
        error = None
    except ValueError as e:
        error = {"message": str(e)}
        results = None
        truncated = False
        execution_time_ms = None

    return templates.TemplateResponse(
        request=request,
        name="queries/view.html",
        context={
            "app_metadata": app_metadata,
            "tables": config.tables,
            "queries": config.queries,
            "query": query_config,
            "query_results": results,
            "truncated_results": truncated,
            "execution_time_ms": execution_time_ms,
            "sql_params": sql_params,
            "error": error,
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
    app.state.app_metadata = AppMetadata(
        app_name="🗼 Laketower", app_version=__about__.__version__
    )
    app.state.config = config

    return app
