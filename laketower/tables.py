import enum
from datetime import datetime, timezone
from typing import Any, BinaryIO, Protocol, TextIO

import deltalake
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as padataset
import pydantic
import sqlglot
import sqlglot.dialects.duckdb
import sqlglot.expressions

from laketower.config import ConfigTable, TableFormats


DEFAULT_LIMIT = 10


class ImportModeEnum(str, enum.Enum):
    append = "append"
    overwrite = "overwrite"


class ImportFileFormatEnum(str, enum.Enum):
    csv = "csv"


class TableMetadata(pydantic.BaseModel):
    table_format: TableFormats
    name: str | None = None
    description: str | None = None
    uri: str
    id: str
    version: int
    created_at: datetime
    partitions: list[str]
    configuration: dict[str, str]


class TableRevision(pydantic.BaseModel):
    version: int
    timestamp: datetime
    client_version: str | None = None
    operation: str
    operation_parameters: dict[str, Any]
    operation_metrics: dict[str, Any]


class TableHistory(pydantic.BaseModel):
    revisions: list[TableRevision]


class TableProtocol(Protocol):  # pragma: no cover
    @classmethod
    def is_valid(cls, table_config: ConfigTable) -> bool: ...
    def __init__(self, table_config: ConfigTable) -> None: ...
    def metadata(self) -> TableMetadata: ...
    def schema(self) -> pa.Schema: ...
    def history(self) -> TableHistory: ...
    def dataset(self, version: int | str | None = None) -> padataset.Dataset: ...
    def import_data(
        self, data: pd.DataFrame, mode: ImportModeEnum = ImportModeEnum.append
    ) -> None: ...


class DeltaTable:
    def __init__(self, table_config: ConfigTable):
        super().__init__()
        self.table_config = table_config
        self._impl = deltalake.DeltaTable(table_config.uri)

    @classmethod
    def is_valid(cls, table_config: ConfigTable) -> bool:
        return deltalake.DeltaTable.is_deltatable(table_config.uri)

    def metadata(self) -> TableMetadata:
        metadata = self._impl.metadata()
        return TableMetadata(
            table_format=self.table_config.table_format,
            name=metadata.name,
            description=metadata.description,
            uri=self._impl.table_uri,
            id=str(metadata.id),
            version=self._impl.version(),
            created_at=datetime.fromtimestamp(
                metadata.created_time / 1000, tz=timezone.utc
            ),
            partitions=metadata.partition_columns,
            configuration=metadata.configuration,
        )

    def schema(self) -> pa.Schema:
        return pa.schema(self._impl.schema().to_arrow())  # type: ignore[arg-type]

    def history(self) -> TableHistory:
        delta_history = self._impl.history()
        revisions = [
            TableRevision(
                version=event["version"],
                timestamp=datetime.fromtimestamp(
                    event["timestamp"] / 1000, tz=timezone.utc
                ),
                client_version=event.get("clientVersion") or event.get("engineInfo"),
                operation=event["operation"],
                operation_parameters=event["operationParameters"],
                operation_metrics=event.get("operationMetrics") or {},
            )
            for event in delta_history
        ]
        return TableHistory(revisions=revisions)

    def dataset(self, version: int | str | None = None) -> padataset.Dataset:
        if version is not None:
            self._impl.load_as_version(version)
        return self._impl.to_pyarrow_dataset()

    def import_data(
        self, data: pd.DataFrame, mode: ImportModeEnum = ImportModeEnum.append
    ) -> None:
        deltalake.write_deltalake(
            self.table_config.uri, data, mode=mode.value, schema_mode="merge"
        )


def load_table(table_config: ConfigTable) -> TableProtocol:
    format_handler: dict[TableFormats, type[TableProtocol]] = {
        TableFormats.delta: DeltaTable
    }
    table_handler = format_handler[table_config.table_format]
    if not table_handler.is_valid(table_config):
        raise ValueError(f"Invalid table: {table_config.uri}")
    return table_handler(table_config)


def load_datasets(table_configs: list[ConfigTable]) -> dict[str, padataset.Dataset]:
    tables_dataset = {}
    for table_config in table_configs:
        try:
            tables_dataset[table_config.name] = load_table(table_config).dataset()
        except ValueError:
            pass
    return tables_dataset


def generate_table_query(
    table_name: str,
    limit: int | None = None,
    cols: list[str] | None = None,
    sort_asc: str | None = None,
    sort_desc: str | None = None,
) -> str:
    query_expr = (
        sqlglot.select(*(cols or ["*"])).from_(table_name).limit(limit or DEFAULT_LIMIT)
    )
    if sort_asc:
        query_expr = query_expr.order_by(f"{sort_asc} asc")
    elif sort_desc:
        query_expr = query_expr.order_by(f"{sort_desc} desc")
    return sqlglot.Generator(dialect=sqlglot.dialects.duckdb.DuckDB).generate(
        query_expr
    )


def generate_table_statistics_query(table_name: str) -> str:
    return (
        f"SELECT column_name, count, avg, std, min, max FROM (SUMMARIZE {table_name})"  # nosec B608
    )


def execute_query(
    tables_datasets: dict[str, padataset.Dataset], sql_query: str
) -> pd.DataFrame:
    try:
        conn = duckdb.connect()
        for table_name, table_dataset in tables_datasets.items():
            # ATTACH IF NOT EXISTS ':memory:' AS {catalog.name};
            # CREATE SCHEMA IF NOT EXISTS {catalog.name}.{database.name};
            # USE {catalog.name}.{database.name};
            # CREATE VIEW IF NOT EXISTS {table.name} AS FROM {table.name}_dataset;

            view_name = f"{table_name}_view"
            conn.register(view_name, table_dataset)
            conn.execute(f"create table {table_name} as select * from {view_name}")  # nosec B608
        return conn.execute(sql_query).df()
    except duckdb.Error as e:
        raise ValueError(str(e)) from e


def import_file_to_table(
    table_config: ConfigTable,
    file_path: BinaryIO | TextIO,
    mode: ImportModeEnum = ImportModeEnum.append,
    file_format: ImportFileFormatEnum = ImportFileFormatEnum.csv,
    delimiter: str = ",",
    encoding: str = "utf-8",
) -> int:
    file_format_handler = {
        ImportFileFormatEnum.csv: lambda f, d, e: pd.read_csv(f, sep=d, encoding=e)
    }
    table = load_table(table_config)
    df = file_format_handler[file_format](file_path, delimiter, encoding)
    table.import_data(df, mode=mode)
    return len(df)
