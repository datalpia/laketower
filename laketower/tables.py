import enum
from datetime import datetime, timezone
from typing import Any, BinaryIO, Protocol, TextIO

import deltalake
import duckdb
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.dataset as padataset
import pydantic
import sqlglot
import sqlglot.dialects.duckdb
import sqlglot.errors
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
        self, data: pa.Table, mode: ImportModeEnum = ImportModeEnum.append
    ) -> None: ...


class DeltaTable:
    def __init__(self, table_config: ConfigTable):
        super().__init__()
        self.table_config = table_config
        storage_options = self._generate_storage_options(table_config)
        self._impl = deltalake.DeltaTable(
            table_config.uri, storage_options=storage_options
        )

    @classmethod
    def _generate_storage_options(
        cls, table_config: ConfigTable
    ) -> dict[str, str] | None:
        # documentation from `object-store` Rust crate:
        # - s3: https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html
        # - adls: https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html
        storage_options = None
        conn_s3 = (
            table_config.connection.s3
            if table_config.connection and table_config.connection.s3
            else None
        )
        conn_adls = (
            table_config.connection.adls
            if table_config.connection and table_config.connection.adls
            else None
        )
        if conn_s3:
            storage_options = (
                {
                    "aws_access_key_id": conn_s3.s3_access_key_id,
                    "aws_secret_access_key": conn_s3.s3_secret_access_key.get_secret_value(),
                    "aws_allow_http": str(conn_s3.s3_allow_http).lower(),
                }
                | ({"aws_region": conn_s3.s3_region} if conn_s3.s3_region else {})
                | (
                    {"aws_endpoint_url": str(conn_s3.s3_endpoint_url).rstrip("/")}
                    if conn_s3.s3_endpoint_url
                    else {}
                )
            )
        elif conn_adls:
            storage_options = (
                {
                    "azure_storage_account_name": conn_adls.adls_account_name,
                    "azure_use_azure_cli": str(conn_adls.use_azure_cli).lower(),
                }
                | (
                    {
                        "azure_storage_access_key": conn_adls.adls_access_key.get_secret_value()
                    }
                    if conn_adls.adls_access_key
                    else {}
                )
                | (
                    {"azure_storage_sas_key": conn_adls.adls_sas_key.get_secret_value()}
                    if conn_adls.adls_sas_key
                    else {}
                )
                | (
                    {"azure_storage_tenant_id": conn_adls.adls_tenant_id}
                    if conn_adls.adls_tenant_id
                    else {}
                )
                | (
                    {"azure_storage_client_id": conn_adls.adls_client_id}
                    if conn_adls.adls_client_id
                    else {}
                )
                | (
                    {
                        "azure_storage_client_secret": conn_adls.adls_client_secret.get_secret_value()
                    }
                    if conn_adls.adls_client_secret
                    else {}
                )
                | (
                    {
                        "azure_msi_endpoint": str(conn_adls.azure_msi_endpoint).rstrip(
                            "/"
                        )
                    }
                    if conn_adls.azure_msi_endpoint
                    else {}
                )
            )
        return storage_options

    @classmethod
    def is_valid(cls, table_config: ConfigTable) -> bool:
        storage_options = cls._generate_storage_options(table_config)
        return deltalake.DeltaTable.is_deltatable(
            table_config.uri, storage_options=storage_options
        )

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
        self, data: pa.Table, mode: ImportModeEnum = ImportModeEnum.append
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


def extract_query_parameter_names(sql: str) -> set[str]:
    try:
        parsed_sql = sqlglot.parse(sql, dialect=sqlglot.dialects.duckdb.DuckDB)
    except sqlglot.errors.SqlglotError as e:
        raise ValueError(f"Error: {e}") from e

    return {
        str(node.this)
        for statement in parsed_sql
        if statement is not None
        for node in statement.walk()
        if isinstance(node, sqlglot.expressions.Placeholder)
    }


def generate_table_query(
    table_name: str,
    limit: int | None = None,
    cols: list[str] | None = None,
    sort_asc: str | None = None,
    sort_desc: str | None = None,
) -> str:
    query_expr = (
        sqlglot.select(*([f'"{col}"' for col in cols] if cols else ["*"]))
        .from_(f'"{table_name}"')
        .limit(limit or DEFAULT_LIMIT)
    )
    if sort_asc:
        query_expr = query_expr.order_by(f"{sort_asc} asc")
    elif sort_desc:
        query_expr = query_expr.order_by(f"{sort_desc} desc")
    return query_expr.sql(dialect=sqlglot.dialects.duckdb.DuckDB, identify="always")


def generate_table_statistics_query(table_name: str) -> str:
    summarize_expr = sqlglot.expressions.Summarize(
        this=sqlglot.expressions.Table(this=f'"{table_name}"')
    )
    subquery_expr = sqlglot.expressions.Subquery(this=summarize_expr)
    query_expr = sqlglot.select(
        "column_name", "count", "avg", "std", "min", "max"
    ).from_(subquery_expr)
    return query_expr.sql(dialect=sqlglot.dialects.duckdb.DuckDB, identify="always")


def limit_query(sql_query: str, max_limit: int) -> str:
    try:
        query_ast = sqlglot.parse(sql_query, dialect=sqlglot.dialects.duckdb.DuckDB)
    except sqlglot.errors.SqlglotError as e:
        raise ValueError(f"Error: {e}") from e

    if query_ast and isinstance(query_ast[-1], sqlglot.expressions.Select):
        limit_wrapper = (
            sqlglot.select("*")
            .from_(sqlglot.expressions.Subquery(this=query_ast[-1]))
            .limit(max_limit)
        )
        query_ast[-1] = limit_wrapper

    return "; ".join(
        [
            stmt.sql(dialect=sqlglot.dialects.duckdb.DuckDB, identify="always")
            for stmt in query_ast
            if stmt is not None
        ]
    )


def execute_query(
    tables_datasets: dict[str, padataset.Dataset],
    sql_query: str,
    sql_params: dict[str, str] = {},
) -> pa.Table:
    if not sql_query:
        raise ValueError("Error: Cannot execute empty SQL query")

    try:
        conn = duckdb.connect()
        for table_name, table_dataset in tables_datasets.items():
            # ATTACH IF NOT EXISTS ':memory:' AS {catalog.name};
            # CREATE SCHEMA IF NOT EXISTS {catalog.name}.{database.name};
            # USE {catalog.name}.{database.name};
            # CREATE VIEW IF NOT EXISTS {table.name} AS FROM {table.name}_dataset;

            view_name = f"{table_name}_view"
            conn.register(view_name, table_dataset)
            conn.execute(f'create view "{table_name}" as select * from "{view_name}"')  # nosec B608
        return conn.execute(sql_query, parameters=sql_params).fetch_arrow_table()
    except duckdb.Error as e:
        raise ValueError(f"Error: {e}") from e


def import_file_to_table(
    table_config: ConfigTable,
    file_path: BinaryIO | TextIO,
    mode: ImportModeEnum = ImportModeEnum.append,
    file_format: ImportFileFormatEnum = ImportFileFormatEnum.csv,
    delimiter: str = ",",
    encoding: str = "utf-8",
) -> int:
    file_format_handler = {
        ImportFileFormatEnum.csv: lambda f, d, e: csv.read_csv(
            f,
            read_options=csv.ReadOptions(encoding=e),
            parse_options=csv.ParseOptions(delimiter=d),
        )
    }
    table = load_table(table_config)
    df = file_format_handler[file_format](file_path, delimiter, encoding)
    table.import_data(df, mode=mode)
    return len(df)
