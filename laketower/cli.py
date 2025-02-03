import argparse
import enum
from datetime import datetime, timezone
from pathlib import Path

import deltalake
import pydantic
import rich.text
import rich.tree
import yaml


class TableFormats(str, enum.Enum):
    delta = "delta"


class ConfigTable(pydantic.BaseModel):
    name: str
    uri: str
    table_format: TableFormats = pydantic.Field(alias="format")

    @pydantic.model_validator(mode="after")
    def check_table(self) -> "ConfigTable":
        def check_delta_table(table_uri: str) -> None:
            if not deltalake.DeltaTable.is_deltatable(table_uri):
                raise ValueError(f"{table_uri} is not a valid Delta table")

        format_check = {TableFormats.delta: check_delta_table}
        format_check[self.table_format](self.uri)

        return self


class ConfigQuery(pydantic.BaseModel):
    name: str
    sql: str


class ConfigDashboard(pydantic.BaseModel):
    name: str


class Config(pydantic.BaseModel):
    tables: list[ConfigTable] = []


def load_yaml_config(config_path: Path) -> Config:
    config_dict = yaml.safe_load(config_path.read_text())
    return Config.model_validate(config_dict)


class TableMetadata(pydantic.BaseModel):
    table_format: TableFormats
    name: str
    description: str
    uri: str
    id: str
    version: int
    created_at: datetime
    partitions: list[str]
    configuration: dict[str, str]


def load_table_metadata(table_config: ConfigTable) -> TableMetadata:
    def load_delta_table_metadata(table_config: ConfigTable) -> TableMetadata:
        delta_table = deltalake.DeltaTable(table_config.uri)
        metadata = delta_table.metadata()
        return TableMetadata(
            table_format=table_config.table_format,
            name=metadata.name,
            description=metadata.description,
            uri=delta_table.table_uri,
            id=str(metadata.id),
            version=delta_table.version(),
            created_at=datetime.fromtimestamp(
                metadata.created_time / 1000, tz=timezone.utc
            ),
            partitions=metadata.partition_columns,
            configuration=metadata.configuration,
        )

    format_handler = {TableFormats.delta: load_delta_table_metadata}
    return format_handler[table_config.table_format](table_config)


def validate_config(config_path: Path) -> None:
    console = rich.get_console()
    try:
        config = load_yaml_config(config_path)
        console.print(rich.text.Text("Configuration is valid"))
        console.print(config)
    except Exception as e:
        console.print(rich.text.Text("Configuration is invalid"))
        console.print(e)


def list_tables(config_path: Path) -> None:
    config = load_yaml_config(config_path)
    tree = rich.tree.Tree("tables")
    for table in config.tables:
        table_tree = tree.add(table.name)
        table_tree.add(f"format: {table.table_format.value}")
        table_tree.add(f"uri: {table.uri}")
    console = rich.get_console()
    console.print(tree)


def inspect_table(config_path: Path, table_name: str) -> None:
    config = load_yaml_config(config_path)
    table_config = next(filter(lambda x: x.name, config.tables))
    metadata = load_table_metadata(table_config)

    tree = rich.tree.Tree(table_name)
    tree.add(f"name: {metadata.name}")
    tree.add(f"description: {metadata.description}")
    tree.add(f"format: {metadata.table_format.value}")
    tree.add(f"uri: {metadata.uri}")
    tree.add(f"id: {metadata.id}")
    tree.add(f"version: {metadata.version}")
    tree.add(f"created at: {metadata.created_at}")
    tree.add(f"partitions: {', '.join(metadata.partitions)}")
    tree.add(f"configuration: {metadata.configuration}")
    console = rich.get_console()
    console.print(tree)


def cli() -> None:
    parser = argparse.ArgumentParser("laketower")
    parser.add_argument(
        "--config",
        "-c",
        default="laketower.yml",
        type=Path,
        help="Path to the Laketower YAML configuration file",
    )
    subparsers = parser.add_subparsers(title="commands", required=True)

    parser_config = subparsers.add_parser(
        "config", help="Work with configuration", add_help=True
    )
    subsparsers_config = parser_config.add_subparsers(required=True)

    parser_config_validate = subsparsers_config.add_parser("validate")
    parser_config_validate.set_defaults(func=lambda x: validate_config(x.config))

    parser_tables = subparsers.add_parser("tables", help="Work with tables")
    subsparsers_tables = parser_tables.add_subparsers(required=True)

    parser_tables_list = subsparsers_tables.add_parser(
        "list", help="List all registered tables"
    )
    parser_tables_list.set_defaults(func=lambda x: list_tables(x.config))

    parser_tables_inspect = subsparsers_tables.add_parser(
        "inspect", help="Inspect a given table"
    )
    parser_tables_inspect.add_argument("table", help="Name of the table")
    parser_tables_inspect.set_defaults(func=lambda x: inspect_table(x.config, x.table))

    args = parser.parse_args()
    args.func(args)
