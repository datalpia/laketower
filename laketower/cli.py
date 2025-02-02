import argparse
import enum
from pathlib import Path

import deltalake
import pydantic
import rich.text
import rich.tree
import yaml


class TableFormats(enum.StrEnum):
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
    config_dict = yaml.load(config_path.read_text(), Loader=yaml.Loader)
    return Config.model_validate(config_dict)


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
        table_tree.add(f"format: {table.table_format}")
        table_tree.add(f"uri: {table.uri}")
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

    args = parser.parse_args()
    args.func(args)
