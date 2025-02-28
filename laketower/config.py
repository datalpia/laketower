import enum
from pathlib import Path

import deltalake
import pydantic
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
    title: str
    sql: str


class ConfigDashboard(pydantic.BaseModel):
    name: str


class Config(pydantic.BaseModel):
    tables: list[ConfigTable] = []
    queries: list[ConfigQuery] = []


def load_yaml_config(config_path: Path) -> Config:
    config_dict = yaml.safe_load(config_path.read_text())
    return Config.model_validate(config_dict)
