import enum
from pathlib import Path

import pydantic
import yaml


class TableFormats(str, enum.Enum):
    delta = "delta"


class ConfigTable(pydantic.BaseModel):
    name: str
    uri: str
    table_format: TableFormats = pydantic.Field(alias="format")


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
