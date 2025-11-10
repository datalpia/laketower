import enum
import json
import os
from pathlib import Path
from typing import Any

import pydantic
import yaml


def substitute_env_vars(config_data: Any) -> Any:
    """
    Substitute environment variables within the input payload.

    Only allowed format:
    ```python
    {
        "some_key": {"env": "VAR_NAME"}
    }

    If the "env" key MUST BE the only key in the dict to be processed.

    The content of the environment variable will be loaded with a JSON parser,
    so it can contain complex and nested structures (default is a string).
    ```
    """
    match config_data:
        case {"env": str(var_name)} if len(config_data) == 1:
            # Handle environment variable substitution
            env_value = os.getenv(var_name)
            if env_value is None:
                raise ValueError(f"environment variable '{var_name}' is not set")

            try:
                return json.loads(env_value)
            except json.JSONDecodeError:
                return env_value

        case dict() as config_dict:
            # Process dictionary recursively
            return {
                key: substitute_env_vars(value) for key, value in config_dict.items()
            }

        case list() as config_list:
            # Process list recursively
            return [substitute_env_vars(item) for item in config_list]

        case _:
            # Return primitive values unchanged
            return config_data


class TableFormats(str, enum.Enum):
    delta = "delta"


class ConfigTableConnectionS3(pydantic.BaseModel):
    s3_access_key_id: str
    s3_secret_access_key: pydantic.SecretStr
    s3_region: str | None = None
    s3_endpoint_url: pydantic.AnyHttpUrl | None = None
    s3_allow_http: bool = False


class ConfigTableConnectionADLS(pydantic.BaseModel):
    adls_account_name: str
    adls_access_key: pydantic.SecretStr | None = None
    adls_sas_key: pydantic.SecretStr | None = None
    adls_tenant_id: str | None = None
    adls_client_id: str | None = None
    adls_client_secret: pydantic.SecretStr | None = None
    azure_msi_endpoint: pydantic.AnyHttpUrl | None = None
    use_azure_cli: bool = False


class ConfigTableConnection(pydantic.BaseModel):
    s3: ConfigTableConnectionS3 | None = None
    adls: ConfigTableConnectionADLS | None = None

    @pydantic.model_validator(mode="after")
    def mutually_exclusive_connectors(self) -> "ConfigTableConnection":
        connectors = [self.s3, self.adls]
        non_null_connectors = list(filter(None, connectors))
        if len(non_null_connectors) > 1:
            raise ValueError(
                "only one connection type can be specified among: 's3', 'adls'"
            )
        return self


class ConfigSettingsWeb(pydantic.BaseModel):
    hide_tables: bool = False


class ConfigSettings(pydantic.BaseModel):
    max_query_rows: int = 1_000
    web: ConfigSettingsWeb = ConfigSettingsWeb()


class ConfigTable(pydantic.BaseModel):
    name: str
    uri: str
    table_format: TableFormats = pydantic.Field(alias="format")
    connection: ConfigTableConnection | None = None


class ConfigQueryParameter(pydantic.BaseModel):
    default: str


class ConfigQuery(pydantic.BaseModel):
    name: str
    title: str
    description: str | None = None
    parameters: dict[str, ConfigQueryParameter] = {}
    sql: str


class Config(pydantic.BaseModel):
    settings: ConfigSettings = ConfigSettings()
    tables: list[ConfigTable] = []
    queries: list[ConfigQuery] = []


def load_yaml_config(config_path: Path) -> Config:
    config_dict = yaml.safe_load(config_path.read_text())
    config_dict = substitute_env_vars(config_dict)
    return Config.model_validate(config_dict)
