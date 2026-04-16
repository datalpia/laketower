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


class ConfigStorageCredentialS3(pydantic.BaseModel):
    access_key_id: str
    secret_access_key: pydantic.SecretStr
    region: str | None = None
    endpoint_url: pydantic.AnyHttpUrl | None = None
    allow_http: bool = False


class ConfigStorageCredentialADLS(pydantic.BaseModel):
    account_name: str
    access_key: pydantic.SecretStr | None = None
    sas_key: pydantic.SecretStr | None = None
    tenant_id: str | None = None
    client_id: str | None = None
    client_secret: pydantic.SecretStr | None = None
    msi_endpoint: pydantic.AnyHttpUrl | None = None
    use_azure_cli: bool = False


class ConfigStorageCredential(pydantic.BaseModel):
    s3: ConfigStorageCredentialS3 | None = None
    adls: ConfigStorageCredentialADLS | None = None

    @pydantic.model_validator(mode="after")
    def at_least_one_storage_type(self) -> "ConfigStorageCredential":
        if not any([self.s3, self.adls]):
            raise ValueError(
                "at least one storage type must be specified among: 's3', 'adls'"
            )
        return self

    @pydantic.model_validator(mode="after")
    def mutually_exclusive_storage_types(self) -> "ConfigStorageCredential":
        if len(list(filter(None, [self.s3, self.adls]))) > 1:
            raise ValueError(
                "only one storage type can be specified among: 's3', 'adls'"
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
    storage_credential: ConfigStorageCredential | None = None


class ConfigQueryParameter(pydantic.BaseModel):
    default: str


class ConfigQuery(pydantic.BaseModel):
    name: str
    title: str
    description: str | None = None
    totals_row: bool = False
    parameters: dict[str, ConfigQueryParameter] = {}
    sql: str


class Config(pydantic.BaseModel):
    settings: ConfigSettings = ConfigSettings()
    storage_credentials: dict[str, ConfigStorageCredential] = {}
    tables: list[ConfigTable] = []
    queries: list[ConfigQuery] = []

    @pydantic.model_validator(mode="before")
    @classmethod
    def resolve_storage_credential_refs(cls, data: dict[str, Any]) -> dict[str, Any]:
        credentials: dict[str, Any] = data.get("storage_credentials", {})
        for table in data.get("tables", []):
            ref = table.get("storage_credential")
            if isinstance(ref, str):
                if ref not in credentials:
                    raise ValueError(
                        f"table '{table.get('name')}' references unknown storage credential '{ref}'"
                    )
                table["storage_credential"] = credentials[ref]
        return data


def load_yaml_config(config_path: Path) -> Config:
    config_dict = yaml.safe_load(config_path.read_text())
    config_dict = substitute_env_vars(config_dict)
    return Config.model_validate(config_dict)
