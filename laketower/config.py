import enum
import json
import os
import re
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

        case str() as s if re.search(r"\$\{[^}]+\}", s):
            # Inline ${VAR_NAME} interpolation within string values

            def _replace(m: re.Match[str]) -> str:
                var_name = m.group("var_name")
                value = os.getenv(var_name)
                if value is None:
                    raise ValueError(f"environment variable '{var_name}' is not set")
                return value

            return re.sub(r"\$\{(?P<var_name>[^}]+)\}", _replace, s)

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


def inline_includes(config_dict: dict[str, Any], config_dir: Path) -> dict[str, Any]:
    def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
        result = dict(base)
        for key, override_val in override.items():
            base_val = result.get(key)
            if isinstance(base_val, dict) and isinstance(override_val, dict):
                result[key] = deep_merge(base_val, override_val)
            elif isinstance(base_val, list) and isinstance(override_val, list):
                result[key] = base_val + override_val
            else:
                result[key] = override_val
        return result

    include_paths = config_dict.get("include")
    config_dict = {k: v for k, v in config_dict.items() if k != "include"}

    if not include_paths:
        return config_dict

    if not isinstance(include_paths, list):
        raise TypeError(
            f"'include' must be a list of file paths, got {type(include_paths).__name__}"
        )

    merged: dict[str, Any] = {}
    for rel_path in include_paths:
        included_path = config_dir / rel_path
        if not included_path.exists():
            raise FileNotFoundError(f"included config file not found: {included_path}")
        included = yaml.safe_load(included_path.read_text())
        if not isinstance(included, dict):
            raise TypeError(
                f"included config file must be a YAML mapping: {included_path}"
            )
        if "include" in included:
            raise ValueError(
                f"nested 'include' is not supported in included files: {included_path}"
            )
        merged = deep_merge(merged, included)

    return deep_merge(merged, config_dict)


def load_yaml_config(config_path: Path) -> Config:
    config_dict = yaml.safe_load(config_path.read_text())
    config_dict = inline_includes(config_dict, config_path.parent)
    config_dict = substitute_env_vars(config_dict)
    return Config.model_validate(config_dict)
