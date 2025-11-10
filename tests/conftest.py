from pathlib import Path
from typing import Any

import deltalake
import numpy as np
import pandas as pd
import pytest
import yaml


@pytest.fixture()
def delta_table_data() -> pd.DataFrame:
    periods = 24 * 7
    daterange = pd.date_range(
        start="2025-01-01T00:00:00+00:00", periods=periods, freq="1h"
    )
    temperature = np.round(np.linspace(-5, -5 + periods, num=periods))
    return pd.DataFrame(
        {"time": daterange, "city": "Grenoble", "temperature": temperature}
    )


@pytest.fixture()
def delta_table(tmp_path: Path, delta_table_data: pd.DataFrame) -> deltalake.DeltaTable:
    table_path = tmp_path / "delta_table"
    schema = deltalake.Schema(
        [
            deltalake.Field("time", "timestamp_ntz", nullable=False),  # type: ignore[arg-type]
            deltalake.Field("city", "string", nullable=False),  # type: ignore[arg-type]
            deltalake.Field("temperature", "float", nullable=False),  # type: ignore[arg-type]
        ]
    )
    dt = deltalake.DeltaTable.create(
        table_path, schema, name="delta_table", description="Sample Delta Table"
    )
    deltalake.write_deltalake(dt, delta_table_data, mode="append")
    return dt


@pytest.fixture()
def sample_config(delta_table: deltalake.DeltaTable) -> dict[str, Any]:
    return {
        "settings": {
            "max_query_rows": 1_000,
            "web": {
                "hide_tables": False,
            },
        },
        "tables": [
            {"name": "delta_table", "uri": delta_table.table_uri, "format": "delta"},
            {
                "name": "123_delta_table",  # test table name starting with digits
                "uri": delta_table.table_uri,
                "format": "delta",
            },
            {
                "name": "invalid_uri_table",
                "uri": "path/to/invalid_uri_table",
                "format": "delta",
            },
        ],
        "queries": [
            {
                "name": "daily_average_temperature",
                "title": "Daily average temperature",
                "sql": "select date_trunc('day', time) as day, round(avg(temperature)) as avg_temperature from delta_table group by day order by day asc",
            },
            {
                "name": "daily_average_temperature_params",
                "title": "Daily average temperature with parameters",
                "description": "Display daily average temperature values from `weather` table, with dynamic filters for start and end dates",
                "parameters": {
                    "start_date": {"default": "2025-01-01"},
                    "end_date": {"default": "2025-01-31"},
                },
                "sql": "select date_trunc('day', time) as day, round(avg(temperature)) as avg_temperature from delta_table where day between $start_date and $end_date group by day order by day asc",
            },
        ],
    }


@pytest.fixture()
def sample_config_table_delta_s3() -> dict[str, Any]:
    return {
        "name": "delta_table_s3",
        "uri": "s3://bucket/path/to/table",
        "format": "delta",
        "connection": {
            "s3": {
                "s3_access_key_id": "s3-access-key-id",
                "s3_secret_access_key": "s3-secret-access-key",
                "s3_region": "s3-region",
                "s3_endpoint_url": "https://s3.domain.com/",
                "s3_allow_http": False,
            },
        },
    }


@pytest.fixture()
def sample_config_table_delta_adls() -> dict[str, Any]:
    return {
        "name": "delta_table_adls",
        "uri": "abfss://container/path/to/table",
        "format": "delta",
        "connection": {
            "adls": {
                "adls_account_name": "adls-account-name",
                "adls_access_key": "adls-access-key",
                "adls_sas_key": "adls-sas-key",
                "adls_tenant_id": "adls-tenant-id",
                "adls_client_id": "adls-client-id",
                "adls_client_secret": "adls-client-secret",
                "azure_msi_endpoint": "https://msi.azure.com/",
                "use_azure_cli": False,
            },
        },
    }


@pytest.fixture()
def sample_config_path(tmp_path: Path, sample_config: dict[str, Any]) -> Path:
    config_path = tmp_path / "laketower.yml"
    config_path.write_text(yaml.dump(sample_config))
    return config_path
