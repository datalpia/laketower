from typing import Any
from unittest import mock

import pytest

from laketower import config, tables


@mock.patch("laketower.tables.deltalake.DeltaTable")
def test_load_table_deltatable_s3(
    mock_deltatable: mock.MagicMock, sample_config_table_delta_s3: dict[str, Any]
) -> None:
    table_config = config.ConfigTable.model_validate(sample_config_table_delta_s3)

    _ = tables.load_table(table_config)

    expected_s3_conn = sample_config_table_delta_s3["connection"]["s3"]
    assert mock_deltatable.call_count == 1
    assert mock_deltatable.call_args.kwargs["storage_options"] == {
        "aws_access_key_id": expected_s3_conn["s3_access_key_id"],
        "aws_secret_access_key": expected_s3_conn["s3_secret_access_key"],
        "aws_region": expected_s3_conn["s3_region"],
        "aws_endpoint_url": str(expected_s3_conn["s3_endpoint_url"]).rstrip("/"),
        "aws_allow_http": str(expected_s3_conn["s3_allow_http"]).lower(),
    }


@mock.patch("laketower.tables.deltalake.DeltaTable")
def test_load_table_deltatable_adls(
    mock_deltatable: mock.MagicMock, sample_config_table_delta_adls: dict[str, Any]
) -> None:
    table_config = config.ConfigTable.model_validate(sample_config_table_delta_adls)

    _ = tables.load_table(table_config)

    expected_adls_conn = sample_config_table_delta_adls["connection"]["adls"]
    assert mock_deltatable.call_count == 1
    assert mock_deltatable.call_args.kwargs["storage_options"] == {
        "azure_storage_account_name": expected_adls_conn["adls_account_name"],
        "azure_storage_access_key": expected_adls_conn["adls_access_key"],
        "azure_storage_sas_key": expected_adls_conn["adls_sas_key"],
        "azure_storage_tenant_id": expected_adls_conn["adls_tenant_id"],
        "azure_storage_client_id": expected_adls_conn["adls_client_id"],
        "azure_storage_client_secret": expected_adls_conn["adls_client_secret"],
        "azure_msi_endpoint": str(expected_adls_conn["azure_msi_endpoint"]).rstrip("/"),
        "azure_use_azure_cli": str(expected_adls_conn["use_azure_cli"]).lower(),
    }


@pytest.mark.parametrize(
    ("sql", "names"),
    [
        ("select * from table", set()),
        ("select * from table where col = $val", {"val"}),
        ("select * from table where col1 = $val1 and col2 = $val2", {"val1", "val2"}),
    ],
)
def test_extract_query_parameter_names(sql: str, names: set[str]) -> None:
    param_names = tables.extract_query_parameter_names(sql)
    assert param_names == names


@pytest.mark.parametrize(
    ["table_name", "limit", "cols", "sort_asc", "sort_desc", "expected_query"],
    [
        ("test_table", None, None, None, None, 'SELECT * FROM "test_table" LIMIT 10'),
        ("test_table", 5, None, None, None, 'SELECT * FROM "test_table" LIMIT 5'),
        (
            "test_table",
            5,
            ["col1", "col2"],
            "col1",
            None,
            'SELECT "col1", "col2" FROM "test_table" ORDER BY "col1" ASC NULLS FIRST LIMIT 5',
        ),
        (
            "test_table",
            5,
            ["col1", "col2"],
            None,
            "col1",
            'SELECT "col1", "col2" FROM "test_table" ORDER BY "col1" DESC LIMIT 5',
        ),
        ("123_table", None, None, None, None, 'SELECT * FROM "123_table" LIMIT 10'),
        (
            "123_table",
            None,
            ["123_col"],
            None,
            None,
            'SELECT "123_col" FROM "123_table" LIMIT 10',
        ),
    ],
)
def test_generate_table_query_success(
    table_name: str,
    limit: int | None,
    cols: list[str] | None,
    sort_asc: str | None,
    sort_desc: str | None,
    expected_query: str,
) -> None:
    query = tables.generate_table_query(table_name, limit, cols, sort_asc, sort_desc)
    assert query == expected_query


@pytest.mark.parametrize("table_name", ["test_table", "123_table"])
def test_generate_table_statistics_query_success(table_name: str) -> None:
    expected_query = f'SELECT "column_name", "count", "avg", "std", "min", "max" FROM (SUMMARIZE "{table_name}")'
    query = tables.generate_table_statistics_query(table_name)
    assert query == expected_query
