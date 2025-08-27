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
