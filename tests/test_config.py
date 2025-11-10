from pathlib import Path
from typing import Any

import pydantic
import pytest
import yaml

from laketower import config


def test_load_yaml_config(
    sample_config: dict[str, Any], sample_config_path: Path
) -> None:
    conf = config.load_yaml_config(sample_config_path)

    assert conf.settings.max_query_rows == sample_config["settings"]["max_query_rows"]
    assert (
        conf.settings.web.hide_tables == sample_config["settings"]["web"]["hide_tables"]
    )

    for table, expected_table in zip(conf.tables, sample_config["tables"], strict=True):
        assert table.name == expected_table["name"]
        assert table.uri == expected_table["uri"]
        assert table.table_format.value == expected_table["format"]
        assert table.connection is None

    for query, expected_query in zip(
        conf.queries, sample_config["queries"], strict=True
    ):
        assert query.name == expected_query["name"]
        assert query.title == expected_query["title"]
        assert query.description == expected_query.get("description")
        assert query.sql == expected_query["sql"]


def test_load_yaml_config_table_delta_s3(
    tmp_path: Path,
    sample_config: dict[str, Any],
    sample_config_table_delta_s3: dict[str, Any],
) -> None:
    sample_config["tables"] = [sample_config_table_delta_s3]
    sample_config_path = tmp_path / "laketower.yml"
    sample_config_path.write_text(yaml.dump(sample_config))

    conf = config.load_yaml_config(sample_config_path)
    assert len(conf.tables) == 1

    table = conf.tables[0]
    assert table.name == sample_config_table_delta_s3["name"]
    assert table.uri == sample_config_table_delta_s3["uri"]
    assert table.table_format.value == sample_config_table_delta_s3["format"]
    assert table.connection is not None
    assert table.connection.s3 is not None

    expected_s3_conn = sample_config_table_delta_s3["connection"]["s3"]
    table_s3_conn = table.connection.s3
    assert table_s3_conn.s3_access_key_id == expected_s3_conn["s3_access_key_id"]
    assert (
        table_s3_conn.s3_secret_access_key.get_secret_value()
        == expected_s3_conn["s3_secret_access_key"]
    )
    assert table_s3_conn.s3_region == expected_s3_conn["s3_region"]
    assert str(table_s3_conn.s3_endpoint_url) == expected_s3_conn["s3_endpoint_url"]
    assert table_s3_conn.s3_allow_http == expected_s3_conn["s3_allow_http"]


def test_load_yaml_config_table_delta_adls(
    tmp_path: Path,
    sample_config: dict[str, Any],
    sample_config_table_delta_adls: dict[str, Any],
) -> None:
    sample_config["tables"] = [sample_config_table_delta_adls]
    sample_config_path = tmp_path / "laketower.yml"
    sample_config_path.write_text(yaml.dump(sample_config))

    conf = config.load_yaml_config(sample_config_path)
    assert len(conf.tables) == 1

    table = conf.tables[0]
    assert table.name == sample_config_table_delta_adls["name"]
    assert table.uri == sample_config_table_delta_adls["uri"]
    assert table.table_format.value == sample_config_table_delta_adls["format"]
    assert table.connection is not None
    assert table.connection.adls is not None

    expected_adls_conn = sample_config_table_delta_adls["connection"]["adls"]
    table_adls_conn = table.connection.adls
    assert table_adls_conn.adls_account_name == expected_adls_conn["adls_account_name"]
    assert table_adls_conn.adls_access_key and (
        table_adls_conn.adls_access_key.get_secret_value()
        == expected_adls_conn["adls_access_key"]
    )
    assert table_adls_conn.adls_sas_key and (
        table_adls_conn.adls_sas_key.get_secret_value()
        == expected_adls_conn["adls_sas_key"]
    )
    assert table_adls_conn.adls_tenant_id == expected_adls_conn["adls_tenant_id"]
    assert table_adls_conn.adls_client_id == expected_adls_conn["adls_client_id"]
    assert table_adls_conn.adls_client_secret and (
        table_adls_conn.adls_client_secret.get_secret_value()
        == expected_adls_conn["adls_client_secret"]
    )
    assert (
        str(table_adls_conn.azure_msi_endpoint)
        == expected_adls_conn["azure_msi_endpoint"]
    )
    assert table_adls_conn.use_azure_cli == expected_adls_conn["use_azure_cli"]


def test_load_yaml_config_table_connection_mutually_exclusive(
    tmp_path: Path,
    sample_config: dict[str, Any],
    sample_config_table_delta_s3: dict[str, Any],
    sample_config_table_delta_adls: dict[str, Any],
) -> None:
    sample_config_table_delta_remote = {
        "name": "remote_delta_table",
        "uri": "somewhere",
        "format": "delta",
        "connection": sample_config_table_delta_s3["connection"]
        | sample_config_table_delta_adls["connection"],
    }
    sample_config["tables"] = [sample_config_table_delta_remote]
    sample_config_path = tmp_path / "laketower.yml"
    sample_config_path.write_text(yaml.dump(sample_config))

    with pytest.raises(
        pydantic.ValidationError,
        match="only one connection type can be specified among: 's3', 'adls'",
    ):
        config.load_yaml_config(sample_config_path)


@pytest.mark.parametrize(
    ("var_name", "var_value", "expected_result"),
    [
        ("TEST_STRING", "test_value", "test_value"),
        ("TEST_JSON", '{"key": "value", "number": 42}', {"key": "value", "number": 42}),
        ("TEST_ARRAY", '["item1", "item2", 42]', ["item1", "item2", 42]),
        ("TEST_BOOL", "true", True),
        ("TEST_NUMBER", "42", 42),
    ],
)
def test_substitute_env_vars(
    monkeypatch: pytest.MonkeyPatch, var_name: str, var_value: str, expected_result: Any
) -> None:
    monkeypatch.setenv(var_name, var_value)
    result = config.substitute_env_vars({"env": var_name})
    assert result == expected_result


def test_substitute_env_vars_nested_dict(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NESTED_VALUE", "nested_result")
    input_dict = {
        "level1": {"level2": {"env_var": {"env": "NESTED_VALUE"}, "regular": "value"}},
        "other": "data",
    }

    result = config.substitute_env_vars(input_dict)
    assert result == {
        "level1": {"level2": {"env_var": "nested_result", "regular": "value"}},
        "other": "data",
    }


def test_substitute_env_vars_list_with_env_vars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LIST_ITEM", "from_env")
    input_list = [
        "regular_item",
        {"env": "LIST_ITEM"},
        {"nested": {"env": "LIST_ITEM"}},
    ]

    result = config.substitute_env_vars(input_list)
    assert result == ["regular_item", "from_env", {"nested": "from_env"}]


def test_substitute_env_vars_env_var_not_set() -> None:
    var_name = "NONEXISTENT_VAR"
    with pytest.raises(
        ValueError, match=f"environment variable '{var_name}' is not set"
    ):
        config.substitute_env_vars({"env": var_name})


def test_substitute_env_vars_dict_with_other_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TEST_VAR", "test_value")
    input_dict = {"env": "TEST_VAR", "other": "key"}

    result = config.substitute_env_vars(input_dict)
    assert result == {"env": "TEST_VAR", "other": "key"}


def test_substitute_env_vars_no_changes_needed() -> None:
    input_dict = {
        "tables": [{"name": "test", "uri": "path/to/table", "format": "delta"}],
        "queries": [],
    }

    result = config.substitute_env_vars(input_dict)
    assert result == input_dict


def test_load_yaml_config_with_env_var_substitution(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("TABLE_NAME", "env_table")
    monkeypatch.setenv("TABLE_URI", "env/path/to/table")
    monkeypatch.setenv("TABLE_FORMAT", "delta")

    config_dict = {
        "tables": [
            {
                "name": {"env": "TABLE_NAME"},
                "uri": {"env": "TABLE_URI"},
                "format": {"env": "TABLE_FORMAT"},
            }
        ],
        "queries": [],
    }

    config_path = tmp_path / "test_config.yml"
    config_path.write_text(yaml.dump(config_dict))

    conf = config.load_yaml_config(config_path)

    assert len(conf.tables) == 1
    assert conf.tables[0].name == "env_table"
    assert conf.tables[0].uri == "env/path/to/table"
    assert conf.tables[0].table_format.value == "delta"
