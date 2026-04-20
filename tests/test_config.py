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
        assert table.storage_credential is None

    for query, expected_query in zip(
        conf.queries, sample_config["queries"], strict=True
    ):
        assert query.name == expected_query["name"]
        assert query.title == expected_query["title"]
        assert query.description == expected_query.get("description")
        assert query.totals_row == expected_query.get("totals_row", False)
        assert query.sql == expected_query["sql"]


def test_load_yaml_config_storage_credentials_s3(
    tmp_path: Path,
    sample_config: dict[str, Any],
    sample_storage_credential_s3: dict[str, Any],
) -> None:
    sample_config["storage_credentials"] = {"my_s3": sample_storage_credential_s3}
    sample_config["tables"] = [
        {
            "name": "delta_table_s3",
            "uri": "s3://bucket/path/to/table",
            "format": "delta",
            "storage_credential": "my_s3",
        }
    ]
    sample_config_path = tmp_path / "laketower.yml"
    sample_config_path.write_text(yaml.dump(sample_config))

    conf = config.load_yaml_config(sample_config_path)
    assert len(conf.tables) == 1

    table = conf.tables[0]
    assert table.name == "delta_table_s3"
    assert table.storage_credential is not None
    assert table.storage_credential.s3 is not None

    expected_s3 = sample_storage_credential_s3["s3"]
    cred_s3 = table.storage_credential.s3
    assert cred_s3.access_key_id == expected_s3["access_key_id"]
    assert (
        cred_s3.secret_access_key.get_secret_value() == expected_s3["secret_access_key"]
    )
    assert cred_s3.region == expected_s3["region"]
    assert str(cred_s3.endpoint_url) == expected_s3["endpoint_url"]
    assert cred_s3.allow_http == expected_s3["allow_http"]


def test_load_yaml_config_storage_credentials_adls(
    tmp_path: Path,
    sample_config: dict[str, Any],
    sample_storage_credential_adls: dict[str, Any],
) -> None:
    sample_config["storage_credentials"] = {"my_adls": sample_storage_credential_adls}
    sample_config["tables"] = [
        {
            "name": "delta_table_adls",
            "uri": "abfss://container/path/to/table",
            "format": "delta",
            "storage_credential": "my_adls",
        }
    ]
    sample_config_path = tmp_path / "laketower.yml"
    sample_config_path.write_text(yaml.dump(sample_config))

    conf = config.load_yaml_config(sample_config_path)
    assert len(conf.tables) == 1

    table = conf.tables[0]
    assert table.name == "delta_table_adls"
    assert table.storage_credential is not None
    assert table.storage_credential.adls is not None

    expected_adls = sample_storage_credential_adls["adls"]
    cred_adls = table.storage_credential.adls
    assert cred_adls.account_name == expected_adls["account_name"]
    assert cred_adls.access_key and (
        cred_adls.access_key.get_secret_value() == expected_adls["access_key"]
    )
    assert cred_adls.sas_key and (
        cred_adls.sas_key.get_secret_value() == expected_adls["sas_key"]
    )
    assert cred_adls.tenant_id == expected_adls["tenant_id"]
    assert cred_adls.client_id == expected_adls["client_id"]
    assert cred_adls.client_secret and (
        cred_adls.client_secret.get_secret_value() == expected_adls["client_secret"]
    )
    assert str(cred_adls.msi_endpoint) == expected_adls["msi_endpoint"]
    assert cred_adls.use_azure_cli == expected_adls["use_azure_cli"]


def test_load_yaml_config_storage_credentials_unknown_ref(
    tmp_path: Path,
    sample_config: dict[str, Any],
) -> None:
    sample_config["tables"] = [
        {
            "name": "delta_table_s3",
            "uri": "s3://bucket/path/to/table",
            "format": "delta",
            "storage_credential": "nonexistent",
        }
    ]
    sample_config_path = tmp_path / "laketower.yml"
    sample_config_path.write_text(yaml.dump(sample_config))

    with pytest.raises(
        pydantic.ValidationError,
        match="references unknown storage credential 'nonexistent'",
    ):
        config.load_yaml_config(sample_config_path)


def test_load_yaml_config_storage_credentials_mutually_exclusive(
    tmp_path: Path,
    sample_config: dict[str, Any],
    sample_storage_credential_s3: dict[str, Any],
    sample_storage_credential_adls: dict[str, Any],
) -> None:
    sample_config["storage_credentials"] = {
        "both": sample_storage_credential_s3 | sample_storage_credential_adls
    }
    sample_config["tables"] = [
        {
            "name": "delta_table",
            "uri": "somewhere",
            "format": "delta",
            "storage_credential": "both",
        }
    ]
    sample_config_path = tmp_path / "laketower.yml"
    sample_config_path.write_text(yaml.dump(sample_config))

    with pytest.raises(
        pydantic.ValidationError,
        match="only one storage type can be specified among: 's3', 'adls'",
    ):
        config.load_yaml_config(sample_config_path)


def test_load_yaml_config_storage_credentials_empty(
    tmp_path: Path,
    sample_config: dict[str, Any],
) -> None:
    sample_config["storage_credentials"] = {"empty": {}}
    sample_config["tables"] = [
        {
            "name": "delta_table",
            "uri": "somewhere",
            "format": "delta",
            "storage_credential": "empty",
        }
    ]
    sample_config_path = tmp_path / "laketower.yml"
    sample_config_path.write_text(yaml.dump(sample_config))

    with pytest.raises(
        pydantic.ValidationError,
        match="at least one storage type must be specified among: 's3', 'adls'",
    ):
        config.load_yaml_config(sample_config_path)


def test_load_yaml_config_storage_credentials_shared(
    tmp_path: Path,
    sample_config: dict[str, Any],
    sample_storage_credential_s3: dict[str, Any],
) -> None:
    sample_config["storage_credentials"] = {"shared_s3": sample_storage_credential_s3}
    sample_config["tables"] = [
        {
            "name": "table_a",
            "uri": "s3://bucket/table_a",
            "format": "delta",
            "storage_credential": "shared_s3",
        },
        {
            "name": "table_b",
            "uri": "s3://bucket/table_b",
            "format": "delta",
            "storage_credential": "shared_s3",
        },
    ]
    sample_config_path = tmp_path / "laketower.yml"
    sample_config_path.write_text(yaml.dump(sample_config))

    conf = config.load_yaml_config(sample_config_path)
    assert len(conf.tables) == 2

    expected_key = sample_storage_credential_s3["s3"]["access_key_id"]
    for table in conf.tables:
        assert table.storage_credential is not None
        assert table.storage_credential.s3 is not None
        assert table.storage_credential.s3.access_key_id == expected_key


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


def test_substitute_env_vars_inline_single(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BUCKET_NAME", "my-bucket")
    result = config.substitute_env_vars("s3://${BUCKET_NAME}/tables/my_table")
    assert result == "s3://my-bucket/tables/my_table"


def test_substitute_env_vars_inline_multiple(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BUCKET", "my-bucket")
    monkeypatch.setenv("PREFIX", "my-prefix")
    result = config.substitute_env_vars("s3://${BUCKET}/${PREFIX}/table")
    assert result == "s3://my-bucket/my-prefix/table"


def test_substitute_env_vars_inline_missing_raises() -> None:
    with pytest.raises(
        ValueError, match="environment variable 'MISSING_VAR' is not set"
    ):
        config.substitute_env_vars("s3://${MISSING_VAR}/table")


def test_substitute_env_vars_inline_in_nested_dict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BUCKET", "my-bucket")
    monkeypatch.setenv("WHOLE_NAME", "my-table")
    result = config.substitute_env_vars(
        {"uri": "s3://${BUCKET}/table", "name": {"env": "WHOLE_NAME"}}
    )
    assert result == {"uri": "s3://my-bucket/table", "name": "my-table"}


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
