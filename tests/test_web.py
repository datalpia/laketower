from http import HTTPStatus
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from laketower import web


@pytest.fixture()
def app(monkeypatch: pytest.MonkeyPatch, sample_config_path: Path) -> FastAPI:
    monkeypatch.setenv("LAKETOWER_CONFIG_PATH", str(sample_config_path.absolute()))
    return web.create_app()


@pytest.fixture()
def client(app: FastAPI) -> TestClient:
    return TestClient(app)


def test_index(client: TestClient, sample_config: dict[str, Any]) -> None:
    response = client.get("/")
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    for table in sample_config["tables"]:
        assert table["name"] in html
