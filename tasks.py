import tempfile
import time
from pathlib import Path

import yaml
from invoke import task
from invoke.context import Context

app_path = "laketower"
tests_path = "tests"


@task
def format(ctx: Context) -> None:
    ctx.run("ruff format .", echo=True, pty=True)


@task
def audit(ctx: Context):
    ctx.run("pip-audit", echo=True, pty=True)


@task
def vuln(ctx: Context):
    ctx.run(f"bandit -r {app_path}", echo=True, pty=True)


@task
def lint(ctx: Context) -> None:
    ctx.run("ruff check .", echo=True, pty=True)


@task
def typing(ctx: Context) -> None:
    ctx.run(f"mypy --strict {app_path} {tests_path}", echo=True, pty=True)


@task
def test(ctx: Context):
    ctx.run(
        f"py.test -v --cov={app_path} --cov={tests_path} --cov-branch --cov-report=term-missing {tests_path}",
        echo=True,
        pty=True,
    )


@task(audit, vuln, lint, typing, test)
def qa(ctx: Context):
    pass


@task
def shots(ctx: Context):
    server_url = "http://localhost:8000"
    screenshots_path = Path(__file__).parent / "docs" / "static"
    screenshots = [
        {
            "url": f"{server_url}/tables/weather",
            "output": screenshots_path / "tables_overview.png",
        },
        {
            "url": f"{server_url}/tables/weather/view",
            "output": screenshots_path / "tables_view.png",
        },
        {
            "url": f"{server_url}/tables/weather/statistics",
            "output": screenshots_path / "tables_statistics.png",
        },
        {
            "url": f"{server_url}/tables/weather/history",
            "output": screenshots_path / "tables_history.png",
        },
        {
            "url": f"{server_url}/tables/query?sql=SELECT * FROM weather LIMIT 10",
            "output": screenshots_path / "tables_query.png",
        },
        {
            "url": f"{server_url}/queries/daily_avg_temperature/view",
            "output": screenshots_path / "queries_view.png",
        },
    ]
    shot_scraper_config = [
        {"wait": 100, "width": 1440, "url": shot["url"], "output": str(shot["output"])}
        for shot in screenshots
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        shots_yml = Path(tmpdir) / "shots.yml"
        shots_yml.write_text(yaml.dump(shot_scraper_config))

        server = ctx.run(
            "uv run laketower -c demo/laketower.yml web",
            asynchronous=True,
            echo=True,
            pty=True,
        )
        try:
            time.sleep(5)
            ctx.run(
                f"uvx shot-scraper multi {shots_yml} --timeout 10000 --retina",
                echo=True,
                pty=True,
            )
        finally:
            server.runner.kill()
