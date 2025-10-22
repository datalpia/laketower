import shutil
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
def audit(ctx: Context) -> None:
    ignored_vulns = [
        "GHSA-4xh5-x5gv-qwph",  # pip<=25.2 affected, no resolution yet
    ]
    options = [f"--ignore-vuln {vuln}" for vuln in ignored_vulns]
    ctx.run(f"pip-audit {' '.join(options)}", echo=True, pty=True)


@task
def vuln(ctx: Context) -> None:
    ctx.run(f"bandit -r {app_path}", echo=True, pty=True)


@task
def lint(ctx: Context) -> None:
    ctx.run("ruff check .", echo=True, pty=True)


@task
def typing(ctx: Context) -> None:
    ctx.run(f"mypy --strict {app_path} {tests_path}", echo=True, pty=True)


@task
def test(ctx: Context) -> None:
    ctx.run(
        f"py.test -v --cov={app_path} --cov={tests_path} --cov-branch --cov-report=term-missing {tests_path}",
        echo=True,
        pty=True,
    )


@task(audit, vuln, lint, typing, test)
def qa(ctx: Context):
    pass


@task
def shots(ctx: Context) -> None:
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
            "url": f"{server_url}/tables/weather/import",
            "output": screenshots_path / "tables_import.png",
        },
        {
            "url": f"{server_url}/tables/weather/history",
            "output": screenshots_path / "tables_history.png",
        },
        {
            "url": f"{server_url}/tables/query?sql=select * from weather where temperature_2m < $temperature_min limit 10&temperature_min=2",
            "output": screenshots_path / "tables_query.png",
        },
        {
            "url": f"{server_url}/queries/daily_avg_temperature_params/view",
            "output": screenshots_path / "queries_view.png",
        },
    ]
    shot_scraper_config = [
        {
            "wait": 100,
            "width": 1440,
            "height": 720,
            "url": shot["url"],
            "output": str(shot["output"]),
        }
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
        ctx.run(
            "uvx shot-scraper install",
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


@task
def vendor_static_assets(ctx: Context) -> None:
    base_path = (Path(__file__).parent / "laketower" / "static" / "vendor").absolute()

    node_packages = {
        "bootstrap": ["bootstrap/dist/js/bootstrap.bundle.min.js"],
        "bootstrap-icons": [
            "bootstrap-icons/font/bootstrap-icons.min.css",
            "bootstrap-icons/font/fonts",
        ],
        "datatables.net-bs5": [
            "datatables.net-bs5/css/dataTables.bootstrap5.css",
        ],
        "datatables.net-columncontrol-bs5": [
            "datatables.net-columncontrol-bs5/css/columnControl.bootstrap5.min.css",
        ],
        "halfmoon": [
            "halfmoon/css/halfmoon.min.css",
            "halfmoon/css/cores/halfmoon.modern.css",
        ],
    }

    bundles = [
        {
            "src": "laketower/static/datatables.js",
            "dest": "laketower/static/datatables.bundle.js",
            "name": "datatables",
        },
        {
            "src": "laketower/static/editor.js",
            "dest": "laketower/static/editor.bundle.js",
            "name": "editor",
        },
    ]

    ctx.run("npm install", echo=True, pty=True)

    for bundle in bundles:
        ctx.run(
            f"node_modules/.bin/rollup {bundle['src']} \
                -o {bundle['dest']} \
                -f iife \
                -n {bundle['name']} \
                -p @rollup/plugin-node-resolve \
                -p @rollup/plugin-commonjs"
        )

    for package_name, package_files in node_packages.items():
        print("vendoring package:", package_name)
        dst = base_path / package_name
        shutil.rmtree(dst, ignore_errors=True)
        dst.mkdir(parents=True, exist_ok=True)
        for package_file in package_files:
            src = Path("node_modules") / package_file
            if src.is_dir():
                print("copying dir:", src)
                shutil.copytree(src, dst / src.name, dirs_exist_ok=True)
            else:
                print("copying file:", src)
                shutil.copy(src, dst)
