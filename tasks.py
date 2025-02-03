from invoke import task
from invoke.context import Context

app_path = "laketower"
tests_path = "tests"


@task
def format(ctx: Context) -> None:
    ctx.run("ruff format .", echo=True, pty=True)


@task
def audit(ctx):
    ctx.run("pip-audit", echo=True, pty=True)


@task
def vuln(ctx):
    ctx.run(f"bandit -r {app_path}", echo=True, pty=True)


@task
def lint(ctx: Context) -> None:
    ctx.run("ruff check .", echo=True, pty=True)


@task
def typing(ctx: Context) -> None:
    ctx.run(f"mypy --strict {app_path} {tests_path}", echo=True, pty=True)


@task
def test(ctx):
    ctx.run(
        f"py.test -v --cov={app_path} --cov={tests_path} --cov-branch --cov-report=term-missing {tests_path}",
        echo=True,
        pty=True,
    )


@task(audit, vuln, lint, typing, test)
def qa(ctx):
    pass
