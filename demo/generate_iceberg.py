from pathlib import Path
import pyarrow as pa
import pandas as pd
from pyiceberg.catalog import load_catalog


def generate() -> None:
    warehouse_path = (Path(__file__).parent / "iceberg_warehouse").absolute()
    warehouse_path.mkdir(parents=True, exist_ok=True)
    catalog = load_catalog(
        "default",
        **{
            "type": "sql",
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )
    catalog.create_namespace("default")

    data = [
        {"num": 1, "letter": "a"},
        {"num": 2, "letter": "b"},
        {"num": 3, "letter": "c"},
    ]
    pa_table = pa.Table.from_pylist(data)
    table = catalog.create_table(
        "default.sample",
        schema=pa_table.schema,
    )
    table.append(pa_table)


if __name__ == "__main__":
    generate()
