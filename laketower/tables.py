from datetime import datetime, timezone
from typing import Protocol

import deltalake
import pyarrow as pa
import pydantic

from laketower.config import ConfigTable, TableFormats


class TableMetadata(pydantic.BaseModel):
    table_format: TableFormats
    name: str
    description: str
    uri: str
    id: str
    version: int
    created_at: datetime
    partitions: list[str]
    configuration: dict[str, str]


class TableProtocol(Protocol):  # pragma: no cover
    def metadata(self) -> TableMetadata: ...
    def schema(self) -> pa.Schema: ...


class DeltaTable:
    def __init__(self, table_config: ConfigTable):
        super().__init__()
        self.table_config = table_config
        self._impl = deltalake.DeltaTable(table_config.uri)

    def metadata(self) -> TableMetadata:
        metadata = self._impl.metadata()
        return TableMetadata(
            table_format=self.table_config.table_format,
            name=metadata.name,
            description=metadata.description,
            uri=self._impl.table_uri,
            id=str(metadata.id),
            version=self._impl.version(),
            created_at=datetime.fromtimestamp(
                metadata.created_time / 1000, tz=timezone.utc
            ),
            partitions=metadata.partition_columns,
            configuration=metadata.configuration,
        )

    def schema(self) -> pa.Schema:
        return self._impl.schema().to_pyarrow()


def load_table(table_config: ConfigTable) -> TableProtocol:
    format_handler = {TableFormats.delta: DeltaTable}
    return format_handler[table_config.table_format](table_config)
