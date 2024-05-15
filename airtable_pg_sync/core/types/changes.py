import dataclasses
import typing

from . import env_types
from ..types import concepts


@dataclasses.dataclass
class CellChange:
    table_id: concepts.TableId
    row_id: concepts.RowId
    field_id: concepts.FieldId
    value: typing.Any


@dataclasses.dataclass
class NewRow:
    table_id: concepts.TableId
    row: concepts.Row


@dataclasses.dataclass
class DestroyedRow:
    table_id: concepts.TableId
    row_id: concepts.RowId


@dataclasses.dataclass
class NewField:
    table_id: concepts.TableId
    field: concepts.Field


@dataclasses.dataclass
class DestroyedField:
    table_id: concepts.TableId
    field_id: concepts.FieldId


@dataclasses.dataclass
class FieldTypeChange:
    table_id: concepts.TableId
    field_id: concepts.FieldId
    field_type: str

    def __post_init__(self):
        self.field_type = self.field_type.upper().strip()

        if self.field_type in concepts.TYPE_MAPPING:
            # Convert from airtable type to postgres type
            self.field_type = concepts.TYPE_MAPPING[self.field_type]

        elif self.field_type in concepts.DB_TYPES:
            # Already a postgres type
            pass

        else:
            # Unknown type
            raise concepts.UnknownType(f'Unknown type ({self.field_type}) found for field (id: {self.field_id})')


@dataclasses.dataclass
class FieldNameChange:
    table_id: concepts.TableId
    field_id: concepts.FieldId
    field_name: str


@dataclasses.dataclass
class NewTable:
    table: concepts.Table


@dataclasses.dataclass
class DestroyedTable:
    table_id: concepts.TableId


@dataclasses.dataclass
class TableNameChange:
    table_id: concepts.TableId
    table_name: str

@dataclasses.dataclass
class ImportedTable:
    table_id: concepts.TableId

Change = typing.Union[
    CellChange,
    NewRow,
    DestroyedRow,
    NewField,
    DestroyedField,
    FieldTypeChange,
    FieldNameChange,
    NewTable,
    DestroyedTable,
    TableNameChange,
    ImportedTable
]


@dataclasses.dataclass
class ChangeContext:
    id: concepts.ChangeId
    replication: env_types.Replication
