import dataclasses
import datetime
import typing

from dateutil import parser

TYPE_MAPPING = {
    'CHECKBOX': 'BOOLEAN',
    'FORMULA': 'TEXT',
    'URL': 'TEXT',
    'CURRENCY': 'FLOAT',
    'SINGLELINETEXT': 'TEXT',
    'MULTIPLERECORDLINKS': 'TEXT[]',
    'AUTONUMBER': 'FLOAT',
    'DATE': 'DATE',
    'MULTIPLELOOKUPVALUES': 'TEXT[]',
    'MULTILINETEXT': 'TEXT',
    'DOUBLE PRECISION': 'FLOAT',
    'TEXT': 'TEXT',
    'FLOAT': 'FLOAT',
    'TIMESTAMP WITHOUT TIME ZONE': 'TIMESTAMP',
    'ARRAY': 'TEXT[]',
    'BOOLEAN': 'BOOLEAN',
    'SINGLECOLLABORATOR': 'TEXT',
    'SINGLESELECT': 'TEXT',
    'MULTIPLEATTACHMENTS': 'TEXT[]',
    'MULTIPLESELECTS': 'TEXT[]',
    'PHONENUMBER': 'TEXT',
    'EMAIL': 'TEXT',
    'NUMBER': 'FLOAT',
    'PERCENT': 'FLOAT',
    'DURATION': 'TEXT',
    'RATING': 'FLOAT',
    'ROLLUP': 'TEXT',
    'COUNT': 'INTEGER',
    'LOOKUP': 'TEXT',
    'CREATEDTIME': 'TIMESTAMP',
    'LASTMODIFIEDTIME': 'TIMESTAMP',
    'AUTO_NUMBER': 'FLOAT',
    'BARCODE': 'TEXT',
    'BUTTON': 'TEXT',
    'LASTMODIFIEDBY': 'TEXT',
    'CREATEDBY': 'TEXT',
}

DB_TYPES = ['TEXT', 'FLOAT', 'BOOLEAN', 'TIMESTAMP', 'TEXT[]', 'INTEGER']


def parse_timestamp(timestamp: str | datetime.datetime) -> str | None:
    if not timestamp:
        return None

    time_stamp = parser.parse(str(timestamp))

    return time_stamp.replace(tzinfo=None).isoformat(timespec='seconds')


VALUE_PARSER = {
    'TEXT': lambda x: str(x),
    'FLOAT': lambda x: float(x),
    'INTEGER': lambda x: int(x),
    'DATE': lambda x: str(x),
    'TIMESTAMP': parse_timestamp,
}


class UnknownType(Exception):
    pass


FieldId = typing.Annotated[str, 'Field ID']


@dataclasses.dataclass
class Field:
    id: FieldId
    name: str
    type: str

    def __post_init__(self):
        self.id = self.id.strip()

        self.type = self.type.upper().strip()

        if self.type in TYPE_MAPPING:
            # Convert from airtable type to postgres type
            self.type = TYPE_MAPPING[self.type]

        elif self.type in DB_TYPES:
            # Already a postgres type
            pass

        else:
            # Unknown type
            raise UnknownType(f'Unknown type ({self.type}) found for field (id: {self.id}, name: {self.name})')


TableId = typing.Annotated[str, 'Table ID']


@dataclasses.dataclass
class Table:
    id: TableId
    name: str
    fields: list[Field]

    def __post_init__(self):
        self.id = self.id.strip()


@dataclasses.dataclass
class FieldValue:
    field: Field
    value: str

    def __post_init__(self):
        parser = VALUE_PARSER.get(self.field.type)
        self.value = parser(self.value) if parser and self.value is not None else self.value


RowId = typing.Annotated[str, 'Row ID']


@dataclasses.dataclass
class Row:
    id: RowId
    field_values: list[FieldValue]


ChangeId = typing.Annotated[str, 'Change ID']
WebhookId = typing.Annotated[str, 'Webhook ID']
WebhookCursor = typing.Annotated[str, 'Webhook Cursor']
