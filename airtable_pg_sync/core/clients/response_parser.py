import functools
import logging

from ..types import changes, concepts


class ResponseParser:

    @functools.cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger('Response Parser')

    @staticmethod
    def parse_list_of_tables(response: list[dict]) -> list[concepts.Table]:
        return [concepts.Table(id=table['id'],
                               name=table['name'],
                               fields=[
                                   concepts.Field(
                                       id=field['id'],
                                       name=field['name'],
                                       type=field['type']
                                   ) for field in table['fields']
                               ]
                               ) for table in response]

    @staticmethod
    def parse_field_value(field_value: str | list | dict) -> str | list:
        if isinstance(field_value, list):
            return [value['name'] for value in field_value]

        if isinstance(field_value, dict):
            return field_value.get('name') or field_value.get('text') or field_value['url']

        return field_value

    def parse_list_of_rows(self, table: concepts.Table, response: dict) -> list[concepts.Row]:
        fields = {field.name: field for field in table.fields}

        return [concepts.Row(
            id=row['id'],
            field_values=[concepts.FieldValue(
                field=fields[name],
                value=self.parse_field_value(value)
            ) for name, value in row['fields'].items()]
        ) for row in response['records']]

    def _parse_changed_records_by_id(self, table_id: concepts.TableId, records: dict) -> list[changes.Change]:
        out = []

        for row_id, values in records.items():
            values = values.get('current', {}).get('cellValuesByFieldId', {})

            for field_id, value in values.items():
                out.append(changes.CellChange(
                    table_id=table_id,
                    row_id=row_id,
                    field_id=field_id,
                    value=self.parse_field_value(value)
                ))

        return out

    @staticmethod
    def _parse_destroyed_filed_id(table_id: str, fields: dict) -> list[changes.Change]:
        return [changes.DestroyedField(table_id=table_id, field_id=field_id) for field_id in fields]

    @staticmethod
    def _parse_created_fields_by_id(table_id: str, fields: dict) -> list[changes.Change]:
        out = []

        for filed_id, field in fields.items():
            out.append(
                changes.NewField(
                    table_id=table_id,
                    field=concepts.Field(
                        id=filed_id,
                        name=field['name'],
                        type=field['type']
                    )
                )
            )

        return out

    def _parse_created_records_by_id(self, table_id: str, rows: dict) -> list[changes.Change]:
        created_rows = [
            changes.NewRow(table_id=table_id, row=concepts.Row(id=row_id, field_values=[]))
            for row_id in rows
        ]
        new_values = []

        for row_id, values in rows.items():
            new_values.extend(self._parse_changed_records_by_id(table_id, {'current': values}))

        return created_rows + new_values

    @staticmethod
    def _parse_deleted_record_ids(table_id: str, rows: list) -> list[changes.Change]:
        return [changes.DestroyedRow(table_id=table_id, row_id=row_id) for row_id in rows]

    @staticmethod
    def _parse_changed_fields_by_id(table_id: str, fields: dict) -> list[changes.Change]:
        type_changes = [
            changes.FieldTypeChange(table_id=table_id, field_id=field_id, field_type=field['current']['type'])
            for field_id, field in fields.items()
            if field['current'].get('type')
        ]

        name_changes = [
            changes.FieldNameChange(table_id=table_id, field_id=field_id, field_name=field['current']['name'])
            for field_id, field in fields.items()
            if field['current'].get('name')
        ]

        return [*type_changes, *name_changes]

    @staticmethod
    def _parse_changed_metadata(table_id: str, metadata: dict) -> list[changes.Change]:
        return [changes.TableNameChange(table_id=table_id, table_name=metadata['current']['name'])]

    def _parse_created_tables_by_id(self, table_id: str, received_changes: dict) -> list[changes.Change]:
        new_table = [
            changes.NewTable(
                table=concepts.Table(
                    id=table_id,
                    name=received_changes['metadata']['name'],
                    fields=[
                        concepts.Field(id=field_id, name=field['name'], type=field['type'])
                        for field_id, field in received_changes['fieldsById'].items()
                    ]
                )
            )
        ]

        new_values = self._parse_created_records_by_id(table_id, received_changes['recordsById'])

        return new_table + new_values

    @staticmethod
    def _parse_destroyed_tables_id(table_id: str) -> list[changes.Change]:
        return [changes.DestroyedTable(table_id=table_id)]

    def _parse_table_change_by_id(self, table_id: str, received_changes: dict) -> list[changes.Change]:
        out = []

        for key, change in received_changes.items():
            if key == 'changedRecordsById':
                out.extend(self._parse_changed_records_by_id(table_id, change))

            elif key == 'destroyedFieldIds':
                out.extend(self._parse_destroyed_filed_id(table_id, change))

            elif key == 'createdFieldsById':
                out.extend(self._parse_created_fields_by_id(table_id, change))

            elif key == 'createdRecordsById':
                out.extend(self._parse_created_records_by_id(table_id, change))

            elif key == 'deletedRecordIds':
                out.extend(self._parse_deleted_record_ids(table_id, change))

            elif key == 'changedFieldsById':
                out.extend(self._parse_changed_fields_by_id(table_id, change))

            elif key == 'changedMetadata':
                out.extend(self._parse_changed_metadata(table_id, change))

            else:
                raise ValueError(f"Unknown change type: {key}")

        return out

    def parse_webhook_payload(self, payload: dict) -> list[changes.Change]:
        out = []

        for table_id, received_changes in payload.get('changedTablesById', {}).items():
            out.extend(self._parse_table_change_by_id(table_id, received_changes))

        for table_id, received_changes in payload.get('createdTablesById', {}).items():
            out.extend(self._parse_created_tables_by_id(table_id, received_changes))

        for table_id in payload.get('destroyedTableIds', []):
            out.extend(self._parse_destroyed_tables_id(table_id))

        if not out:
            self.logger.error('Unknown payload type')
            for k, v in payload.items():
                self.logger.error(f'{k}: {v}')

        return out
