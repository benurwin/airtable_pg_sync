import functools
import logging

from src.core import change_handler
from src.core.types import changes, concepts
from src.initial_sync import row_syncer


class TableSyncer:

    def __init__(self, airtable_table: concepts.Table, pg_table: concepts.Table):
        self.airtable_table = airtable_table
        self.pg_table = pg_table
        self.airtable_fields = {field.id: field for field in self.airtable_table.fields}
        self.pg_fields = {field.id: field for field in self.pg_table.fields}

    @functools.cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger(f'Table Syncer: {self.airtable_table.id}')

    def _get_destroyed_field_changes(self) -> list[changes.DestroyedField]:
        extra_field_ids = set(self.pg_fields.keys()) - set(self.airtable_fields.keys())

        if extra_field_ids:
            self.logger.info(f'Found {len(extra_field_ids)} fields that need to be destroyed')

        return [changes.DestroyedField(table_id=self.pg_table.id, field_id=field_id) for field_id in extra_field_ids]

    def _get_new_field_changes(self) -> list[changes.NewField]:
        missing_field_ids = set(self.airtable_fields.keys()) - set(self.pg_fields.keys())

        if missing_field_ids:
            self.logger.info(f'Found {len(missing_field_ids)} fields that need to be created')

        return [
            changes.NewField(table_id=self.pg_table.id, field=self.airtable_fields[field_id])
            for field_id in missing_field_ids
        ]

    def _get_field_type_changes(self) -> list[changes.FieldTypeChange]:
        changed_fields: list[changes.FieldTypeChange] = []

        for field_id in self.airtable_fields:

            if field_id in self.pg_fields and self.airtable_fields[field_id].type != self.pg_fields[field_id].type:
                changed_fields.append(changes.FieldTypeChange(
                    table_id=self.pg_table.id,
                    field_id=field_id,
                    field_type=self.airtable_fields[field_id].type
                ))

        if changed_fields:
            self.logger.info(f'Found {len(changed_fields)} fields that need their type to be changed')

        return changed_fields

    def _sync_rows(self):
        row_syncer.RowSyncer(table=self.airtable_table).sync()

    def sync(self):
        self.logger.info('Syncing table')
        handler = change_handler.Handler()

        for change in [
            *self._get_new_field_changes(),
            *self._get_destroyed_field_changes(),
            *self._get_field_type_changes()
        ]:
            handler.handle_change(change)

        self._sync_rows()
