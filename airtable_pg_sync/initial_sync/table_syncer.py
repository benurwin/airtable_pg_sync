import functools
import logging

from . import reduced_memory_usage_row_syncer, row_syncer
from ..core import change_handler
from ..core import env
from ..core.types import changes, concepts, env_types


class TableSyncer:

    def __init__(self, replication: env_types.Replication, airtable_table: concepts.Table, pg_table: concepts.Table):
        self.replication = replication
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
        if env.value.reduced_memory:
            self.logger.info('Using reduced memory row syncer')
            reduced_memory_usage_row_syncer.RowSyncer(replication=self.replication, table=self.airtable_table).sync()

        else:
            row_syncer.RowSyncer(replication=self.replication, table=self.airtable_table).sync()

    def sync(self):
        self.logger.info(f'Syncing table - {self.airtable_table.name} ({self.airtable_table.id})')
        handler = change_handler.Handler(self.replication)

        for change in [
            *self._get_new_field_changes(),
            *self._get_destroyed_field_changes(),
            *self._get_field_type_changes()
        ]:
            handler.handle_change(change)

        self._sync_rows()
