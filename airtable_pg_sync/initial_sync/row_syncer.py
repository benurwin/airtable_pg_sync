import functools
import logging

from ..core import change_handler
from ..core.clients import postgres, airtable
from ..core.types import changes, concepts, env_types


class RowSyncer:

    def __init__(self, replication: env_types.Replication, table: concepts.Table):
        self.replication = replication
        self.table = table

    @functools.cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger(f'Row Syncer: {self.table.id}')

    @functools.cached_property
    def pg_rows(self) -> dict[concepts.RowId, concepts.Row]:
        return {row.id: row for row in postgres.Client(self.replication.schema_name).get_rows(table=self.table)}

    @functools.cached_property
    def airtable_rows(self) -> dict[concepts.RowId, concepts.Row]:
        return {row.id: row for row in airtable.Client(self.replication.base_id).get_rows(table=self.table)}

    def _get_destroyed_row_changes(self) -> list[changes.DestroyedRow]:
        extra_row_ids = set(self.pg_rows.keys()) - set(self.airtable_rows.keys())

        if extra_row_ids:
            self.logger.info(f'Found {len(extra_row_ids)} rows that need to be destroyed')

        return [changes.DestroyedRow(table_id=self.table.id, row_id=row_id) for row_id in extra_row_ids]

    def _get_new_row_changes(self) -> list[changes.NewRow]:
        missing_row_ids = set(self.airtable_rows.keys()) - set(self.pg_rows.keys())

        if missing_row_ids:
            self.logger.info(f'Found {len(missing_row_ids)} rows that need to be created')

        return [
            changes.NewRow(table_id=self.table.id, row=self.airtable_rows[row_id])
            for row_id in missing_row_ids
        ]

    def _update_changed_values(self) -> list[changes.CellChange]:
        cell_changes = []

        for row_id, airtable_row in self.airtable_rows.items():

            if row_id in self.pg_rows:

                pg_row = self.pg_rows[row_id]
                pg_field_values = {field_value.field.id: field_value for field_value in pg_row.field_values}
                airtable_field_values = {field_value.field.id: field_value for field_value in
                                         airtable_row.field_values}

                for field_id in pg_field_values:

                    # Case when null in airtable
                    if field_id not in airtable_field_values:

                        if pg_field_values[field_id].value is not None:
                            cell_changes.append(changes.CellChange(
                                table_id=self.table.id,
                                row_id=pg_row.id,
                                field_id=field_id,
                                value=None
                            ))

                    elif pg_field_values[field_id].value != airtable_field_values[field_id].value:
                        cell_changes.append(changes.CellChange(
                            table_id=self.table.id,
                            row_id=pg_row.id,
                            field_id=field_id,
                            value=airtable_field_values[field_id].value
                        ))
        if cell_changes:
            self.logger.info(f'Found {len(cell_changes)} cells that need to be updated')

        return cell_changes

    def sync(self):
        self.logger.info('Syncing table rows')
        handler = change_handler.Handler(self.replication)

        for change in [
            *self._get_destroyed_row_changes(),
            *self._get_new_row_changes(),
            *self._update_changed_values()
        ]:
            handler.handle_change(change)
