import functools
import logging

from src.core import change_handler
from src.core.clients import postgres, airtable
from src.core.types import changes, concepts


class RowSyncer:

    def __init__(self, table: concepts.Table):
        self.table = table

    @functools.cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger(f'Row Syncer: {self.table.id}')

    @functools.cache
    def _get_pg_rows(self) -> dict[concepts.RowId, concepts.Row]:
        return {row.id: row for row in postgres.Client().get_rows(table=self.table)}

    @functools.cache
    def _get_airtable_rows(self) -> dict[concepts.RowId, concepts.Row]:
        return {row.id: row for row in airtable.Client().get_rows(table=self.table)}

    def _get_destroyed_row_changes(self) -> list[changes.DestroyedRow]:
        extra_row_ids = set(self._get_pg_rows().keys()) - set(self._get_airtable_rows().keys())

        if extra_row_ids:
            self.logger.info(f'Found {len(extra_row_ids)} rows that need to be destroyed')

        return [changes.DestroyedRow(table_id=self.table.id, row_id=row_id) for row_id in extra_row_ids]

    def _get_new_row_changes(self) -> list[changes.NewRow]:
        missing_row_ids = set(self._get_airtable_rows().keys()) - set(self._get_pg_rows().keys())

        if missing_row_ids:
            self.logger.info(f'Found {len(missing_row_ids)} rows that need to be created')

        return [
            changes.NewRow(table_id=self.table.id, row=self._get_airtable_rows()[row_id])
            for row_id in missing_row_ids
        ]

    def _update_changed_values(self) -> list[changes.CellChange]:
        cell_changes = []

        for row_id, airtable_row in self._get_airtable_rows().items():

            if row_id in self._get_pg_rows():

                pg_row = self._get_pg_rows()[row_id]
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
        handler = change_handler.Handler()

        for change in [
            *self._get_destroyed_row_changes(),
            *self._get_new_row_changes(),
            *self._update_changed_values()
        ]:
            handler.handle_change(change)
