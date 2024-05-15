import functools
import itertools
import logging

from ..core import change_handler
from ..core.clients import airtable, postgres
from ..core.types import concepts, env_types, changes


class RowSyncer:

    def __init__(self, replication: env_types.Replication, table: concepts.Table):
        self.replication = replication
        self.table = table

    @functools.cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger(f'Row Syncer: {self.table.id}')

    @functools.cached_property
    def _handler(self) -> change_handler.Handler:
        return change_handler.Handler(
            replication=self.replication,
        )

    def _remove_extra_rows(self) -> None:
        pg_row_id_chunks = postgres.Client(self.replication.schema_name).get_row_id_chunks(table=self.table)

        for chunk in pg_row_id_chunks:
            matched_airtable_ids = airtable.Client(self.replication.base_id).get_matching_ids(
                table=self.table,
                row_ids=chunk
            )
            extra_row_ids = set(chunk) - set(matched_airtable_ids)

            if extra_row_ids:
                self.logger.info(f'Found {len(extra_row_ids)} rows that need to be destroyed')

            for row_id in extra_row_ids:
                self._handler.handle_change(
                    changes.DestroyedRow(
                        table_id=self.table.id,
                        row_id=row_id
                    )
                )

    def _get_cell_changes(self, pg_row: concepts.Row, airtable_row: concepts.Row) -> list[changes.CellChange]:
        pg_field_values = {field_value.field.id: field_value for field_value in pg_row.field_values}
        airtable_field_values = {field_value.field.id: field_value for field_value in airtable_row.field_values}

        cell_changes = []

        for field_id in pg_field_values:

            # Case when null in airtable
            if field_id not in airtable_field_values:

                if pg_field_values[field_id].value is not None:
                    cell_changes.append(
                        changes.CellChange(
                            table_id=self.table.id,
                            row_id=pg_row.id,
                            field_id=field_id,
                            value=None
                        )
                    )

            # Case when values are different
            elif pg_field_values[field_id].value != airtable_field_values[field_id].value:
                cell_changes.append(
                    changes.CellChange(
                        table_id=self.table.id,
                        row_id=pg_row.id,
                        field_id=field_id,
                        value=airtable_field_values[field_id].value
                    )
                )

        return cell_changes

    def _create_missing_rows_and_update_cell_values(self) -> None:
        airtable_rows = airtable.Client(self.replication.base_id).get_rows(table=self.table)

        chunk = {row.id: row for row in list(itertools.islice(airtable_rows, 100))}

        while chunk:
            pg_rows = postgres.Client(self.replication.schema_name).get_rows(
                table=self.table,
                id_filter=list(chunk.keys())
            )
            # Add missing rows
            missing_row_ids = {id for id in chunk} - {row.id for row in pg_rows}

            if missing_row_ids:
                self.logger.info(f'Found {len(missing_row_ids)} rows that need to be created')

            for row_id in missing_row_ids:
                self._handler.handle_change(
                    changes.NewRow(
                        table_id=self.table.id,
                        row=chunk[row_id]
                    )
                )

            # Update cell values
            for pg_row in pg_rows:
                airtable_row = chunk[pg_row.id]
                cell_changes = self._get_cell_changes(pg_row=pg_row, airtable_row=airtable_row)

                if cell_changes:
                    self.logger.info(f'Found {len(cell_changes)} cell changes that need to be applied on row {pg_row.id}')

                for change in cell_changes:
                    self._handler.handle_change(change)

            chunk = {row.id: row for row in list(itertools.islice(airtable_rows, 100))}

    def sync(self) -> None:
        self.logger.info('Starting sync')

        self.logger.info('Removing extra rows')
        self._remove_extra_rows()

        self.logger.info('Creating missing rows and updating cell values')
        self._create_missing_rows_and_update_cell_values()
