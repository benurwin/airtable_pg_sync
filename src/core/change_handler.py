import functools
import logging

from src.core.clients import postgres, airtable
from src.core.types import changes
from src.initial_sync import table_syncer, view_syncer


class Handler:

    @functools.cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger('Change Handler')

    @functools.singledispatchmethod
    def handle_change(self, change):
        raise NotImplementedError(f'Unknown change type type: {type(change)}')

    @handle_change.register
    def _handle_new_table(self, change: changes.NewTable):
        self.logger.info(f'Creating new table {change.table.id}')
        postgres.Client().create_table(table=change.table)
        view_syncer.ViewSyncer().sync()

    @handle_change.register
    def _handle_destroyed_table(self, change: changes.DestroyedTable):
        self.logger.info(f'Handling destroyed table {change.table_id}')
        postgres.Client().drop_table(table_id=change.table_id)
        view_syncer.ViewSyncer().sync()

    @handle_change.register
    def _handle_table_name_change(self, change: changes.TableNameChange):
        self.logger.info(
            f'Updating name of table {change.table_id} to {change.table_name}'
        )
        view_syncer.ViewSyncer().sync()

    @handle_change.register
    def _handle_new_field(self, change: changes.NewField):
        self.logger.info(f'Creating new field {change.field.id} in table {change.table_id}')
        postgres.Client().create_field(table_id=change.table_id, field=change.field)
        view_syncer.ViewSyncer().sync()

    @handle_change.register
    def _handle_destroyed_field(self, change: changes.DestroyedField):
        self.logger.info(f'Destroying field {change.field_id} in table {change.table_id}')
        postgres.Client().drop_field(table_id=change.table_id, field_id=change.field_id)
        view_syncer.ViewSyncer().sync()

    @handle_change.register
    def _handle_field_type_change(self, change: changes.FieldTypeChange):
        self.logger.info(f'Changing field type of {change.field_id} to {change.field_type} in table {change.table_id}')
        view_syncer.ViewSyncer().drop_views()
        try:
            postgres.Client().change_field_type(
                table_id=change.table_id,
                field_id=change.field_id,
                new_type=change.field_type
            )

        except Exception as e:
            self.logger.error(e)
            self.logger.error(
                f'Failed to change field type of {change.field_id} to {change.field_type} in table {change.table_id}')
            self.logger.error('Dropping column and re-syncing table')
            postgres.Client().drop_field(table_id=change.table_id, field_id=change.field_id)
            table_syncer.TableSyncer(
                airtable_table=next((table for table in airtable.Client().get_schema() if table.id == change.table_id)),
                pg_table=next((table for table in postgres.Client().get_schema() if table.id == change.table_id))
            ).sync()

        view_syncer.ViewSyncer().sync()

    @handle_change.register
    def _handle_field_name_change(self, change: changes.FieldNameChange):
        self.logger.info(
            f'Updating name of column {change.field_id} in table {change.table_id} to {change.field_name}'
        )
        view_syncer.ViewSyncer().sync()

    @handle_change.register
    def _handle_destroyed_row_change(self, change: changes.DestroyedRow):
        self.logger.info(f'Destroying row {change.row_id} in table {change.table_id}')
        postgres.Client().drop_row(table_id=change.table_id, row_id=change.row_id)

    @handle_change.register
    def _handle_new_row_change(self, change: changes.NewRow):
        self.logger.info(f'Creating new row {change.row.id} in table {change.table_id}')
        postgres.Client().insert_row(table_id=change.table_id, row=change.row)

    @handle_change.register
    def _handle_cell_change(self, change: changes.CellChange):
        self.logger.info(
            f'Updating value in row {change.row_id} and column {change.field_id} '
            f'in table {change.table_id} to {change.value}'
        )
        postgres.Client().update_cell(
            table_id=change.table_id,
            row_id=change.row_id,
            field_id=change.field_id,
            value=change.value
        )
