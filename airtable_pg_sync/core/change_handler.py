import functools
import logging

from .clients import postgres, airtable
from .types import changes, env_types
from ..initial_sync import table_syncer, individual_view_syncer, row_syncer


class Handler:

    def __init__(self, replication: env_types.Replication):
        self.replication = replication

    @functools.cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger('Change Handler')

    @functools.singledispatchmethod
    def handle_change(self, change):
        raise NotImplementedError(f'Unknown change type type: {type(change)}')

    @handle_change.register
    def _handle_new_table(self, change: changes.NewTable):
        self.logger.info(f'Creating new table {change.table.id}')
        postgres.Client(self.replication.schema_name).create_table(table=change.table)
        individual_view_syncer.IndividualViewSyncer(self.replication, change.table.id).sync()


    @handle_change.register
    def _handle_imported_table(self, change: changes.ImportedTable):
        self.logger.info(f'Importing table {change.table_id}')
        airtable_table = next(
            (table for table in airtable.Client(self.replication.base_id).get_schema()
                if table.id == change.table_id),
            None
        )

        try:
            postgres.Client(self.replication.schema_name).create_table(table=airtable_table)

        except Exception as e:
            self.logger.warning(f'Failed to create table')
            self.logger.warning(e)

        row_syncer.RowSyncer(replication=self.replication, table=airtable_table).sync()
        individual_view_syncer.IndividualViewSyncer(self.replication, change.table_id).sync()

    @handle_change.register
    def _handle_destroyed_table(self, change: changes.DestroyedTable):
        self.logger.info(f'Handling destroyed table {change.table_id}')
        postgres.Client(self.replication.schema_name).drop_table(table_id=change.table_id)

    @handle_change.register
    def _handle_table_name_change(self, change: changes.TableNameChange):
        self.logger.info(
            f'Updating name of table {change.table_id} to {change.table_name}'
        )
        individual_view_syncer.IndividualViewSyncer(self.replication, change.table_id).sync()

    @handle_change.register
    def _handle_new_field(self, change: changes.NewField):
        self.logger.info(f'Creating new field {change.field.id} in table {change.table_id}')
        postgres.Client(self.replication.schema_name).create_field(table_id=change.table_id, field=change.field)
        individual_view_syncer.IndividualViewSyncer(self.replication, change.table_id).sync()

    @handle_change.register
    def _handle_destroyed_field(self, change: changes.DestroyedField):
        self.logger.info(f'Destroying field {change.field_id} in table {change.table_id}')
        postgres.Client(self.replication.schema_name).drop_field(table_id=change.table_id, field_id=change.field_id)
        individual_view_syncer.IndividualViewSyncer(self.replication, change.table_id).sync()

    @handle_change.register
    def _handle_field_type_change(self, change: changes.FieldTypeChange):
        self.logger.info(f'Changing field type of {change.field_id} to {change.field_type} in table {change.table_id}')
        individual_view_syncer.IndividualViewSyncer(self.replication, change.table_id).drop_view()

        try:
            postgres.Client(self.replication.schema_name).change_field_type(
                table_id=change.table_id,
                field_id=change.field_id,
                new_type=change.field_type
            )

        except Exception as e:
            self.logger.error(e)
            self.logger.error(
                f'Failed to change field type of {change.field_id} to {change.field_type} in table {change.table_id}')
            self.logger.error('Dropping column and re-syncing table')
            postgres.Client(self.replication.schema_name).drop_field(table_id=change.table_id, field_id=change.field_id)
            table_syncer.TableSyncer(
                replication=self.replication,
                airtable_table=next((
                    table for table in airtable.Client(self.replication.base_id).get_schema()
                    if table.id == change.table_id
                )),
                pg_table=next((
                    table for table in postgres.Client(self.replication.schema_name).get_schema()
                    if table.id == change.table_id
                ))
            ).sync()

        individual_view_syncer.IndividualViewSyncer(self.replication, change.table_id).sync()

    @handle_change.register
    def _handle_field_name_change(self, change: changes.FieldNameChange):
        self.logger.info(
            f'Updating name of column {change.field_id} in table {change.table_id} to {change.field_name}'
        )
        individual_view_syncer.IndividualViewSyncer(self.replication, change.table_id).sync()

    @handle_change.register
    def _handle_destroyed_row_change(self, change: changes.DestroyedRow):
        self.logger.info(f'Destroying row {change.row_id} in table {change.table_id}')
        postgres.Client(self.replication.schema_name).drop_row(table_id=change.table_id, row_id=change.row_id)

    @handle_change.register
    def _handle_new_row_change(self, change: changes.NewRow):
        self.logger.info(f'Creating new row {change.row.id} in table {change.table_id}')
        postgres.Client(self.replication.schema_name).insert_row(table_id=change.table_id, row=change.row)

    @handle_change.register
    def _handle_cell_change(self, change: changes.CellChange):
        self.logger.info(
            f'Updating value in row {change.row_id} and column {change.field_id} '
            f'in table {change.table_id} to {change.value}'
        )
        postgres.Client(self.replication.schema_name).update_cell(
            table_id=change.table_id,
            row_id=change.row_id,
            field_id=change.field_id,
            value=change.value
        )
