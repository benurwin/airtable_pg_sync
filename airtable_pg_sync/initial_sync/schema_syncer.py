import functools
import logging

from . import table_syncer
from ..core import change_handler
from ..core.clients import postgres, airtable
from ..core.types import changes, concepts, env_types


class SchemaSyncer:

    def __init__(self, replication: env_types.Replication):
        self.replication = replication

    @functools.cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger('Schema Syncer')

    @functools.cached_property
    def _get_airtable_schema(self) -> dict[concepts.TableId, concepts.Table]:
        self.logger.debug('Getting schema from Airtable')

        return {table.id: table for table in airtable.Client(self.replication.base_id).get_schema()}

    @functools.cached_property
    def _get_pg_schema(self) -> dict[concepts.TableId, concepts.Table]:
        self.logger.debug('Getting schema from Postgres')

        return {table.id: table for table in postgres.Client(self.replication.schema_name).get_schema()}

    def _make_sure_schema_exists(self) -> None:
        self.logger.info('Making sure schema exists')
        postgres.Client(self.replication.schema_name).create_schema_is_not_exists()
        postgres.Client(self.replication.schema_name).create_table_names_table_if_not_exists()

    def _get_destroyed_table_changes(self) -> list[changes.DestroyedTable]:
        extra_table_ids = set(self._get_pg_schema.keys()) - set(self._get_airtable_schema.keys())

        if extra_table_ids:
            self.logger.info(f'Found {len(extra_table_ids)} tables that need to be destroyed')

        return [changes.DestroyedTable(table_id=table_id) for table_id in extra_table_ids]

    def _get_new_table_changes(self) -> list[changes.NewTable]:
        missing_table_ids = set(self._get_airtable_schema.keys()) - set(self._get_pg_schema.keys())

        if missing_table_ids:
            self.logger.info(f'Found {len(missing_table_ids)} tables that need to be created')

        return [changes.NewTable(table=self._get_airtable_schema[table_id]) for table_id in missing_table_ids]

    def _sync_tables(self) -> None:
        self.__dict__.pop('_get_pg_schema', None)
        for table in self._get_airtable_schema.values():
            table_syncer.TableSyncer(
                replication=self.replication,
                airtable_table=table,
                pg_table=self._get_pg_schema[table.id]
            ).sync()

    def sync(self) -> None:
        self.logger.info(f'Syncing schema - {self.replication.base_id} -> {self.replication.schema_name}')
        self._make_sure_schema_exists()
        handler = change_handler.Handler(self.replication)

        for change in [*self._get_new_table_changes(), *self._get_destroyed_table_changes()]:
            handler.handle_change(change)

        self._sync_tables()
