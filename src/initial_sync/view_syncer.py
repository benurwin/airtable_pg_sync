import functools
import logging

from src.core.clients import postgres, airtable
from src.core.types import concepts


class ViewSyncer:

    @functools.cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger('Schema Syncer')

    @functools.cache
    def _get_airtable_schema(self) -> list[concepts.Table]:
        self.logger.debug('Getting schema from Airtable')

        return airtable.Client().get_schema()

    def drop_views(self):
        self.logger.info('Dropping views')
        for view in postgres.Client().get_all_views():
            self.logger.info(f'Dropping view {view}')
            postgres.Client().drop_view(view_name=view)

    def create_views(self):
        self.logger.info('Creating views')
        for table in self._get_airtable_schema():
            self.logger.info(f'Creating view {table.name}')
            postgres.Client().create_view(table=table)

    def sync(self):
        self.logger.info('Syncing views')
        self.drop_views()
        self.create_views()
