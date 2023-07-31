import functools
import logging
import logging.config

from src.initial_sync import schema_syncer, view_syncer


class InitialSyncer:

    @functools.cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger('Initial Syncer')

    def sync(self):
        # Drop views now because they depend on tables
        self.logger.info('Starting initial sync')
        schema_syncer.SchemaSyncer().sync()
        view_syncer.ViewSyncer().sync()
        self.logger.info('Finished initial sync')
