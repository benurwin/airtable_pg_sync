import functools
import logging
import logging.config

from . import schema_syncer, view_syncer
from ..core.types import env_types


class InitialSyncer:

    def __init__(self, replication: env_types.Replication):
        self.replication = replication

    @functools.cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger('Initial Syncer')

    def sync(self):
        # Drop views now because they depend on tables
        self.logger.info(f'Starting initial sync {self.replication.base_id} -> {self.replication.schema_name}')
        schema_syncer.SchemaSyncer(self.replication).sync()
        view_syncer.ViewSyncer(self.replication).sync()
        self.logger.info(f'Finished initial sync {self.replication.base_id} -> {self.replication.schema_name}')
