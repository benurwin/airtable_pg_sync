import functools
import logging

from . import individual_view_syncer
from ..core.clients import postgres
from ..core.types import concepts, env_types


class ViewSyncer:

    def __init__(self, replication: env_types.Replication):
        self.replication = replication

    @functools.cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger('Schema Syncer')

    @functools.cached_property
    def pg_schema(self) -> list[concepts.Table]:
        self.logger.debug('Getting schema from Postgres')

        return postgres.Client(self.replication.schema_name).get_schema()

    def sync(self) -> None:
        for table in self.pg_schema:
            individual_view_syncer.IndividualViewSyncer(self.replication, table).sync()
