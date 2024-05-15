import functools
import logging

from ..core import change_handler
from ..core.types import bridges


class PerpetualSyncer:

    def __init__(self, queue: bridges.Queue):
        self.queue = queue

    @functools.cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger('Perpetual Syncer')

    def start(self):

        for change_context, change in self.queue:
            change_handler.Handler(replication=change_context.replication).handle_change(change)
