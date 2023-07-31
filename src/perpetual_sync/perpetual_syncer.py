import functools
import logging
import time

from src.core import change_handler
from src.core.clients import airtable
from src.core.types import bridges


class PerpetualSyncer:

    def __init__(self, queue: bridges.Queue):
        self.queue = queue

    @functools.cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger('Perpetual Syncer')

    def start(self):
        webhook_cursor = None
        handler = change_handler.Handler()
        iterations_from_change = 0

        while True:

            if self.queue.empty:

                if iterations_from_change == 100:
                    self.logger.info('No Changes detected in the last 25s')
                    iterations_from_change = 0

                iterations_from_change += 1
                time.sleep(0.25)
                continue

            iterations_from_change = 0
            received_changes, webhook_cursor = airtable.Client().get_changes(
                cursor=webhook_cursor,
                webhook_id=self.queue.pop()
            )
            for change in received_changes:
                handler.handle_change(change)
