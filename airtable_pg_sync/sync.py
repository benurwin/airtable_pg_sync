import functools
import logging
import logging.config
import threading

import pkg_resources

from .core import env
from .core.types import bridges
from .initial_sync import initial_syncer
from .perpetual_sync import perpetual_syncer, webhook_listener


def setup_logging():
    logging.config.fileConfig(pkg_resources.resource_filename(__name__, './logging.conf'),
                              disable_existing_loggers=False)
    root_logger = logging.getLogger()
    root_logger.setLevel("INFO")
    root_logger.info(f'Logger created')

    logging.getLogger('asyncio').setLevel(logging.WARNING)

class Sync:

    def __init__(self, config_path: str, perpetual: bool = True):
        env.load_config(config_path)
        self.perpetual = perpetual
        self.queue = bridges.Queue()

    @functools.cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger('Sync')

    def start_tracking_changes(self):
        self.logger.info('Starting to track changes through webhook listener')
        listener = webhook_listener.WebhookListener(queue=self.queue)
        listener_thread = threading.Thread(target=listener.start, args=(), daemon=True)
        listener_thread.start()

    def perform_initial_sync(self):
        self.logger.info('Starting initial sync')
        for replication in env.value.replications:
            initial_syncer.InitialSyncer(replication).sync()
        self.logger.info('Finished initial sync')

    def run(self):

        if self.perpetual:
            self.start_tracking_changes()

        while True:

            try:
                self.perform_initial_sync()

                if not self.perpetual:
                    break

                perpetual_syncer.PerpetualSyncer(queue=self.queue).start()

            except Exception as e:
                self.logger.exception('Error while syncing changes')
                self.logger.exception(e)

                self.logger.info('Clearing queue')
                self.queue.clear()
                self.logger.info('Re-syncing all tables')
