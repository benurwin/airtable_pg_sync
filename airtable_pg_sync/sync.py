import functools
import logging
import logging.config
import threading

from .core import env
from .core.types import bridges
from .initial_sync import initial_syncer
from .perpetual_sync import perpetual_syncer, webhook_listener


class Sync:

    def __init__(self, config_path: str, perpetual: bool = True):
        env.load_env(config_path)
        self.perpetual = perpetual
        self.queue = bridges.Queue()

    @functools.cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger('Sync')

    def start_tracking_changes(self):
        self.logger.info('Starting to track changes through webhook listener')
        listener = webhook_listener.WebhookListener(queue=self.queue)
        listener_thread = threading.Thread(target=listener.start, args=())
        listener_thread.start()

    def perform_initial_sync(self):
        self.logger.info('Starting initial sync')
        initial_syncer.InitialSyncer().sync()
        self.logger.info('Finished initial sync')

    def run(self):
        if self.perpetual:
            self.start_tracking_changes()

        self.perform_initial_sync()

        if self.perpetual:
            self.logger.info('Perpetually syncing changes...')
            perpetual_syncer.PerpetualSyncer(queue=self.queue).start()
