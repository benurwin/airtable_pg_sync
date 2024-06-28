import _thread
import asyncio
import functools
import json
import logging
import logging.config
import threading
import time

from aiohttp import web

from ..core import env
from ..core.clients import airtable
from ..core.types import bridges, env_types, concepts


class WebhookListener:
    __session = None

    def __init__(self, queue: bridges.Queue):
        self.env = env.value
        self.queue = queue

    @functools.cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger('Webhook Listener')

    async def handle_event(self, request, replication: env_types.Replication) -> web.Response:
        self.logger.info('Received webhook event - adding to queue')
        res = json.loads(await request.text())
        self.logger.info(res)
        self.queue.add(id=res['webhook']['id'], replication=replication)

        return web.Response(text="Nice Webhook")

    async def health_check(self, _) -> web.Response:
        self.logger.info('Received health check - responded with 200')

        return web.Response(text="I'm alive!")

    def remove_webhooks(self):
        self.logger.info('Removing webhooks')

        for replication in self.env.replications:

            for id, url in airtable.Client(replication.base_id).list_webhooks():

                if url.startswith(self.env.webhook_url):
                    self.logger.info(f'Removing webhook for {replication.endpoint}')
                    airtable.Client(replication.base_id).delete_webhook(id)

    @functools.cache
    def set_up_webhooks(self) -> list[tuple[env_types.Replication, concepts.WebhookId]]:
        self.logger.info('Setting up webhooks')
        ids = []

        for replication in self.env.replications:
            self.logger.info(f'Setting up webhook for {replication.endpoint}')
            ids.append((
                replication,
                airtable.Client(replication.base_id).setup_webhook(replication)
            ))

        return ids

    def get_endpoints(self):
        self.logger.info('Getting webhook endpoints')
        health_check_endpoint = [web.get('/', self.health_check)]
        webhook_endpoints = [
            web.post(
                f'/{replication.endpoint}',
                functools.partial(self.handle_event, replication=replication)
            )
            for replication in self.env.replications
        ]

        return health_check_endpoint + webhook_endpoints

    def keep_webhooks_alive(self):

        while True:

            try:
                for replication, id in self.set_up_webhooks():
                    self.logger.info(f'Refreshing webhook for {replication.endpoint}')
                    airtable.Client(replication.base_id).refresh_webhook(id)

            except Exception as e:
                self.logger.error(e)
                self.logger.error('Webhook refresh failed')
                _thread.interrupt_main()

            time.sleep(6 * 24 * 60 * 60)  # refresh every 6th day because it expires after 7 days

    def start(self):
        self.logger.info('Starting webhook listener')

        try:

            self.remove_webhooks()
            self.set_up_webhooks()

            threading.Thread(target=self.keep_webhooks_alive, args=(), daemon=True).start()

            logger = logging.getLogger('Server')
            logger.setLevel(logging.WARNING)
            app = web.Application()
            app.add_routes(self.get_endpoints())
            runner = web.AppRunner(app, access_log=logger)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(runner.setup())
            site = web.TCPSite(runner, '0.0.0.0', self.env.listener_port)
            loop.run_until_complete(site.start())
            loop.run_forever()

        except Exception as e:
            self.logger.error(e)
            self.logger.error('Webhook listener failed')
            _thread.interrupt_main()
