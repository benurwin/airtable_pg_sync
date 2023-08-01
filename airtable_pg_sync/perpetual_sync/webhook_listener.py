import _thread
import asyncio
import functools
import json
import logging
import logging.config

from aiohttp import web

from ..core import env
from ..core.clients import airtable
from ..core.types import bridges


class WebhookListener:
    __session = None

    def __init__(self, queue: bridges.Queue):
        self.queue = queue

    @functools.cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger('Webhook Listener')

    async def handle_event(self, request):
        self.logger.info('Received webhook event - adding to queue')
        res = json.loads(await request.text())
        self.queue.add(res['webhook']['id'])

        return web.Response(text="Nice Webhook")

    def create_webhook(self):
        self.logger.info('Creating webhook')
        airtable.Client().setup_webhook()

    def start(self):
        self.logger.info('Starting webhook listener')
        # TODO: Remove this before publishing and provide a way to set up / tear down webhooks

        for id in airtable.Client().list_webhooks():
            airtable.Client().delete_webhook(id)

        # TODO: Provide a way to pass in webhooks too and handle error when too many have been created

        try:
            self.create_webhook()

            app = web.Application()
            app.add_routes([web.post('/', self.handle_event)])
            runner = web.AppRunner(app)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(runner.setup())
            site = web.TCPSite(runner, 'localhost', env.LISTENER_INFO.port)
            loop.run_until_complete(site.start())
            loop.run_forever()

        except Exception as e:
            self.logger.error(e)
            self.logger.error('Webhook listener failed')
            _thread.interrupt_main()
