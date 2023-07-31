import functools
import json
import logging
import typing

import requests
from src.core.config import env

from src.core.clients import response_parser
from src.core.types import changes, concepts


class Client:
    __session = None
    PAT = env.AIRTABLE_INFO.pat
    BASE = env.AIRTABLE_INFO.base_id
    API_URL = 'https://api.airtable.com/v0'

    @functools.cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger('Airtable Client')

    @classmethod
    def session(cls):
        if not cls.__session:
            cls.__session = requests.session()

        return cls.__session

    def _fetch(self, url_extension: str, params: dict = None) -> requests.Response:
        self.logger.debug(f'Fetching {url_extension} with params {params}')
        try:
            response = self.session().get(
                url=f'{self.API_URL}/{url_extension}',
                headers={'Authorization': f'Bearer {self.PAT}'},
                params=params
            )
            self.logger.debug(f'Got response with code: {response.status_code}')

            return response

        except Exception as e:
            self.logger.exception(e)
            raise

    def get_schema(self) -> list[concepts.Table]:
        self.logger.debug('Getting schema')
        response = self._fetch(f'meta/bases/{self.BASE}/tables')

        return response_parser.ResponseParser().parse_list_of_tables(response.json()['tables'])

    def _get_row_chunk(
            self,
            table: concepts.Table,
            offset: str = None
    ) -> tuple[typing.Optional[str], list[concepts.Row]]:
        self.logger.debug(f'Getting row chunk for table {table.id} with offset {offset}')
        response = self._fetch(f'{self.BASE}/{table.id}', params={'offset': offset or ''})

        return response.json().get('offset'), response_parser.ResponseParser().parse_list_of_rows(table,
                                                                                                  response.json())

    def get_rows(self, table: concepts.Table) -> list[concepts.Row]:
        self.logger.debug(f'Getting rows for table {table.id}')
        offset, out = self._get_row_chunk(table, None)

        while offset:
            offset, chunk = self._get_row_chunk(table, offset)
            out.extend(chunk)

        return out

    def list_webhooks(self) -> list[concepts.WebhookId]:
        self.logger.debug('Listing webhooks')
        response = self._fetch(f'bases/{self.BASE}/webhooks')

        return [x['id'] for x in response.json()['webhooks']]

    def delete_webhook(self, webhook_id: concepts.WebhookId):
        self.session().delete(
            url=f'{self.API_URL}/bases/{self.BASE}/webhooks/{webhook_id}',
            headers={'Authorization': f'Bearer {self.PAT}'}
        )

    def setup_webhook(self):
        response = self.session().post(
            url=f'{self.API_URL}/bases/{self.BASE}/webhooks',
            headers={'Authorization': f'Bearer {self.PAT}', 'Content-Type': 'application/json'},
            data=json.dumps({
                'notificationUrl': env.LISTENER_INFO.webhook_url,
                'specification': {
                    'options': {
                        'filters': {
                            'dataTypes': [
                                'tableData',
                                'tableFields',
                                'tableMetadata'
                            ],
                        }
                    }
                }
            })
        )
        response.raise_for_status()

    def get_changes(
            self,
            cursor: concepts.WebhookCursor | None,
            webhook_id: concepts.WebhookId
    ) -> tuple[list[changes.Change], concepts.WebhookCursor]:
        response = self._fetch(f'bases/{self.BASE}/webhooks/{webhook_id}/payloads', params={'cursor': cursor})
        response = json.loads(response.text)

        changes = []

        for payload in response['payloads']:
            changes.extend(response_parser.ResponseParser().parse_webhook_payload(payload))

        return changes, response['cursor']
