import functools
import json
import logging
import typing

import requests

from .. import env
from ..clients import response_parser
from ..types import changes, concepts, env_types


class Client:
    __session = None
    API_URL = 'https://api.airtable.com/v0'

    def __init__(self, base_id: str):
        self.pat = env.value.airtable_pat
        self.base = base_id

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
                headers={'Authorization': f'Bearer {self.pat}'},
                params=params
            )
            self.logger.debug(f'Got response with code: {response.status_code}')

            return response

        except Exception as e:
            self.logger.exception(e)
            raise e

    def get_schema(self) -> list[concepts.Table]:
        self.logger.debug('Getting schema')
        response = self._fetch(f'meta/bases/{self.base}/tables')

        return response_parser.ResponseParser().parse_list_of_tables(response.json()['tables'])

    def _get_row_chunk(
            self,
            table: concepts.Table,
            offset: str = None
    ) -> tuple[typing.Optional[str], list[concepts.Row]]:
        self.logger.debug(f'Getting row chunk for table {table.id} with offset {offset}')
        response = self._fetch(f'{self.base}/{table.id}', params={'offset': offset or '', 'pageSize': 100})

        return response.json().get('offset'), response_parser.ResponseParser().parse_list_of_rows(table,
                                                                                                  response.json())

    def get_rows(self, table: concepts.Table) -> typing.Generator[concepts.Row, None, None]:
        self.logger.debug(f'Getting rows for table {table.id}')
        offset = None
        first_loop = True

        while offset or first_loop:

            if first_loop:
                first_loop = False

            offset, chunk = self._get_row_chunk(table, offset)

            for row in chunk:
                yield row

    def get_matching_ids(self, table: concepts.Table, row_ids: list[concepts.RowId]) -> list[concepts.RowId]:
        self.logger.debug(f'Getting rows with ids {row_ids}')

        if len(row_ids) > 100:
            raise ValueError('get_rows_with_matching_ids only works for 100 or fewer ids')

        formula_list = ', '.join([f'RECORD_ID() = \'{row_id}\'' for row_id in row_ids])
        response = self._fetch(
            f'{self.base}/{table.id}',
            params={
                'filterByFormula': f'OR({formula_list})',
                'fields': [table.fields[0].id, table.fields[0].id]
            }
        )
        rows = response_parser.ResponseParser().parse_list_of_rows(table, response.json())

        return [row.id for row in rows]

    def list_webhooks(self) -> list[tuple[concepts.WebhookId, concepts.WebhookUrl]]:
        self.logger.debug('Listing webhooks')
        response = self._fetch(f'bases/{self.base}/webhooks')

        return [(x['id'], x['notificationUrl']) for x in response.json()['webhooks']]

    def delete_webhook(self, webhook_id: concepts.WebhookId):
        self.session().delete(
            url=f'{self.API_URL}/bases/{self.base}/webhooks/{webhook_id}',
            headers={'Authorization': f'Bearer {self.pat}'}
        )

    def setup_webhook(self, replication: env_types.Replication) -> concepts.WebhookId:
        response = self.session().post(
            url=f'{self.API_URL}/bases/{self.base}/webhooks',
            headers={'Authorization': f'Bearer {self.pat}', 'Content-Type': 'application/json'},
            data=json.dumps({
                'notificationUrl': f'{env.value.webhook_url}{replication.endpoint}',
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

        if not response.status_code == 200:
            self.logger.error(f'Error creating webhook: {response.text}')

        response.raise_for_status()

        return response.json()['id']

    def get_changes(
            self,
            cursor: concepts.WebhookCursor | None,
            webhook_id: concepts.WebhookId
    ) -> tuple[list[changes.Change], concepts.WebhookCursor]:
        response = self._fetch(f'bases/{self.base}/webhooks/{webhook_id}/payloads', params={'cursor': cursor})
        response = json.loads(response.text)

        _changes = []

        for payload in response['payloads']:
            _changes.extend(response_parser.ResponseParser().parse_webhook_payload(payload))

        return _changes, response['cursor']

    def refresh_webhook(self, webhook_id: concepts.WebhookId):
        response = self.session().post(
            url=f'{self.API_URL}/bases/{self.base}/webhooks/{webhook_id}/refresh',
            headers={'Authorization': f'Bearer {self.pat}'}
        )
        response.raise_for_status()
