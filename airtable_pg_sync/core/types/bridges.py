import functools
import logging
import time
import typing

from . import concepts, changes, env_types
from ..clients import airtable


class Queue:

    def __init__(self):
        self.values: list[changes.ChangeContext] = []
        self.cursors: dict[env_types.Replication, str] = {}
        self.__generator = None

    @functools.cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger('Queue')

    def add(self, id: concepts.ChangeId, replication: env_types.Replication) -> None:
        self.values.append(changes.ChangeContext(id=id, replication=replication))


    def __get_change_group(self) -> list[tuple[changes.ChangeContext, changes.Change]] | None:

        if self.empty:
            return None

        change_context = self.values.pop()
        airtable_client = airtable.Client(base_id=change_context.replication.base_id)
        received_changes, self.cursors[change_context.replication] = airtable_client.get_changes(
            cursor=self.cursors.get(change_context.replication),
            webhook_id=change_context.id
        )

        return [(change_context, change) for change in received_changes]

    def __get_new_generator(self) -> typing.Generator[tuple[changes.ChangeContext, changes.Change], None, None]:
        iterations_from_change = 0

        while True:

            if self.empty:
                time.sleep(0.25)
                iterations_from_change += 1

                if iterations_from_change % 240 == 0:
                    self.logger.info(
                        f'No changes have been detected in the last {int(iterations_from_change/240)} minutes'
                    )

                continue

            iterations_from_change = 0

            for change_context, change in self.__get_change_group():
                yield change_context, change
    @property
    def generator(self):
        if not self.__generator:
            self.__generator = self.__get_new_generator()

        return self.__generator

    def __next__(self) -> tuple[changes.ChangeContext, changes.Change]:
        return next(self.generator)

    def __iter__(self) -> 'Queue':
        return self

    def clear(self) -> None:
        self.__generator = None

        while not self.empty:
            self.__get_change_group()

    @property
    def empty(self) -> bool:
        return len(self.values) == 0

    def __repr__(self) -> str:
        return f'<Queue len={len(self.values)}>'
