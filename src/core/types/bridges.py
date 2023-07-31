import dataclasses

from src.core.types import concepts


@dataclasses.dataclass
class ChangeHolder:
    changes: list

    def add(self, change):
        self.changes.append(change)

    def pop(self):
        return self.changes.pop()

    def empty(self) -> bool:
        return bool(self.changes)


@dataclasses.dataclass
class Queue:
    values: list[concepts.ChangeId] = dataclasses.field(default_factory=list)

    def add(self, id: concepts.ChangeId):
        self.values.append(id)

    def pop(self) -> concepts.ChangeId:
        return self.values.pop(0)

    @property
    def empty(self) -> bool:
        return len(self.values) == 0

    def __repr__(self):
        return f'<Queue len={len(self.values)}>'
