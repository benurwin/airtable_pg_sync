import dataclasses


@dataclasses.dataclass(frozen=True)
class Replication:
    base_id: str
    schema_name: str

    @property
    def endpoint(self) -> str:
        return f'{self.base_id}/{self.schema_name}'


@dataclasses.dataclass
class Config:
    reduced_memory: bool
    webhook_url: str
    listener_port: int
    airtable_pat: str
    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_name: str
    replications: list[Replication]

    def __post_init__(self):
        self.webhook_url = self.webhook_url.strip('/') + '/'

    @property
    def connection_info(self) -> str:
        with_user = f'host={self.db_host} port={self.db_port} dbname={self.db_name} user={self.db_user} ' \
                    f'password={self.db_password}'
        without_user = f'host={self.db_host} port={self.db_port} dbname={self.db_name}'

        return with_user if self.db_user else without_user
