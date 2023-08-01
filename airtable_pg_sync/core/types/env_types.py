import dataclasses


@dataclasses.dataclass
class PgInfo:
    host: str
    port: int
    user: str
    password: str
    db_name: str
    schema_name: str

    def __post_init__(self):
        self.schema_name = self.schema_name.lower().strip()
        self.db_name = self.db_name.lower().strip()
        self.host = self.host.strip()
        self.user = self.user.strip()
        self.password = self.password.strip()

    @property
    def conn_info(self):
        return f'host={self.host} port={self.port} dbname={self.db_name} user={self.user} password={self.password}'


@dataclasses.dataclass
class AirtableInfo:
    base_id: str
    pat: str


@dataclasses.dataclass
class ListenerInfo:
    webhook_url: str
    port: int

    def __post_init__(self):
        self.webhook_url = self.webhook_url.strip('/') + '/'
