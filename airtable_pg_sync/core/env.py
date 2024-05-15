import yaml

from .types import env_types

value: env_types.Config | None = None


def load_config(path: str):
    global value
    with open(path, 'r') as f:
        raw_yaml = yaml.load(f, Loader=yaml.FullLoader)

    try:
        value = env_types.Config(
            reduced_memory=str(raw_yaml['AIRTABLE_PG_SYNC'].get('REDUCED_MEMORY', '')).upper() == 'TRUE',
            webhook_url=raw_yaml['AIRTABLE_PG_SYNC']['WEBHOOK_URL'],
            listener_port=raw_yaml['AIRTABLE_PG_SYNC']['LISTENER_PORT'],
            airtable_pat=raw_yaml['AIRTABLE_PG_SYNC']['AIRTABLE_PAT'],
            db_host=raw_yaml['AIRTABLE_PG_SYNC']['DB_HOST'],
            db_port=raw_yaml['AIRTABLE_PG_SYNC']['DB_PORT'],
            db_user=raw_yaml['AIRTABLE_PG_SYNC']['DB_USER'],
            db_password=raw_yaml['AIRTABLE_PG_SYNC']['DB_PASSWORD'],
            db_name=raw_yaml['AIRTABLE_PG_SYNC']['DB_NAME'],
            replications=[
                env_types.Replication(
                    base_id=replication['BASE_ID'],
                    schema_name=replication['SCHEMA_NAME'],
                ) for replication in raw_yaml['AIRTABLE_PG_SYNC']['REPLICATIONS'].values()
            ]
        )

    except KeyError as e:
        raise ValueError(f'Could not find {e} in config.yaml') from e
