import yaml

from .types import env_types

DB_INFO = None

AIRTABLE_INFO = None

LISTENER_INFO = None


def get_env_attribute(env: dict, relative_path: str, full_path: str = None):
    if full_path is None:
        full_path = relative_path

    if '.' not in relative_path:

        if relative_path not in env:
            raise ValueError(f'Could not find {full_path} in config.yaml')

        return env[relative_path]

    outer_attribute, *inner_attributes = relative_path.split('.')

    if outer_attribute not in env:
        raise ValueError(f'Could not find {full_path} in config.yaml')

    return get_env_attribute(env[outer_attribute], '.'.join(inner_attributes), full_path)


def load_env(path: str):
    with open(path, 'r') as f:
        env = yaml.load(f, Loader=yaml.FullLoader)

    env = get_env_attribute(env, 'AIRTABLE_PG_SYNC')

    global DB_INFO
    DB_INFO = env_types.PgInfo(
        host=get_env_attribute(env, 'DB_INFO.HOST'),
        port=get_env_attribute(env, 'DB_INFO.PORT'),
        user=get_env_attribute(env, 'DB_INFO.USER'),
        password=get_env_attribute(env, 'DB_INFO.PASSWORD'),
        db_name=get_env_attribute(env, 'DB_INFO.DB_NAME'),
        schema_name=get_env_attribute(env, 'DB_INFO.SCHEMA_NAME'),
    )

    global AIRTABLE_INFO
    AIRTABLE_INFO = env_types.AirtableInfo(
        base_id=get_env_attribute(env, 'AIRTABLE_INFO.BASE_ID'),
        pat=get_env_attribute(env, 'AIRTABLE_INFO.PAT'),
    )

    global LISTENER_INFO
    LISTENER_INFO = env_types.ListenerInfo(
        webhook_url=get_env_attribute(env, 'LISTENER_INFO.WEBHOOK_URL'),
        port=get_env_attribute(env, 'LISTENER_INFO.PORT'),
    )
