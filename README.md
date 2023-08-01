# Airtable Postgres Sync

The goal of this library is to provide an out-of-the-box solution for replicating
an entire Airtable base in a Postgres schema. There are two modes of operation:

- **One-off-sync**: This mode will replicate the Airtable base in the specified Postgres schema
  and then exit. This is useful for creating snapshots of the base for analysis or for storage as a backup.
- **Perpetual sync**: This mode will replicate the Airtable base in the specified Postgres schema
  and then continue to watch for changes in the base. When a change is detected, the
  change will be applied to the Postgres schema. This is useful for creating a
  replica of the base that can be used for analysis in real time.

## Installation

To install the library, run the following command:

```bash
pip install airtable-postgres-sync
```

## Permissions

To use this library, you will need to create a personal access token in Airtable. This
token will need to have the following scopes:

- data.records:read
- schema.bases:read
- webhook:manage

You will also need to give the Postgres user that you are using read and write access to the schema
you are syncing to.

## Usage

To use the library, you will need to create a config file. The config file defines
all the parameters that are needed to connect to Airtable and Postgres, as well as how
your program will listen for changes. The file must be in YAML format and must contain
the following fields:

```yaml
AIRTABLE_PG_SYNC:
  DB_INFO:
    HOST: # Postgres host
    PORT: # Postgres port
    USER: # Postgres user
    PASSWORD: # Postgres password
    DB_NAME: # Postgres database name
    SCHEMA_NAME: # Postgres schema name
  AIRTABLE_INFO:
    BASE_ID: # Airtable base id to sync
    PAT: # Airtable personal access token
  LISTENER_INFO:
    WEBHOOK_URL: # The url that Airtable will send change notifications to
    PORT: # The port to listen for change notifications on
```

The library can be used in two ways:

1. As a command line tool

To trigger a one-time sync, run the following command:

```bash
airtable-pg-sync one-time-sync --config /path/to/config.yml
```

To trigger a perpetual sync, run the following command:

```bash
airtable-pg-sync perpetual-sync --config /path/to/config.yml
```

2. As a python library

To trigger a sync from within a python program, run the following code:

```python
from airtable_pg_sync import Sync

Sync(config_path="/path/to/config.yml", perpetual=True / False).run()
```

## Bugs, Feature Requests, and Contributions

If you find a bug or have a feature request, please open an issue
on [GitHub](https://github.com/benurwin/airtable_pg_sync/issues).
Any contributions are welcome and appreciated. If you would like to
contribute, please open a pull request on [GitHub](https://github.com/benurwin/airtable_pg_sync/pulls).

### Ideas for contributions:

- Add support for other databases
- Add support for Postgres -> Airtable sync

## License

This library is licensed under the MIT License. See the
[LICENSE](https://github.com/benurwin/airtable_pg_sync/blob/main/LICENSE) file
