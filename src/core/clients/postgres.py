import functools
import logging
import typing

import psycopg
from psycopg import sql
from src.core.config import env

from src.core.types import concepts


class Client:
    __connection = None
    CONNECTION_INFO = env.DB_INFO.conn_info
    SCHEMA = env.DB_INFO.schema_name

    @functools.cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger('Postgres Client')

    @classmethod
    def connection(cls) -> psycopg.Connection:
        if not cls.__connection:
            cls.__connection = psycopg.connect(conninfo=cls.CONNECTION_INFO, autocommit=True)

        return cls.__connection

    def _run_query(self, query: sql.Composed, fetch: bool = False) -> list[typing.Tuple] | None:
        self.logger.debug(f'Running query:\n{query.as_string(context=self.connection())}')

        with self.connection().cursor() as cursor:
            cursor.execute(query)

            return cursor.fetchall() if fetch else None

    def create_schema_is_not_exists(self) -> None:
        self.logger.debug('Creating schema if it doesnt exist')
        self._run_query(sql.SQL('CREATE SCHEMA IF NOT EXISTS {schema}').format(schema=sql.Identifier(self.SCHEMA)))

    def get_schema(self) -> list[concepts.Table]:
        self.logger.debug('Getting schema')
        query = sql.SQL('''
                SELECT
                    tables.table_name,
                    array_agg(ARRAY[column_name, data_type]) AS columns
                FROM information_schema.tables
                LEFT JOIN information_schema.columns ON columns.table_name = tables.table_name
                WHERE
                    tables.table_schema = {schema} AND
                    tables.table_type = 'BASE TABLE'
                GROUP BY tables.table_name;
            ''')

        return [
            concepts.Table(
                id=table[0],
                name='',
                fields=[
                    concepts.Field(id=field[0], name='Dummy name (FROM DB)', type=field[1])
                    for field in table[1] if field[0] != 'id' and field != [None, None]
                ]
            ) for table in self._run_query(query.format(schema=self.SCHEMA), fetch=True)
        ]

    def drop_table(self, table_id: concepts.TableId) -> None:
        self.logger.debug(f'Dropping table: {table_id}')
        self._run_query(
            sql.SQL('DROP TABLE {table_path} CASCADE').format(table_path=sql.SQL(f'{self.SCHEMA}."{table_id}"')),
            fetch=False
        )

    def create_table(self, table: concepts.Table) -> None:
        self.logger.debug(f'Creating tables: {table.id}')
        self._run_query(
            sql.SQL('CREATE TABLE {table_path} (id VARCHAR(17) PRIMARY KEY, {table_definition})').format(
                table_path=sql.SQL(f'{self.SCHEMA}."{table.id}"'),
                table_definition=sql.SQL(', ').join(
                    (sql.SQL(f'"{field.id}" {field.type}') for field in table.fields)
                )
            ),
            fetch=False
        )

    def drop_field(self, table_id: concepts.TableId, field_id: concepts.FieldId) -> None:
        self.logger.debug(f'Dropping field: {field_id} from table: {table_id}')
        self._run_query(
            sql.SQL('ALTER TABLE {table_path} DROP COLUMN {field_path} CASCADE').format(
                table_path=sql.SQL(f'{self.SCHEMA}."{table_id}"'),
                field_path=sql.SQL(f'"{field_id}"')
            ),
        )

    def create_field(self, table_id: concepts.TableId, field: concepts.Field) -> None:
        self.logger.debug(f'Adding field: {field.id} to table: {table_id}')
        self._run_query(
            sql.SQL('ALTER TABLE {table_path} ADD COLUMN {field_path} {field_type}').format(
                table_path=sql.SQL(f'{self.SCHEMA}."{table_id}"'),
                field_path=sql.SQL(f'"{field.id}"'),
                field_type=sql.SQL(field.type)
            ),
            fetch=False
        )

    def change_field_type(self, table_id: concepts.TableId, field_id: concepts.FieldId, new_type: str) -> None:
        self.logger.debug(f'Changing field: {field_id} type to: {new_type} in table: {table_id}')
        self._run_query(
            sql.SQL(
                'ALTER TABLE {table_path} ALTER COLUMN {field_path} TYPE {new_type} USING {field_path}::{new_type}').format(
                table_path=sql.SQL(f'{self.SCHEMA}."{table_id}"'),
                field_path=sql.SQL(f'"{field_id}"'),
                new_type=sql.SQL(new_type)
            ),
            fetch=False
        )

    def get_rows(self, table: concepts.Table) -> list[concepts.Row]:
        self.logger.debug(f'Getting rows from table: {table.id}')
        query = sql.SQL('SELECT id, {columns} FROM {table_path}').format(
            table_path=sql.SQL(f'{self.SCHEMA}."{table.id}"'),
            columns=sql.SQL(', ').join((sql.SQL(f'"{field.id}"') for field in table.fields))
        )

        return [
            concepts.Row(
                id=row[0],
                field_values=[
                    concepts.FieldValue(field=field, value=value) for field, value in zip(table.fields, row[1:])
                ]
            ) for row in self._run_query(query, fetch=True)
        ]

    def drop_row(self, table_id: concepts.TableId, row_id: concepts.RowId) -> None:
        self.logger.debug(f'Dropping row: {row_id} from table: {table_id}')
        self._run_query(
            sql.SQL('DELETE FROM {table_path} WHERE id = {row_id}').format(
                table_path=sql.SQL(f'{self.SCHEMA}."{table_id}"'),
                row_id=sql.Literal(row_id)
            ),
            fetch=False
        )

    def insert_row(self, table_id: concepts.TableId, row: concepts.Row) -> None:
        self.logger.debug(f'Inserting row: {row.id} to table: {table_id}')

        if row.field_values:
            self._run_query(
                sql.SQL('INSERT INTO {table_path} (id, {field_columns}) VALUES ({id_value}, {field_values})').format(
                    table_path=sql.SQL(f'{self.SCHEMA}."{table_id}"'),
                    field_columns=sql.SQL(', ').join(
                        (sql.SQL(f'"{field_value.field.id}"') for field_value in row.field_values)
                    ),
                    id_value=sql.Literal(row.id),
                    field_values=sql.SQL(', ').join(
                        (sql.Literal(field_value.value) for field_value in row.field_values)
                    )
                ),
                fetch=False
            )
            return

        self._run_query(
            sql.SQL('INSERT INTO {table_path} (id) VALUES ({id_value})').format(
                table_path=sql.SQL(f'{self.SCHEMA}."{table_id}"'),
                id_value=sql.Literal(row.id)
            ),
            fetch=False
        )

    def update_cell(
            self,
            table_id: concepts.TableId,
            row_id: concepts.RowId,
            field_id: concepts.FieldId,
            value: str
    ) -> None:
        self._run_query(
            sql.SQL('UPDATE {table_path} SET {field_path} = {value} WHERE id = {row_id}').format(
                table_path=sql.SQL(f'{self.SCHEMA}."{table_id}"'),
                field_path=sql.SQL(f'"{field_id}"'),
                value=sql.Literal(value),
                row_id=sql.Literal(row_id)
            ),
            fetch=False
        )

    def drop_view(self, view_name: str):
        self._run_query(
            sql.SQL('DROP VIEW IF EXISTS {table_path} CASCADE').format(
                table_path=sql.SQL(f'{self.SCHEMA}."{view_name}"'),
            ),
            fetch=False
        )

    def get_all_views(self):
        return [x[0] for x in self._run_query(
            sql.SQL("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = {schema} AND table_type = 'VIEW'
            """).format(
                schema=sql.Literal(f'{self.SCHEMA}'),
            ),
            fetch=True
        )]

    def create_view(self, table: concepts.Table):
        self.logger.debug(f'Creating view for table: {table.id}')
        self._run_query(
            sql.SQL('CREATE VIEW {view_path} AS SELECT {columns_and_names} FROM {table_path}').format(
                view_path=sql.SQL(f'{self.SCHEMA}."{table.name}"'),
                table_path=sql.SQL(f'{self.SCHEMA}."{table.id}"'),
                columns_and_names=sql.SQL(', ').join(
                    (sql.SQL(f'"{field.id}" AS "{field.name}"') for field in table.fields)
                )
            ),
            fetch=False
        )
