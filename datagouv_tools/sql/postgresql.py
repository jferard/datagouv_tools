#  DataGouv Tools. An utility to import some data from data.gouv.fr to
#                  PostgreSQL and other DBMS.
#        Copyright (C) 2020 J. Férard <https://github.com/jferard>
#
#   This file is part of DataGouv Tools.
#
#  DataGouv Tools is free software: you can redistribute it and/or modify it
#  under the terms of the GNU General Public License as published by the Free
#  Software Foundation, either version 3 of the License, or (at your option)
#  any later version.
#
#  DataGouv Tools is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
#  or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
#  more details.
#  You should have received a copy of the GNU General Public License along with
#  this program. If not, see <http://www.gnu.org/licenses/>.
#
#
from csv import Dialect
from encodings import normalize_encoding
from logging import Logger
from typing import Iterable, BinaryIO

from datagouv_tools.sql.generic import (SQLIndex, QueryProvider,
                                        QueryExecutor, SQLTable)


class PostgreSQLQueryProvider(QueryProvider):
    """
    A provider for PostgreSQL queries.
    """

    # override
    def prepare_copy(self, table: SQLTable) -> Iterable[str]:
        return f'TRUNCATE {table.name}',

    def copy_stream(self, table: SQLTable, encoding: str,
                    dialect: Dialect) -> \
            Iterable[str]:
        encoding = normalize_encoding(encoding).upper()
        options = {"FORMAT": "CSV",
                   "HEADER": "TRUE", "ENCODING": f"'{encoding}'"}
        if dialect.delimiter != ',':
            options["DELIMITER"] = self._escape_char(dialect.delimiter)
        if not dialect.doublequote:
            options["ESCAPE"] = self._escape_char(dialect.escapechar)
        if dialect.quotechar != '"':
            options["QUOTE"] = self._escape_char(dialect.quotechar)
        options_str = ", ".join(f"{k} {v}" for k, v in options.items())

        return f"COPY {table.name} FROM STDIN WITH ({options_str})",

    def _escape_char(self, text: str) -> str:
        """
        See 4.1.2.2. String Constants with C-style Escapes
        :param text:
        :return:
        """
        if text == "\\":
            return "E'\\\\'"
        elif text in "\b\f\n\r\t":
            return f"E'{text}'"
        elif text == "'":
            return "E'\\\''"
        return f"'{text}'"

    # override
    def finalize_copy(self, table: SQLTable) -> Iterable[str]:
        return f'ANALYZE {table.name}',

    # override
    def create_index(self, table: SQLTable, index: SQLIndex) -> Iterable[str]:
        return (f'CREATE INDEX {index.name} ON {table.name} '
                f'USING {index.type_str}({index.field_name})',)


class PostgreSQLQueryExecutor(QueryExecutor):
    def __init__(self, logger: Logger, connection,
                 query_provider: PostgreSQLQueryProvider):
        self._logger = logger
        self._connection = connection
        self._cursor = connection.cursor()
        self._query_provider = query_provider

    # override
    def execute(self, queries: Iterable[str], *args, **kwargs):
        for query in queries:
            self._logger.debug(query)
            self._cursor.execute(query)

    # override
    def executemany(self, query, *args, **kwargs):
        self._logger.debug("%s (%s, %s)", query, args, kwargs)
        self._cursor.executemany(query, *args)

    # override
    def commit(self):
        self._logger.debug("commit")
        self._connection.commit()

    def close(self):
        self._connection.close()

    # override
    @property
    def query_provider(self) -> QueryProvider:
        return self._query_provider

    # override
    def copy_stream(self, table: SQLTable, stream: BinaryIO, encoding: str,
                    dialect: Dialect, count=0):
        queries = self._query_provider.copy_stream(table, encoding,
                                                   dialect)
        for query in queries:
            self._logger.debug(query)
            self._cursor.execute(query, stream=stream)
