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
import csv
from csv import Dialect
from encodings import normalize_encoding
from enum import Enum
from io import BytesIO
from logging import Logger
from typing import Sequence, Iterable, Any, Mapping

from datagouv_tools.sql.generic import SQLField, SQLIndex, QueryProvider, \
    QueryExecutor, SQLType, SQLIndexType


class PostgreSQLType(Enum):
    """
    A SQL type
    """
    TEXT: SQLType = "text"
    DATE: SQLType = "date"
    NUMERIC: SQLType = "numeric"
    BOOLEAN: SQLType = "boolean"

    def type_str(self, params: Mapping[str, Any]) -> str:
        return self.value


class PostgreSQLIndexType(Enum):
    """
    A SQL index type
    """
    B_TREE: SQLIndexType = "btree"
    HASH: SQLIndexType = "hash"
    GIST: SQLIndexType = "gist"
    SP_GIST: SQLIndexType = "spgist"
    GIN: SQLIndexType = "gin"

    @property
    def type_str(self):
        return self.value


class PostgreSQLQueryProvider(QueryProvider[SQLField[PostgreSQLType],
                                            SQLIndex[PostgreSQLIndexType]]):
    """
    A provider for PostgreSQL queries.
    """

    # override
    def prepare_copy(self, table_name: str) -> Iterable[str]:
        return f'TRUNCATE {table_name}',

    def copy_stream(self, table_name: str, encoding: str, dialect: Dialect) -> \
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

        return f"COPY {table_name} FROM STDIN WITH ({options_str})",

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
    def finalize_copy(self, table_name: str) -> Iterable[str]:
        return f'ANALYZE {table_name}',

    # override
    def create_index(self, table_name: str,
                     index: SQLIndex[PostgreSQLIndexType]) -> Iterable[str]:
        return (f'CREATE INDEX {index.name} ON {table_name} '
                f'USING {index.type_str}({index.field_name})',)


class PostgreSQLQueryExecutor(
    QueryExecutor[SQLField[PostgreSQLType],
                  SQLIndex[PostgreSQLIndexType]]):
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

    # override
    @property
    def query_provider(self) -> QueryProvider:
        return self._query_provider

    # override
    def copy_stream(self, table_name: str, stream: BytesIO, encoding: str,
                    dialect: Dialect):
        queries = self._query_provider.copy_stream(table_name, encoding,
                                                   dialect)
        for query in queries:
            self._logger.debug(query)
            self._cursor.execute(query, stream=stream)
