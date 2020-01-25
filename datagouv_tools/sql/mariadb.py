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
from pathlib import Path
from typing import Iterable

from datagouv_tools.sql.generic import (QueryProvider,
                                        QueryExecutor, SQLIndex, SQLField)


class MariaDBQueryProvider(QueryProvider[SQLField, SQLIndex]):
    def copy_path(self, table_name: str, path: Path, encoding: str,
                  dialect: Dialect) -> Iterable[str]:
        encoding = normalize_encoding(encoding).upper().replace("_", "")
        lines = [
            f"LOAD DATA INFILE '{path}'",
            f"INTO TABLE `{table_name}`",
            f"CHARACTER SET '{encoding}'",
            f"FIELDS TERMINATED BY '{dialect.delimiter}'",
            f"OPTIONALLY ENCLOSED BY '{dialect.quotechar}'",
        ]
        if not dialect.doublequote:
            lines.append(f"ESCAPED BY '{dialect.escapechar}'")
        lines.append("IGNORE 1 LINES")
        return "\n".join(lines),

    def create_index(self, table_name: str, index: SQLIndex) -> Iterable[str]:
        return f'CREATE INDEX {index.name} ON {table_name}({index.field_name}(255))',


class MariaDBQueryExecutor(QueryExecutor):
    def __init__(self, logger: Logger, connection,
                 query_provider: QueryProvider):
        self._logger = logger
        self._connection = connection
        self._cursor = connection.cursor()
        self._query_provider = query_provider

    def execute(self, queries: Iterable[str], *args, **kwargs):
        for query in queries:
            self._logger.debug(query)
            self._cursor.execute(query)

    def executemany(self, query: str, *args, **kwargs):
        self._logger.debug("%s (%s, %s)", query, args, kwargs)
        self._cursor.executemany(query, *args)

    def commit(self):
        self._logger.debug("commit")
        self._connection.commit()

    @property
    def query_provider(self) -> QueryProvider:
        return self._query_provider

    def copy_path(self, table_name: str, path: Path, encoding: str,
                  dialect: Dialect, count=0):
        queries = self.query_provider.copy_path(table_name, path, encoding,
                                                dialect)
        self.execute(queries)
