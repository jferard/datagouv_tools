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
from codecs import getreader
from csv import Dialect, reader as csv_reader
from io import BytesIO
from logging import Logger
from typing import Iterable, Iterator, Tuple

from datagouv_tools.sql.generic import (QueryProvider,
                                        QueryExecutor, SQLTable)


class SQLiteQueryExecutor(QueryExecutor):
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

    def close(self):
        self._connection.close()

    @property
    def query_provider(self) -> QueryProvider:
        return self._query_provider

    def copy_stream(self, table: SQLTable, stream: BytesIO, encoding: str,
                    dialect: Dialect, count=0):
        # No bulk copy query in sqlite
        self.insert_all(table, stream, encoding, dialect)

    def insert_all(self, table: SQLTable, stream: BytesIO, encoding: str,
                   dialect: Dialect, count=0):
        stream = getreader(encoding)(stream)
        reader = csv_reader(stream, dialect)
        next(reader)
        self.insert_rows(table, reader)
