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

import unittest
from csv import unix_dialect
from io import BytesIO
from logging import Logger
from sqlite3 import Connection, Cursor
from unittest import mock
from unittest.mock import Mock, call

from datagouv_tools.sql.generic import QueryProvider, SQLTable
from datagouv_tools.sql.sqlite import SQLiteQueryExecutor


class TestSQLite(unittest.TestCase):
    def setUp(self):
        self.logger: Logger = Mock()
        self.connection: Connection = Mock()
        self.cursor: Cursor = Mock()
        self.query_provider = QueryProvider()

        self.connection.cursor.return_value = self.cursor

        self.executor = SQLiteQueryExecutor(self.logger, self.connection,
                                            self.query_provider)

    def test_execute_empty(self):
        self.executor.execute([])
        self.assertEqual([], self.logger.mock_calls)
        self.assertEqual([call.cursor()], self.connection.mock_calls)

    def test_execute(self):
        self.executor.execute(["a query"])
        self.assertEqual([call.debug("a query")], self.logger.mock_calls)
        self.assertEqual([call.cursor(), call.cursor().execute('a query')],
                         self.connection.mock_calls)

    def test_executemany(self):
        self.executor.executemany("a query")
        self.assertEqual([call.debug('%s (%s, %s)', 'a query', (), {})],
                         self.logger.mock_calls)
        self.assertEqual([call.cursor(), call.cursor().executemany('a query')],
                         self.connection.mock_calls)

    def test_commit(self):
        self.executor.commit()
        self.assertEqual([call.debug('commit')],
                         self.logger.mock_calls)
        self.assertEqual([call.cursor(), call.commit()],
                         self.connection.mock_calls)

    def test_copy_stream(self):
        self.executor.copy_stream(SQLTable("table", [], []), BytesIO(b"data"),
                                  "utf-8", unix_dialect)
        self.assertEqual([call.debug('%s (%s, %s)',
                                     'INSERT INTO table VALUES ()', mock.ANY,
                                     {})],
                         self.logger.mock_calls)
        self.assertEqual([call.cursor(),
                          call.cursor().executemany(
                              'INSERT INTO table VALUES ()', mock.ANY)],
                         self.connection.mock_calls)


if __name__ == '__main__':
    unittest.main()
