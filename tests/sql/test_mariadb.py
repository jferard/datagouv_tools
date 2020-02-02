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
from logging import Logger
from pathlib import Path
from sqlite3 import Connection, Cursor
from unittest.mock import Mock, call

from datagouv_tools.sql.generic import SQLTable, SQLField, \
    SQLIndex
from datagouv_tools.sql.mariadb import MariaDBQueryExecutor, \
    MariaDBQueryProvider
from datagouv_tools.sql.sql_type import SQLTypes, SQLIndexTypes


class TestMariaDBProvider(unittest.TestCase):
    def setUp(self):
        self.field1 = SQLField("table", "field1", SQLTypes.BOOLEAN,
                               comment="comment")
        self.field2 = SQLField("table", "field2", SQLTypes.NUMERIC)
        self.index = SQLIndex("table", "field1", SQLIndexTypes.HASH)
        self.table = SQLTable("table", [self.field1, self.field2], [])
        self.query_provider = MariaDBQueryProvider()

    def test_copy_path(self):
        self.assertEqual(("LOAD DATA INFILE 'path'\n"
                          'INTO TABLE `table`\n'
                          "CHARACTER SET 'UTF8'\n"
                          "FIELDS TERMINATED BY ','\n"
                          'OPTIONALLY ENCLOSED BY \'"\'\n'
                          'IGNORE 1 LINES',),
                         self.query_provider.copy_path(self.table,
                                                       Path("path"),
                                                       "utf-8", unix_dialect))

    def test_create_other_index(self):
        self.table = SQLTable("other_table", [], [self.index])
        with self.assertRaises(AssertionError):
            self.query_provider.create_index(self.table, self.index)

    def test_create_index(self):
        self.table = SQLTable("table", [], [self.index])
        self.assertEqual(
            ('CREATE INDEX field1_table_idx ON table(field1(255))',),
            self.query_provider.create_index(self.table,
                                             self.index))


class TestMariaDBExecutor(unittest.TestCase):
    def setUp(self):
        self.logger: Logger = Mock()
        self.connection: Connection = Mock()
        self.cursor: Cursor = Mock()
        self.query_provider = MariaDBQueryProvider()

        self.connection.cursor.return_value = self.cursor

        self.executor = MariaDBQueryExecutor(self.logger, self.connection,
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


if __name__ == '__main__':
    unittest.main()
