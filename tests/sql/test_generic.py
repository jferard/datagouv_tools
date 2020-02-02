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

from datagouv_tools.sql.generic import (QueryProvider, SQLTable, SQLField,
                                        SQLIndex)
from datagouv_tools.sql.sql_type import SQLTypes, SQLIndexTypes
from datagouv_tools.util import to_snake


class TestGenericProvider(unittest.TestCase):
    def setUp(self):
        self.provider = QueryProvider()
        self.field1 = SQLField("table", "field1", SQLTypes.BOOLEAN,
                               comment="comment")
        self.field2 = SQLField("table", "field2", SQLTypes.NUMERIC)
        self.index = SQLIndex("table", "field1", SQLIndexTypes.HASH)

    def test_drop_table(self):
        self.table = SQLTable("table", [], [])
        self.assertEqual(('DROP TABLE IF EXISTS table',),
                         self.provider.drop_table(self.table))

    def test_create_empty_table(self):
        self.table = SQLTable("table", [], [])
        self.assertEqual(('CREATE TABLE table ()',),
                         self.provider.create_table(self.table))

    def test_create_one_field_table(self):
        self.table = SQLTable("table", [self.field1], [])
        self.assertEqual(('CREATE TABLE table (\n'
                          '    field1 boolean -- comment\n'
                          ')',),
                         self.provider.create_table(self.table))

    def test_create_two_fields_table(self):
        self.table = SQLTable("table", [self.field1, self.field2], [])
        self.assertEqual(('CREATE TABLE table (\n'
                          '    field1 boolean, -- comment\n'
                          '    field2 numeric\n'
                          ')',),
                         self.provider.create_table(self.table))

    def test_prepare_copy(self):
        self.table = SQLTable("table", [], [])
        self.assertEqual((),
                         self.provider.prepare_copy(self.table))

    def test_insert_all(self):
        self.table = SQLTable("table", [self.field1, self.field2], [])
        self.assertEqual('INSERT INTO table VALUES (?, ?)',
                         self.provider.insert_all(self.table))

    def test_finalize_copy(self):
        self.table = SQLTable("table", [], [])
        self.assertEqual((),
                         self.provider.finalize_copy(self.table))

    def test_create_other_index(self):
        self.table = SQLTable("other_table", [], [self.index])
        with self.assertRaises(AssertionError):
            self.provider.create_index(self.table, self.index)

    def test_create_index(self):
        self.table = SQLTable("table", [], [self.index])
        self.assertEqual(('CREATE INDEX field1_table_idx ON table(field1)',),
                         self.provider.create_index(self.table, self.index))


class SQLFieldTest(unittest.TestCase):
    def test_sort(self):
        self.assertEqual(
            [SQLField("t", "f1", SQLTypes.TEXT, 1),
             SQLField("t", "f2", SQLTypes.TEXT, 2),
             SQLField("t", "f3", SQLTypes.TEXT, 3)],
            list(sorted([
                SQLField("t", "f3", SQLTypes.TEXT, 3),
                SQLField("t", "f1", SQLTypes.TEXT, 1),
                SQLField("t", "f2", SQLTypes.TEXT, 2)
            ])))

    def test_compare(self):
        with self.assertRaises(ValueError):
            SQLField("t1", "f1", SQLTypes.TEXT, 1) < SQLField(
                "t2", "f2", SQLTypes.TEXT, 1)
        self.assertTrue(
            SQLField("t", "f1", SQLTypes.TEXT, 1) < SQLField("t", "f2",
                                                             SQLTypes.TEXT,
                                                             2))

    def test_process(self):
        f = SQLField("CamelCaseTable", "camelCaseField", SQLTypes.TEXT)
        self.assertEqual(
            SQLField('camel_case_table', 'camel_case_field',
                     SQLTypes.TEXT),
            f.process(to_snake))


class SQLIndexTest(unittest.TestCase):
    def test_process(self):
        f = SQLIndex("CamelCaseTable", "camelCaseField",
                     SQLIndexTypes.HASH)
        self.assertEqual(
            SQLIndex('camel_case_table', 'camel_case_field',
                     SQLIndexTypes.HASH),
            f.process(to_snake))


if __name__ == '__main__':
    unittest.main()
