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
import unittest

from datagouv_tools.sql.generic import SQLField, SQLTable, SQLIndex
from datagouv_tools.sql.postgresql import PostgreSQLQueryProvider
from datagouv_tools.sql.sql_type import SQLTypes, SQLIndexTypes


class TestQueryProvider(unittest.TestCase):
    def setUp(self):
        self.provider = PostgreSQLQueryProvider()
        self.sql_field1 = SQLField("t", "f1", SQLTypes.TEXT,
                                   comment="comment1")
        self.sql_field2 = SQLField("t", "field_with_long_name2",
                                   SQLTypes.NUMERIC)
        self.sql_field3 = SQLField("t", "f3", SQLTypes.TEXT,
                                   comment="comment2")

    def test_drop(self):
        self.assertEqual(('DROP TABLE IF EXISTS t',),
                         self.provider.drop_table(SQLTable("t", (), ())))

    def test_create_empty(self):
        self.assertEqual(('CREATE TABLE t ()',),
                         self.provider.create_table(SQLTable("t", [], [])))

    def test_prepare_copy(self):
        self.assertEqual(('TRUNCATE t',),
                         self.provider.prepare_copy(SQLTable("t", [], [])))

    def test_copy(self):
        self.assertEqual(("COPY t FROM STDIN WITH "
                          "(FORMAT CSV, HEADER TRUE, ENCODING 'UTF_8')",),
                         self.provider.copy_stream(SQLTable("t", [], []),
                                                   "utf-8", csv.excel))

    def test_finalize_copy(self):
        self.assertEqual(('ANALYZE t',),
                         self.provider.finalize_copy(SQLTable("t", [], [])))

    def test_create_one(self):
        self.assertEqual(('CREATE TABLE t (\n'
                          '    f1 text -- comment1\n'
                          ')',),
                         self.provider.create_table(
                             SQLTable("t", [self.sql_field1], [])))

    def test_create_three(self):
        provider = PostgreSQLQueryProvider()
        self.assertEqual(('CREATE TABLE t (\n'
                          '    f1                    text,    -- comment1\n'
                          '    field_with_long_name2 numeric,\n'
                          '    f3                    text    -- comment2\n'
                          ')',),
                         provider.create_table(SQLTable("t", [self.sql_field1,
                                                              self.sql_field2,
                                                              self.sql_field3],
                                                        [])))

    def test_one_with_index(self):
        sql_index = SQLIndex("t", "f", SQLIndexTypes.HASH)
        self.assertEqual(('CREATE INDEX f_t_idx ON t USING hash(f)',),
                         self.provider.create_index(SQLTable("t", [], []),
                                                    sql_index))


if __name__ == '__main__':
    unittest.main()
