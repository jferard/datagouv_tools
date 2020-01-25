# coding: utf-8

#   DataGouv Tools. An utility to import  some data.gouv.fr data to PostgreSQL and other DBMS.
#       Copyright (C) 2020 J. Férard <https://github.com/jferard>
#
#   This file is part of DataGouv Tools.
#
#  DataGouv Tools is free software: you can redistribute it and/or modify it under
#  the terms of the GNU General Public License as published by the Free
#  Software Foundation, either version 3 of the License, or (at your option)
#  any later version.
#
#  DataGouv Tools is distributed in the hope that it will be useful, but WITHOUT ANY
#  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
#  FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
#  details. You should have received a copy of the GNU General Public License
#  along with this program. If not, see <http://www.gnu.org/licenses/>.

# import SIRENE to PostgreSQL
import csv
import sqlite3
import unittest
from logging import Logger
from pathlib import Path
from unittest.mock import Mock, call

import mysql.connector as mariadb
import pg8000
from pg8000 import Connection

from datagouv_tools.import_sirene import (SireneSQLIndexProvider,
                                          to_snake,
                                          NormalQueryExecutor,
                                          DryRunQueryExecutor,
                                          BasicSireneTypeToSQLTypeConverter,
                                          POSTGRE_SQL_TYPE_BY_SIREN_TYPE,
                                          SireneSchemaParser,
                                          NAME, TYPE, LENGTH,
                                          RANK,
                                          CAPTION,
                                          PatchedPostgreSireneTypeToSQLTypeConverter,
                                          import_sirene)
from datagouv_tools.sql.generic import SQLField, SQLIndex
from datagouv_tools.sql.postgresql import (PostgreSQLType, PostgreSQLIndexType,
                                           PostgreSQLQueryProvider)


class ImportSireneTest(unittest.TestCase):
    @unittest.skip("intregation test")
    def test_postgres(self):
        path = Path(r"/home/jferard/SIRENE")
        connection = pg8000.connect(user="sirene", password="yourpass",
                                    database="sirene")
        try:
            import_sirene(path, connection, "postgresql")
        finally:
            connection.commit()
            connection.close()

    @unittest.skip("intregation test")
    def test_sqlite(self):
        path = Path(r"/home/jferard/SIRENE")
        connection = sqlite3.connect('sirene.db')
        try:
            import_sirene(path, connection, "sqlite")
        finally:
            connection.commit()
            connection.close()

    @unittest.skip("intregation test")
    def test_maria(self):
        path = Path(r"/home/jferard/SIRENE")
        connection = mariadb.connect(user="sirene", password="yourpass",
                                     database="sirene")
        try:
            import_sirene(path, connection, "mariadb")
        finally:
            connection.commit()
            connection.close()

    def test_dry_run(self):
        path = Path(r"/home/jferard/SIRENE")
        import_sirene(path, None, "postgresql")


class IndexProviderTest(unittest.TestCase):
    def setUp(self):
        self.provider = SireneSQLIndexProvider(
            SQLIndex('tableName', 'fieldName', PostgreSQLIndexType.B_TREE))

    def test_index_provider_with_extra(self):
        self.assertEqual([SQLIndex(table_name='tableName',
                                   field_name='siren',
                                   type=PostgreSQLIndexType.HASH),
                          SQLIndex(table_name='tableName',
                                   field_name='fieldName',
                                   type=PostgreSQLIndexType.B_TREE)],
                         list(self.provider.get_indices(
                             [SQLField("tableName", "fieldName",
                                       PostgreSQLType.TEXT),
                              SQLField("tableName", "siren",
                                       PostgreSQLType.TEXT),
                              SQLField("tableName", "other",
                                       PostgreSQLType.TEXT)
                              ])))

    def test_index_provider(self):
        self.assertEqual([SQLIndex(table_name='tableName2',
                                   field_name='siren',
                                   type=PostgreSQLIndexType.HASH)],
                         list(self.provider.get_indices(
                             [SQLField("tableName2", "fieldName",
                                       PostgreSQLType.TEXT),
                              SQLField("tableName2", "siren",
                                       PostgreSQLType.TEXT),
                              SQLField("tableName2", "other",
                                       PostgreSQLType.TEXT)
                              ])))


class SQLFieldTest(unittest.TestCase):
    def test_sort(self):
        self.assertEqual(
            [SQLField("t", "f1", PostgreSQLType.TEXT, 1),
             SQLField("t", "f2", PostgreSQLType.TEXT, 2),
             SQLField("t", "f3", PostgreSQLType.TEXT, 3)],
            list(sorted([
                SQLField("t", "f3", PostgreSQLType.TEXT, 3),
                SQLField("t", "f1", PostgreSQLType.TEXT, 1),
                SQLField("t", "f2", PostgreSQLType.TEXT, 2)
            ])))

    def test_compare(self):
        with self.assertRaises(ValueError):
            SQLField("t1", "f1", PostgreSQLType.TEXT, 1) < SQLField("t2", "f2",
                                                                    PostgreSQLType.TEXT,
                                                                    1)
        self.assertTrue(
            SQLField("t", "f1", PostgreSQLType.TEXT, 1) < SQLField("t", "f2",
                                                                   PostgreSQLType.TEXT,
                                                                   2))

    def test_process(self):
        f = SQLField("CamelCaseTable", "camelCaseField", PostgreSQLType.TEXT)
        self.assertEqual(
            SQLField('camel_case_table', 'camel_case_field',
                     PostgreSQLType.TEXT),
            f.process(to_snake))


class SQLIndexTest(unittest.TestCase):
    def test_process(self):
        f = SQLIndex("CamelCaseTable", "camelCaseField",
                     PostgreSQLIndexType.HASH)
        self.assertEqual(
            SQLIndex('camel_case_table', 'camel_case_field',
                     PostgreSQLIndexType.HASH),
            f.process(to_snake))


class TestQueryProvider(unittest.TestCase):
    def setUp(self):
        self.provider = PostgreSQLQueryProvider()
        self.sql_field1 = SQLField("t", "f1", PostgreSQLType.TEXT,
                                   comment="comment1")
        self.sql_field2 = SQLField("t", "field_with_long_name2",
                                   PostgreSQLType.NUMERIC)
        self.sql_field3 = SQLField("t", "f3", PostgreSQLType.TEXT,
                                   comment="comment2")

    def test_drop(self):
        self.assertEqual(('DROP TABLE IF EXISTS t',),
                         self.provider.drop_table("t"))

    def test_create_empty(self):
        self.assertEqual(('CREATE TABLE t ()',),
                         self.provider.create_table("t", []))

    def test_prepare_copy(self):
        self.assertEqual(('TRUNCATE t',), self.provider.prepare_copy("t"))

    def test_copy(self):
        self.assertEqual(("COPY t FROM STDIN WITH "
                          "(FORMAT CSV, HEADER TRUE, ENCODING 'UTF_8')",),
                         self.provider.copy_stream("t", "utf-8", csv.excel))

    def test_finalize_copy(self):
        self.assertEqual(('ANALYZE t',), self.provider.finalize_copy("t"))

    def test_create_one(self):
        self.assertEqual(('CREATE TABLE t (\n'
                          '    f1 text -- comment1\n'
                          ')',),
                         self.provider.create_table("t", [self.sql_field1]))

    def test_create_three(self):
        provider = PostgreSQLQueryProvider()
        self.assertEqual(('CREATE TABLE t (\n'
                          '    f1                    text,    -- comment1\n'
                          '    field_with_long_name2 numeric,\n'
                          '    f3                    text    -- comment2\n'
                          ')',),
                         provider.create_table("t", [self.sql_field1,
                                                     self.sql_field2,
                                                     self.sql_field3]))

    def test_one_with_index(self):
        sql_index = SQLIndex("t", "f", PostgreSQLIndexType.HASH)
        self.assertEqual(('CREATE INDEX f_t_idx ON t USING hash(f)',),
                         self.provider.create_index("t", sql_index))


class QueryExecutorTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

    def test_normal(self):
        logger: Logger = Mock()
        connection: Connection = Mock()
        qe = NormalQueryExecutor(logger, connection)
        qe.execute_all(["sql1", "sql2"], stream="s")
        qe.commit()

        self.assertEqual(
            [call.debug('Execute: %s (args=%s, stream=%s)', 'sql1', (),
                        {'stream': 's'}),
             call.debug('Execute: %s (args=%s, stream=%s)', 'sql2', (),
                        {'stream': 's'}),
             call.debug('Commit')],
            logger.mock_calls)
        self.assertEqual(
            [call.cursor(),
             call.cursor().execute('sql1', stream='s'),
             call.cursor().execute('sql2', stream='s'),
             call.commit()], connection.mock_calls)

    def test_dry(self):
        logger: Logger = Mock()
        qe = DryRunQueryExecutor(logger)
        qe.execute_all(["sql1", "sql2"], stream="s")
        qe.commit()

        self.assertEqual(
            [call.debug('>>> Dry run: %s (args=%s, stream=%s)', 'sql1', None,
                        's'),
             call.debug('>>> Dry run: %s (args=%s, stream=%s)', 'sql2', None,
                        's'),
             call.debug('>>> Dry run: Commit')],
            logger.mock_calls)


class ParserTest(unittest.TestCase):
    def setUp(self):
        self.type_provider = BasicSireneTypeToSQLTypeConverter(
            POSTGRE_SQL_TYPE_BY_SIREN_TYPE)
        self.index_provider = SireneSQLIndexProvider()

    def test_empty(self):
        p = SireneSchemaParser("t", [], self.type_provider,
                               self.index_provider)
        self.assertEqual("t", p.table_name)
        self.assertEqual([], p.get_fields())
        self.assertEqual([], list(p.get_indices()))

    def test_one_indexed(self):
        p = SireneSchemaParser("t", [
            {NAME: "siren", TYPE: "Texte", LENGTH: "10", RANK: "1",
             CAPTION: "c"}],
                               self.type_provider,
                               self.index_provider)
        self.assertEqual("t", p.table_name)
        self.assertEqual(
            [SQLField('t', 'siren', PostgreSQLType.TEXT, 1, 'c', 10)],
            p.get_fields())
        self.assertEqual([SQLIndex('t', 'siren', PostgreSQLIndexType.HASH)],
                         list(p.get_indices()))


if __name__ == "__main__":
    unittest.main()
