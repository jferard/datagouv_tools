# coding: utf-8

#   Sirene2pg. An utility to import the SIRENE database to PostgreSQL
#       Copyright (C) 2020 J. Férard <https://github.com/jferard>
#
#   This file is part of Sirene2pg.
#
#  Sirene2pg is free software: you can redistribute it and/or modify it under
#  the terms of the GNU General Public License as published by the Free
#  Software Foundation, either version 3 of the License, or (at your option)
#  any later version.
#
#  Sirene2pg is distributed in the hope that it will be useful, but WITHOUT ANY
#  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
#  FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
#  details. You should have received a copy of the GNU General Public License
#  along with this program. If not, see <http://www.gnu.org/licenses/>.

# import SIRENE to PostgreSQL

import unittest
from logging import Logger
from pathlib import Path
from unittest.mock import Mock, call

import pg8000
from pg8000 import Connection

from import_sirene import import_sirene, SQLIndex, SireSQLIndexProvider, \
    SQLIndexType, SQLField, SQLType, to_snake, PostgreSQLQueryProvider, \
    NormalQueryExecutor, DryRunQueryExecutor, BasicSQLTypeProvider, \
    SQL_TYPE_BY_SIREN_TYPE, SireneSchemaParser, NAME, TYPE, LENGTH, RANK, \
    CAPTION


class ImportSireneTest(unittest.TestCase):
    @unittest.skip("intregation test")
    def test(self):
        path = Path(r"/home/jferard/SIRENE")
        connection = pg8000.connect(user="sirene", password="yourpass",
                                    database="sirene")
        try:
            import_sirene(path, connection)
        finally:
            connection.commit()
            connection.close()

    @unittest.skip("intregation test")
    def test_dry_run(self):
        path = Path(r"/home/jferard/SIRENE")
        import_sirene(path, dry_run=True)


class IndexProviderTest(unittest.TestCase):
    def setUp(self):
        self.provider = SireSQLIndexProvider(
            SQLIndex('tableName', 'fieldName', SQLIndexType.B_TREE))

    def test_index_provider_with_extra(self):
        self.assertEqual([SQLIndex(table_name='tableName',
                                   field_name='siren',
                                   type=SQLIndexType.HASH),
                          SQLIndex(table_name='tableName',
                                   field_name='fieldName',
                                   type=SQLIndexType.B_TREE)],
                         list(self.provider.get_indices(
                             [SQLField("tableName", "fieldName",
                                       SQLType.TEXT),
                              SQLField("tableName", "siren",
                                       SQLType.TEXT),
                              SQLField("tableName", "other",
                                       SQLType.TEXT)
                              ])))

    def test_index_provider(self):
        self.assertEqual([SQLIndex(table_name='tableName2',
                                   field_name='siren',
                                   type=SQLIndexType.HASH)],
                         list(self.provider.get_indices(
                             [SQLField("tableName2", "fieldName",
                                       SQLType.TEXT),
                              SQLField("tableName2", "siren",
                                       SQLType.TEXT),
                              SQLField("tableName2", "other",
                                       SQLType.TEXT)
                              ])))


class SQLFieldTest(unittest.TestCase):
    def test_sort(self):
        self.assertEqual(
            [SQLField("t", "f1", SQLType.TEXT, 1),
             SQLField("t", "f2", SQLType.TEXT, 2),
             SQLField("t", "f3", SQLType.TEXT, 3)],
            list(sorted([
                SQLField("t", "f3", SQLType.TEXT, 3),
                SQLField("t", "f1", SQLType.TEXT, 1),
                SQLField("t", "f2", SQLType.TEXT, 2)
            ])))

    def test_compare(self):
        with self.assertRaises(ValueError):
            SQLField("t1", "f1", SQLType.TEXT, 1) < SQLField("t2", "f2",
                                                             SQLType.TEXT, 1)
        self.assertTrue(
            SQLField("t", "f1", SQLType.TEXT, 1) < SQLField("t", "f2",
                                                            SQLType.TEXT, 2))

    def test_process(self):
        f = SQLField("CamelCaseTable", "camelCaseField", SQLType.TEXT)
        self.assertEqual(
            SQLField('camel_case_table', 'camel_case_field', SQLType.TEXT),
            f.process(to_snake))


class SQLIndexTest(unittest.TestCase):
    def test_process(self):
        f = SQLIndex("CamelCaseTable", "camelCaseField", SQLIndexType.HASH)
        self.assertEqual(
            SQLIndex('camel_case_table', 'camel_case_field',
                     SQLIndexType.HASH),
            f.process(to_snake))


class TestQueryProvider(unittest.TestCase):
    def test_empty(self):
        provider = PostgreSQLQueryProvider(
            "t", [], [], 0, 0
        )
        self.assertEqual(('DROP TABLE IF EXISTS t', 'CREATE TABLE t (\n)'),
                         provider.create_table())
        self.assertEqual(('TRUNCATE t',), provider.prepare_copy())
        self.assertEqual(("COPY t FROM STDIN CSV "
                          "HEADER DELIMITER ',' ENCODING 'UTF-8'",),
                         provider.copy())
        self.assertEqual(('ANALYZE t',), provider.finalize_copy())

    def test_one(self):
        provider = PostgreSQLQueryProvider(
            "t", [SQLField("t", "f", SQLType.TEXT)], [], 1, 10
        )
        self.assertEqual(('DROP TABLE IF EXISTS t',
                          'CREATE TABLE t (\n    f          text\n)'),
                         provider.create_table())

    def test_two(self):
        provider = PostgreSQLQueryProvider(
            "t", [SQLField("t", "f1", SQLType.TEXT, comment="comment1"),
                  SQLField("t", "f2", SQLType.TEXT, comment="comment2")],
            [], 2, 10)
        self.assertEqual(('DROP TABLE IF EXISTS t',
                          'CREATE TABLE t (\n'
                          '    f1         text, -- comment1\n'
                          '    f2         text  -- comment2\n'
                          ')'),
                         provider.create_table())

    def test_one_with_index(self):
        provider = PostgreSQLQueryProvider(
            "t", [SQLField("t", "f", SQLType.TEXT)],
            [SQLIndex("t", "f", SQLIndexType.HASH)], 1, 10
        )
        self.assertEqual(('DROP TABLE IF EXISTS t',
                          'CREATE TABLE t (\n    f          text\n)'),
                         provider.create_table())
        self.assertEqual(
            ('ANALYZE t', 'CREATE INDEX f_t_idx ON t USING hash(f)'),
            provider.finalize_copy())


class QueryExecutorTest(unittest.TestCase):
    def test_normal(self):
        logger: Logger = Mock()
        connection: Connection = Mock()
        qe = NormalQueryExecutor(logger, connection)
        qe.execute_all(["sql1", "sql2"], stream="s")
        qe.commit()

        self.assertEqual(
            [call.debug('Execute: %s (args=%s, stream=%s)', 'sql1', None, 's'),
             call.debug('Execute: %s (args=%s, stream=%s)', 'sql2', None, 's'),
             call.debug('Commit')],
            logger.mock_calls)
        self.assertEqual(
            [call.cursor(),
             call.cursor().execute('sql1', None, 's'),
             call.cursor().execute('sql2', None, 's'),
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
        self.type_provider = BasicSQLTypeProvider(SQL_TYPE_BY_SIREN_TYPE)
        self.index_provider = SireSQLIndexProvider()

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
        self.assertEqual([SQLField('t', 'siren', SQLType.TEXT, 1, 'c', 10)],
                         p.get_fields())
        self.assertEqual([SQLIndex('t', 'siren', SQLIndexType.HASH)],
                         list(p.get_indices()))


if __name__ == "__main__":
    unittest.main()
