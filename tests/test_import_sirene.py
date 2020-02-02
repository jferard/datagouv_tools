# coding: utf-8

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
#
#   This file is part of DataGouv Tools.

# import SIRENE to PostgreSQL
import sqlite3
import unittest
from logging import Logger
from pathlib import Path
from unittest.mock import Mock, call

from datagouv_tools.import_sirene import (SireneSQLIndexProvider,
                                          NormalQueryExecutor,
                                          DryRunQueryExecutor,
                                          BasicSireneTypeToSQLTypeConverter,
                                          SQL_TYPE_BY_SIRENE_TYPE,
                                          SireneSchemaParser,
                                          NAME, TYPE, LENGTH,
                                          RANK,
                                          CAPTION,
                                          import_sirene)
from datagouv_tools.sql.generic import SQLField, SQLIndex
from datagouv_tools.sql.sql_type import SQLIndexTypes, SQLTypes

SKIP_IT = True

try:
    import mysql.connector as mariadb
    import pg8000
    from pg8000 import Connection
except ModuleNotFoundError:
    SKIP_IT = True


class ImportSireneTest(unittest.TestCase):
    def setUp(self):
        self.path = Path(r"/home/jferard/datagouv/sirene")

    @unittest.skipIf(SKIP_IT, "integration test")
    def test_postgres(self):
        connection = pg8000.connect(user="sirene", password="yourpass",
                                    database="sirene")
        pg8000.paramstyle = "qmark"
        try:
            import_sirene(self.path, connection, "postgresql")
        finally:
            connection.commit()
            connection.close()

    @unittest.skipIf(SKIP_IT, "integration test")
    def test_sqlite(self):
        connection = sqlite3.connect('sirene.db')
        try:
            import_sirene(self.path, connection, "sqlite")
        finally:
            connection.commit()
            connection.close()

    @unittest.skipIf(SKIP_IT, "integration test")
    def test_maria(self):
        connection = mariadb.connect(user="sirene", password="yourpass",
                                     database="sirene")
        try:
            import_sirene(self.path, connection, "mariadb")
        finally:
            connection.commit()
            connection.close()

    @unittest.skipIf(SKIP_IT, "integration test")
    def test_dry_run(self):
        import_sirene(self.path, None, "postgresql")


class IndexProviderTest(unittest.TestCase):
    def setUp(self):
        self.provider = SireneSQLIndexProvider(
            SQLIndex('tableName', 'fieldName', SQLIndexTypes.B_TREE))

    def test_index_provider_with_extra(self):
        self.assertEqual([SQLIndex(table_name='tableName',
                                   field_name='siren',
                                   type=SQLIndexTypes.HASH),
                          SQLIndex(table_name='tableName',
                                   field_name='fieldName',
                                   type=SQLIndexTypes.B_TREE)],
                         list(self.provider.get_indices(
                             [SQLField("tableName", "fieldName",
                                       SQLTypes.TEXT),
                              SQLField("tableName", "siren",
                                       SQLTypes.TEXT),
                              SQLField("tableName", "other",
                                       SQLTypes.TEXT)
                              ])))

    def test_index_provider(self):
        self.assertEqual([SQLIndex(table_name='tableName2',
                                   field_name='siren',
                                   type=SQLIndexTypes.HASH)],
                         list(self.provider.get_indices(
                             [SQLField("tableName2", "fieldName",
                                       SQLTypes.TEXT),
                              SQLField("tableName2", "siren",
                                       SQLTypes.TEXT),
                              SQLField("tableName2", "other",
                                       SQLTypes.TEXT)
                              ])))


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
            SQL_TYPE_BY_SIRENE_TYPE)
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
            [SQLField('t', 'siren', SQLTypes.TEXT, 1, 'c', 10)],
            p.get_fields())
        self.assertEqual([SQLIndex('t', 'siren', SQLIndexTypes.HASH)],
                         list(p.get_indices()))


if __name__ == "__main__":
    unittest.main()
