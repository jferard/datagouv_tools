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
from pathlib import Path

import pg8000

from import_sirene import import_sirene, SQLIndex, SireSQLIndexProvider, \
    SQLIndexType, SQLField, SQLType


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


if __name__ == "__main__":
    unittest.main()
