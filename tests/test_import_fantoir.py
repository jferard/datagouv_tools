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
from pathlib import Path

import pg8000

from datagouv_tools.import_fantoir import import_fantoir, import_fantoir_thread


class TestImportFantoir(unittest.TestCase):
    def setUp(self):
        self.path = Path(
            r"/home/jferard/datagouv/fantoir/Fichier national FANTOIR (situation octobre 2019).zip")

    def test_import_temp(self):
        connection = pg8000.connect(user="postgres", password="postgres",
                                    database="sirene")
        rdbms = "pg"
        fantoir_path = self.path
        import_fantoir(connection, fantoir_path, rdbms)
        connection.close()

    def test_import_threads(self):
        rdbms = "pg"
        fantoir_path = self.path
        import_fantoir_thread(
            lambda: pg8000.connect(user="postgres", password="postgres",
                                   database="sirene"), fantoir_path, rdbms)


if __name__ == '__main__':
    unittest.main()
