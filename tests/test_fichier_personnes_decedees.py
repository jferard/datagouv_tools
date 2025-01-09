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
import sqlite3
import unittest
from pathlib import Path

from datagouv_tools.fichier_personnes_decedees import InseeLineParser, \
    import_deces

TEST_FILES_PATH = Path(__file__).parent.parent / "test_files"


class DecedeesTestCase(unittest.TestCase):
    def test_encoding(self):
        bs = b'                                                  '
        with (TEST_FILES_PATH / "deces-2022.txt").open("rb") as s:
            for i in range(10000000):
                c = s.read(1)
                bs = bs[1:] + c
                if int.from_bytes(c) >= 128:
                    print(bs)

    def test_parse(self):
        with (TEST_FILES_PATH / "deces-2022.txt").open("r",
                                                       encoding="utf-8") as s:
            for line in s:
                d = InseeLineParser().parse(line)

    def test_import_file(self):
        connection = sqlite3.connect("deces.db")
        rdbms = "sqlite"
        deces_path = TEST_FILES_PATH / "deces-2022.txt"
        import_deces(connection, deces_path, rdbms)
        connection.close()


if __name__ == '__main__':
    unittest.main()
