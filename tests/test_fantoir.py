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
import io
import unittest
import zipfile
from pathlib import Path

from datagouv_tools.fantoir import parse


class TestFantoir(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.path = Path(
            Path(__file__).parent, "resources", "fantoir",
            r"Fichier national FANTOIR (situation octobre 2019)_reduit.zip")

    def test(self):
        data = zipfile.ZipFile(self.path).read("-")
        for r in parse(io.StringIO(data.decode("ascii"))):
            record_type = r.get_type()
            if record_type == "direction":
                self.assertEqual(
                    ['01', '0', 'AIN                           '], list(r))
            elif record_type == "commune":
                self.assertEqual(
                    ['01', '0', '001', 'W', "L'ABERGEMENT-CLEMENCIAT       ",
                     'N', '3', ' ', '0000825', '0000000', '0000000', ' ',
                     '0000000', '1987001'], list(r))
            elif record_type == "voie":
                self.assertEqual(
                    ['01', '0', '001', 'A008', 'W', 'LOT ',
                     'BELLEVUE                  ', 'N', '3', '0', ' ',
                     '0000000', '0000000', ' ', '0000000', '2001351', '00059',
                     '2', ' ', 'BELLEVUE'], list(r))
                break


if __name__ == '__main__':
    unittest.main()
