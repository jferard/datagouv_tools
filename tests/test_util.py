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
import queue
import unittest

from datagouv_tools.util import CSVStream


class TestUtil(unittest.TestCase):
    def test_csv_stream(self):
        stream = CSVStream("test stream", ["a", "b", "c"], queue.Queue())
        self.assertEqual(bytearray(b'a'), stream.read(1))
        stream.send("".join(map(str, range(10))))
        self.assertEqual(bytearray(b'\tb\tc\n01234'), stream.read(10))
        ba = bytearray(5)
        stream.readinto(ba)
        self.assertEqual(bytearray(b'56789'), ba)


if __name__ == '__main__':
    unittest.main()
