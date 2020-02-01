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
from datetime import datetime

from datagouv_tools.sql.sql_type import SQLTypes


class TestSQLTypes(unittest.TestCase):
    def test_types(self):
        self.assertEqual("boolean", SQLTypes.BOOLEAN.to_str())
        self.assertEqual(False, SQLTypes.BOOLEAN.type_value("False"))

        self.assertEqual("character(10)", SQLTypes.CHARACTER.to_str('10'))
        self.assertEqual("text", SQLTypes.CHARACTER.type_value("text"))

        self.assertEqual("decimal(10, 2)", SQLTypes.DECIMAL.to_str('10', '2'))
        self.assertEqual(10.5, SQLTypes.DECIMAL.type_value("10.5"))

        self.assertEqual("timestamp without time zone",
                         SQLTypes.TIMESTAMP_WITHOUT_TIME_ZONE.to_str())
        self.assertEqual("timestamp(5) without time zone",
                         SQLTypes.TIMESTAMP_WITHOUT_TIME_ZONE.to_str('5'))
        self.assertEqual(datetime(2017, 1, 5, 10, 13, 15),
                         SQLTypes.TIMESTAMP_WITHOUT_TIME_ZONE.type_value(
                             "2017-01-05 10:13:15"))


if __name__ == '__main__':
    unittest.main()
