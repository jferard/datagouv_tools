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
from dataclasses import dataclass
from typing import Callable

from datagouv_tools.sql.generic import SQLTypeConverter, SQLIndexProvider, \
    QueryExecutor


@dataclass
class ImporterContext:
    """
    :param type_converter: a function `field_name, table_name -> None or
                            a SQLType`
    :param index_provider: a index provider
    """
    type_converter: SQLTypeConverter
    index_provider: SQLIndexProvider
    query_executor: QueryExecutor


@dataclass
class ImporterThreadContext:
    type_converter: SQLTypeConverter
    index_provider: SQLIndexProvider
    new_executor: Callable[[], QueryExecutor]
