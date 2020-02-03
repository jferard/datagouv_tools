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
import os
import shutil
import tempfile
from abc import ABC, abstractmethod
from codecs import getreader
from csv import reader as csv_reader, Dialect
from dataclasses import dataclass
from io import BytesIO
from itertools import chain
from pathlib import Path
from typing import (Callable, Iterable, Generic, TypeVar, Any, BinaryIO,
                    Collection)

from datagouv_tools.sql.sql_type import SQLType, SQLIndexType, SQLTypes

U = TypeVar('U')


@dataclass(eq=True)
class SQLField:
    """
    A SQL field
    """
    table_name: str  # original field name
    field_name: str
    type: SQLType
    rank: int = 0
    comment: str = ""
    length: int = 0
    type_params: Iterable[Any] = None

    def __post_init__(self):
        if self.type_params is None:
            self.type_params = ()

    def process(self, process_names: Callable[[str], str]) -> "SQLField":
        return SQLField(process_names(self.table_name),
                        process_names(self.field_name), self.type, self.rank,
                        self.comment, self.length)

    @property
    def type_str(self):
        return self.type.to_str(*self.type_params)

    def type_value(self, value):
        return self.type.type_value(value)

    def __lt__(self, other):
        if self.table_name != other.table_name:
            raise ValueError("field from different tables are not comparable")
        return self.rank < other.rank

    def __repr__(self):
        return (f"SQLField({self.table_name}, {self.field_name}, {self.type}, "
                f"{self.rank}, {self.comment}, {self.length}, "
                f"{self.type_params})")


F = TypeVar('F', bound=SQLField)


@dataclass(eq=True)
class SQLIndex:
    """
    A SQL index
    """
    table_name: str
    field_name: str
    type: SQLIndexType

    def process(self, process_names: Callable[[str], str]) -> "SQLIndex":
        return SQLIndex(process_names(self.table_name),
                        process_names(self.field_name), self.type)

    @property
    def name(self) -> str:
        """
        :return: the name of the index, that is field name + table name + "idx"
        """
        table = str.maketrans(dict.fromkeys('aeiou'))
        if len(self.field_name) + len(self.table_name) > 64:
            table_name = self.table_name.translate(table)
            field_name = self.field_name.translate(table)
        else:
            table_name = self.table_name
            field_name = self.field_name

        return f'{field_name}_{table_name}_idx'

    @property
    def type_str(self) -> str:
        return self.type.to_str()


@dataclass
class SQLTable:
    name: str
    fields: Collection[SQLField]
    indices: Iterable[SQLIndex]


class QueryProvider(ABC):
    """
    A SQL query provider
    """

    def drop_table(self, table: SQLTable) -> Iterable[str]:
        return f'DROP TABLE IF EXISTS {table.name}',

    def create_table(self, table: SQLTable) -> Iterable[str]:
        """
        :return: a list of queries to create a table
        """
        fields = list(table.fields)
        if not fields:
            return f'CREATE TABLE {table.name} ()',

        sql_type_max_size = max(len(f.type_str) for f in fields)
        name_max_size = max(len(f.field_name) for f in fields)

        lines = [f'CREATE TABLE {table.name} (']
        for field in fields[:-1]:
            lines.append(
                self._create_line(field, name_max_size, sql_type_max_size, ',')
            )
        lines.append(
            self._create_line(fields[-1], name_max_size,
                              sql_type_max_size, ''))
        lines.append(")")
        return "\n".join(lines),

    def _create_line(self, field: F, name_max_size: int,
                     sql_type_max_size: int, comma) -> str:
        field_name = self._field_name(field, name_max_size)
        field_type = self._type(field, sql_type_max_size, comma)
        if field.comment:
            return f"    {field_name} {field_type} -- {field.comment}"
        else:
            return f"    {field_name} {field_type}"

    def _field_name(self, field: F, width) -> str:
        return field.field_name.ljust(width)

    def _type(self, field: SQLField, width, comma=','):
        return (field.type_str + comma).ljust(
            width + len(comma))

    def prepare_copy(self, table: SQLTable) -> Iterable[str]:
        """
        :return: a list of queries to prepare a copy
        """
        return ()

    def insert_all(self, table: SQLTable) -> str:
        field_count = len(table.fields)
        question_marks = ", ".join(["?"] * field_count)
        return f'INSERT INTO {table.name} VALUES ({question_marks})'

    def finalize_copy(self, table: SQLTable) -> Iterable[str]:
        """
        :return: a list of queries to finalize a copy
        """
        return ()

    def create_index(self, table: SQLTable, index: SQLIndex) -> Iterable[str]:
        assert index.table_name == table.name
        return (f'DROP INDEX IF EXISTS {index.name}',
                f'CREATE INDEX {index.name} ON '
                f'{table.name}({index.field_name})')


class QueryExecutor(ABC):
    @abstractmethod
    def execute(self, queries: Iterable[str], *args, **kwargs):
        pass

    @abstractmethod
    def executemany(self, query: str, *args, **kwargs):
        pass

    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @property
    @abstractmethod
    def query_provider(self) -> QueryProvider:
        pass

    def create_table(self, table: SQLTable):
        """
        create a new table

        :param table: the table
        """
        self.execute(self.query_provider.drop_table(table))
        self.execute(self.query_provider.create_table(table))

    def prepare_copy(self, table: SQLTable):
        """
        create a new table
        """
        self.execute(self.query_provider.prepare_copy(table))

    def copy_path(self, table: SQLTable, path: Path, encoding: str,
                  dialect: Dialect, count=0):
        """
        create a new table
        """
        if count > 1:
            raise Exception(
                "An executor should implement copy_stream or copy_path")
        with path.open("rb") as source:
            self.copy_stream(table, source, encoding, dialect,
                             count=count + 1)

    def copy_stream(self, table: SQLTable, stream: BinaryIO, encoding: str,
                    dialect: Dialect, count=0):
        """
        create a new table
        """
        if count > 1:
            raise Exception(
                "An executor should implement copy_stream or copy_path")
        handle, temp_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(handle, "wb") as dest:
            shutil.copyfileobj(stream, dest)
            os.chmod(temp_path, 0o644)

        self.copy_path(table, Path(temp_path), encoding, dialect,
                       count=count + 1)
        os.remove(temp_path)

    def insert_all(self, table: SQLTable, stream: BytesIO, encoding: str,
                   dialect: Dialect, count=0):
        stream = getreader(encoding)(stream)
        reader = csv_reader(stream, dialect)
        next(reader)
        query = self.query_provider.insert_all(table)
        table_fields = table.fields
        typed_reader = (tuple(field.type_value(value)
                              for field, value in zip(table_fields, row))
                        for row in reader)
        self.executemany(query, typed_reader)

    def finalize_copy(self, table: SQLTable):
        """
        create a new table
        """
        self.execute(self.query_provider.finalize_copy(table))

    def create_indices(self, table: SQLTable):
        """
        create a new table
        """
        for index in table.indices:
            self.execute(self.query_provider.create_index(table, index))


class FakeAttr:
    def __init__(self, logger, obj, item):
        self._logger = logger
        self._obj = obj
        self._item = item

    def __call__(self, *args, **kwargs):
        arg_str = ', '.join(chain(
            map(str, args),
            (f"{k}={v}" for k, v in kwargs.items())
        ))
        self._logger.info(f"DRY RUN: {self._obj}.{self._item}({arg_str})")


class FakeCursor:
    def __init__(self, logger, connection):
        self._logger = logger
        self._connection = connection

    def __getattr__(self, item):
        return FakeAttr(self._logger, self, item)

    def __repr__(self):
        return "cursor"


class FakeConnection:
    def __init__(self, logger):
        self._logger = logger
        self._cursor = FakeCursor(logger, self)

    def cursor(self):
        return self._cursor

    def __getattr__(self, item):
        return FakeAttr(self._logger, self, item)

    def __repr__(self):
        return "connection"


class SQLIndexProvider(ABC):
    """
    A index provider
    """

    @abstractmethod
    def get_indices(self, fields: Iterable[SQLField]) -> Iterable[SQLIndex]:
        """
        :param fields: the fields of the table
        :return: the indices to create
        """
        pass


class EmptySQLIndexProvider(SQLIndexProvider):
    """
    A index provider
    """

    def get_indices(self, fields: Iterable[SQLField]) -> Iterable[SQLIndex]:
        return []


class SQLTypeConverter(ABC, Generic[U]):
    @abstractmethod
    def get_type(self, item: U) -> SQLType:
        """
        :param item: the input
        :return: the sql type
        """
        pass


class DefaultSQLTypeConverter(SQLTypeConverter[Any]):
    def get_type(self, item: Any) -> SQLType:
        return SQLTypes.TEXT
