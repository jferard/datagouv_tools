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
from csv import Dialect
from dataclasses import dataclass
from enum import Enum
from io import BytesIO
from itertools import chain
from pathlib import Path
from typing import Callable, Iterable, Generic, Sequence, TypeVar, Any, Mapping, \
    BinaryIO

U = TypeVar('U')


class SQLType(ABC):
    @abstractmethod
    def type_str(self, params: Mapping[str, Any]) -> str:
        """
        :return: the representation of the type
        """
        pass


T = TypeVar('T', bound=SQLType)


@dataclass(eq=True)
class SQLField(Generic[T]):
    """
    A SQL field
    """
    table_name: str  # original field name
    field_name: str
    type: T
    rank: int = 0
    comment: str = ""
    length: int = 0
    type_params: Mapping[str, Any] = None

    def __post_init__(self):
        if self.type_params is None:
            self.type_params = {}

    def process(self, process_names: Callable[[str], str]) -> "SQLField[T]":
        return SQLField(process_names(self.table_name),
                        process_names(self.field_name), self.type, self.rank,
                        self.comment, self.length)

    @property
    def type_str(self):
        return self.type.type_str(self.type_params)

    def __lt__(self, other):
        if self.table_name != other.table_name:
            raise ValueError("field from different tables are not comparable")
        return self.rank < other.rank


F = TypeVar('F', bound=SQLField)


class SQLIndexType(ABC):
    @property
    @abstractmethod
    def type_str(self):
        pass


IT = TypeVar('IT', bound=SQLIndexType)


@dataclass(eq=True)
class SQLIndex(Generic[IT]):
    """
    A SQL index
    """
    table_name: str
    field_name: str
    type: IT

    def process(self, process_names: Callable[[str], str]) -> "SQLIndex[IT]":
        return SQLIndex(process_names(self.table_name),
                        process_names(self.field_name), self.type)

    @property
    def name(self) -> str:
        """
        :return: the name of the index, that is field name + table name + "idx"
        """
        return f'{self.field_name}_{self.table_name}_idx'

    @property
    def type_str(self) -> str:
        return self.type.type_str


IN = TypeVar('IN', bound=SQLIndex)


class QueryProvider(ABC, Generic[F, IN]):
    """
    A SQL query provider
    """

    def drop_table(self, table_name: str) -> Iterable[str]:
        return f'DROP TABLE IF EXISTS {table_name}',

    def create_table(self, table_name: str, fields: Iterable[F]
                     ) -> Iterable[str]:
        """
        :return: a list of queries to create a table
        """
        if not fields:
            return f'CREATE TABLE {table_name} ()',

        sql_type_max_size = max(len(f.type_str) for f in fields)
        name_max_size = max(len(f.field_name) for f in fields)

        lines = [f'CREATE TABLE {table_name} (']
        for field in fields[:-1]:
            lines.append(
                self._create_line(field, name_max_size, sql_type_max_size, ','))
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

    def copy_all(self, table_name: str, field_count: int):
        question_marks = ", ".join(["?"] * field_count)
        return f'INSERT INTO {table_name} VALUES({question_marks})'

    def prepare_copy(self, table_name: str) -> Iterable[str]:
        """
        :return: a list of queries to prepare a copy
        """
        return ()

    def finalize_copy(self, table_name: str) -> Iterable[str]:
        """
        :return: a list of queries to finalize a copy
        """
        return ()

    def create_index(self, table_name: str, index: IN) -> Iterable[str]:
        return f'CREATE INDEX {index.name} ON {table_name}({index.field_name})',


class QueryExecutor(ABC, Generic[F, IN]):
    @abstractmethod
    def execute(self, queries: Iterable[str], *args, **kwargs):
        pass

    @abstractmethod
    def executemany(self, query: str, *args, **kwargs):
        pass

    @abstractmethod
    def commit(self):
        pass

    @property
    @abstractmethod
    def query_provider(self) -> QueryProvider:
        pass

    def create_table(self, table_name: str, fields: Sequence[F]):
        """
        create a new table

        :param table_name: the name of the table
        :param fields: the fields
        """
        self.execute(self.query_provider.drop_table(table_name))
        self.execute(self.query_provider.create_table(table_name, fields))

    def prepare_copy(self, table_name: str):
        """
        create a new table
        """
        self.execute(self.query_provider.prepare_copy(table_name))

    def copy_path(self, table_name: str, path: Path, encoding: str,
                  dialect: Dialect, count=0):
        """
        create a new table
        """
        if count > 1:
            raise Exception(
                "An executor should implement copy_stream or copy_path")
        with path.open("rb") as source:
            self.copy_stream(table_name, source, encoding, dialect,
                             count=count + 1)

    def copy_stream(self, table_name: str, stream: BinaryIO, encoding: str,
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

        self.copy_path(table_name, Path(temp_path), encoding, dialect,
                       count=count + 1)

    def copy_all(self, table_name: str, field_count: int,
                 records: Iterable[Iterable[str]]):
        self.executemany()
        pass
        # TODO: code me

    def finalize_copy(self, table_name: str):
        """
        create a new table
        """
        self.execute(self.query_provider.finalize_copy(table_name))

    def create_indices(self, table_name: str, indices: Iterable[IN]):
        """
        create a new table
        """
        for index in indices:
            self.execute(self.query_provider.create_index(table_name, index))


class FakeAttr:
    def __init__(self, obj, item):
        self._obj = obj
        self._item = item

    def __call__(self, *args, **kwargs):
        arg_str = ', '.join(chain(
            map(str, args),
            (f"{k}={v}" for k, v in kwargs.items())
        ))
        print(f"Call {self._obj}.{self._item}({arg_str})")


class FakeCursor:
    def __init__(self, connection):
        self._connection = connection

    def __getattr__(self, item):
        return FakeAttr(self, item)

    def __repr__(self):
        return "cursor"


class FakeConnection:
    def __init__(self):
        self._cursor = FakeCursor(self)

    def cursor(self):
        return self._cursor

    def __getattr__(self, item):
        return FakeAttr(self, item)

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


class SQLTypeConverter(ABC, Generic[U]):
    @abstractmethod
    def get_type(self, item: U) -> SQLType:
        """
        :param item: the input
        :return: the sql type
        """
        pass
