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
from abc import ABC, abstractmethod
from datetime import datetime, date
from decimal import Decimal
from typing import Any, Callable, Optional


class SQLType(ABC):
    @abstractmethod
    def to_str(self, *args) -> str:
        """
        :return: the representation of the type
        """
        pass

    @abstractmethod
    def type_value(self, value: str) -> Any:
        """
        Type a value
        :param value: the value
        :return: the typed value
        """
        pass


class BasicSQLType(SQLType):
    """
    INT, INTEGER, TINYINT...
    """

    def __init__(self, name: str, typer: Optional[Callable[[str], Any]] = None,
                 suffix: Optional[str] = None):
        self._name = name
        self._typer = typer
        self._suffix = suffix

    def to_str(self, *args) -> str:
        if args:
            if self._suffix is None:
                return f"{self._name}({', '.join(args)})"
            else:
                return f"{self._name}({', '.join(args)}) {self._suffix}"
        else:
            if self._suffix is None:
                return f"{self._name}"
            else:
                return f"{self._name} {self._suffix}"

    def type_value(self, value: str) -> Any:
        if self._typer is None:
            return value
        else:
            return self._typer(value)

    def __repr__(self):
        return f"BasicSQLType('{self._name}', {self._typer}, {self._suffix})"


def type_boolean(value: str) -> bool:
    if value.casefold().strip() in ("true", "yes", "on", "1"):
        return True
    elif value.casefold().strip() in ("false", "no", "off", "0"):
        return False
    else:
        raise ValueError(f"'{value}' is not a bool")


def timestamp_value(value):
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def timestamp_z_value(value):
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S%Z")


def date_value(value):
    return date.strptime(value, "%Y-%m-%d")


def time_value(value):
    return datetime.strptime(value, "%H:%M:%S")


def time_z_value(value):
    return datetime.strptime(value, "%H:%M:%S%Z")


class SQLTypes:
    SMALLINT = BasicSQLType("smallint", int)
    INTEGER = BasicSQLType("integer", int)
    BIGINT = BasicSQLType("bigint", int)
    DECIMAL = BasicSQLType("decimal", Decimal)
    NUMERIC = BasicSQLType("numeric", Decimal)
    REAL = BasicSQLType("real", float)
    DOUBLE_PRECISON = BasicSQLType("double precision", float)
    SERIAL = BasicSQLType("serial", int)
    BIGSERIAL = BasicSQLType("bigserial", int)

    MONEY = BasicSQLType("money", Decimal)

    CHARACTER_VARYING = BasicSQLType("character varying")
    VARCHAR = BasicSQLType("varchar")
    CHARACTER = BasicSQLType("character")
    CHAR = BasicSQLType("char")
    TEXT = BasicSQLType("text")

    BYTEA = BasicSQLType("bytea")

    TIMESTAMP = BasicSQLType("timestamp", timestamp_value)
    TIMESTAMP_WITHOUT_TIME_ZONE = BasicSQLType("timestamp", timestamp_value,
                                               "without time zone")
    TIMESTAMP_WITH_TIME_ZONE = BasicSQLType("timestamp", timestamp_z_value,
                                            "with time zone")
    DATE = BasicSQLType("date", date_value)
    TIME = BasicSQLType("time", time_value)
    TIME_WITHOUT_TIME_ZONE = BasicSQLType("time", time_value,
                                          "without time zone")
    TIME_WITH_TIME_ZONE = BasicSQLType("time", time_z_value, "with time zone")
    INTERVAL = None,  # not implemented

    BOOLEAN = BasicSQLType("boolean", type_boolean)


class SQLIndexType(ABC):
    """
    A SQL index type
    """

    def to_str(self, *args):
        pass


class BasicSQLIndexType(SQLIndexType):
    def __init__(self, type_str):
        self._type_str = type_str

    def to_str(self):
        return self._type_str


class SQLIndexTypes:
    """
    A SQL index type
    """
    B_TREE = BasicSQLIndexType("btree")
    HASH = BasicSQLIndexType("hash")
    GIST = BasicSQLIndexType("gist")
    SP_GIST = BasicSQLIndexType("spgist")
    GIN = BasicSQLIndexType("gin")
