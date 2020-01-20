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
#
#   This file is part of Sirene2pg.

"""
A small utility to import the SIRENE database
(https://www.data.gouv.fr/fr/datasets/
base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret).
"""
import csv
import logging
import zipfile
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Mapping, Iterable, Any, Optional, Tuple, Iterator, \
    Callable, Sequence

logging.basicConfig(level=logging.DEBUG)


###############################################################
#  POSTGRES: these classes are independent of the SIRENE data #
###############################################################
class SQLType(Enum):
    """
    A SQL type
    """
    TEXT = "text"
    DATE = "date"
    NUMERIC = "numeric"
    BOOLEAN = "boolean"


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

    def process(self, process_names: Callable[[str], str]) -> "SQLField":
        return SQLField(process_names(self.table_name),
                        process_names(self.field_name), self.type, self.rank,
                        self.comment, self.length)

    def __lt__(self, other):
        return (self.table_name < other.table_name
                or self.rank < other.rank)


class SQLIndexType(Enum):
    """
    A SQL index type
    """
    B_TREE = "btree"
    HASH = "hash"
    GIST = "gist"
    SP_GIST = "spgist"
    GIN = "gin"


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
    def name(self):
        """
        :return: the name of the index, that is field name + table name + "idx"
        """
        return '{field}_{table}_idx'.format(table=self.table_name,
                                            field=self.field_name)


class QueryProvider:
    """
    A SQL query provider
    """

    @abstractmethod
    def create_table(self) -> Iterable[str]:
        """
        :return: a list of queries to create a table
        """
        pass

    @abstractmethod
    def prepare_copy(self) -> Iterable[str]:
        """
        :return: a list of queries to prepare a copy
        """
        pass

    @abstractmethod
    def copy(self) -> Iterable[str]:
        """
        :return: a list of queries to perform a copy
        """
        pass

    @abstractmethod
    def finalize_copy(self) -> Iterable[str]:
        """
        :return: a list of queries to finalize a copy
        """
        pass


class PostgreSQLQueryProvider(QueryProvider):
    """
    A provider for PostgreSQL queries.
    """

    def __init__(self, table_name: str, fields: Sequence[SQLField],
                 indices: Iterable[SQLIndex], sql_type_width: int,
                 name_width: int):
        """
        :param table_name: *processed* name of the table
        :param fields: fields with *processed* names
        :param indices: indices with *processed* names
        :param sql_type_width: argument to rjust
        :param name_width: argument to rjust
        """
        self._table_name = table_name
        self._fields = fields
        self._indices = indices
        self._sql_type_width = sql_type_width
        self._name_width = name_width

    def create_table(self) -> Iterable[str]:
        return 'DROP TABLE IF EXISTS {}'.format(
            self._table_name), self._create()

    def _create(self) -> str:
        lines = ['CREATE TABLE {} ('.format(self._table_name)]
        for field in self._fields[:-1]:
            lines.append(self._create_line(field, ','))
        field = self._fields[-1]
        lines.append(self._create_line(field, ' '))
        lines.append(")")
        return "\n".join(lines)

    def _create_line(self, field: SQLField, comma: str):
        return "    {} {} -- {}".format(self._field_name(field),
                                        self._type(field, comma),
                                        field.comment)

    def _field_name(self, field: SQLField) -> str:
        return field.field_name.ljust(self._name_width)

    def _type(self, field: SQLField, comma=','):
        return (field.type.value + comma).ljust(
            self._sql_type_width + len(comma))

    def prepare_copy(self) -> Iterable[str]:
        return 'TRUNCATE {}'.format(self._table_name),

    def copy(self) -> Iterable[str]:
        return "COPY {} FROM STDIN CSV HEADER " \
               "DELIMITER ',' ENCODING 'UTF-8'".format(self._table_name),

    def finalize_copy(self) -> Iterable[str]:
        return ('ANALYZE {}'.format(self._table_name),) + tuple(
            self._add_index(index) for index in self._indices)

    @staticmethod
    def _add_index(index: SQLIndex) -> str:
        return 'CREATE INDEX {idx_name} ON {table} ' \
               'USING {idx_type}({field})'.format(idx_name=index.name,
                                                  table=index.table_name,
                                                  idx_type=index.type.value,
                                                  field=index.field_name)


class QueryExecutor(ABC):
    @abstractmethod
    def execute_all(self, operations: Iterable[str], args=None, stream=None):
        """
        Execute all SQL queries
        :param operations: queries
        :param args: args as in pg8000 args
        :param stream: for copy
        """
        pass

    @abstractmethod
    def commit(self):
        """
        commit the transaction
        """
        pass


class NormalQueryExecutor(QueryExecutor):
    """
    Execute and log the queries
    """

    def __init__(self, logger: logging.Logger, connection: Any):
        self._logger = logger
        self._connection = connection

    def execute_all(self, operations: Iterable[str], args=None, stream=None):
        cursor = self._connection.cursor()
        for sql in operations:
            self._logger.debug("Execute: %s (args=%s, stream=%s)", sql, args,
                               stream)
            cursor.execute(sql, args, stream)

    def commit(self):
        self._logger.debug("Commit")
        self._connection.commit()


class DryRunQueryExecutor(QueryExecutor):
    """
    Don't really execute the queries
    """

    def __init__(self, logger: logging.Logger):
        self._logger = logger

    def execute_all(self, operations: Iterable[str], args=None, stream=None):
        for sql in operations:
            self._logger.debug(">>> Dry run: %s (args=%s, stream=%s)", sql,
                               args,
                               stream)

    def commit(self):
        self._logger.debug(">>> Dry run: Commit")


###############################################################
#  SIRENE: these classes represent the SIRENE data and schema #
###############################################################
ZIP_SUFFIX = "_utf8"

NAME = "Nom"
LENGTH = 'Longueur'
CAPTION = 'Libellé'
TYPE = 'Type'
RANK = 'Ordre'


@dataclass
class Source:
    """
    A SIRENE source
    """
    table_name: str
    data_path: Path
    schema_path: Path


@dataclass
class SchemaRow:
    """
    A row as in the schema.
    No snake_case conversion
    """
    table_name: str
    name: str
    type_name: str
    length: int
    rank: int
    caption: str


class SQLTypeProvider(ABC):
    @abstractmethod
    def get_type(self, row: SchemaRow) -> SQLType:
        """
        :param row: the input row
        :return: the sql type
        """
        pass


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


class SireSQLIndexProvider(SQLIndexProvider):
    """
    Provide index for SIREN/SIRET and code postal
    """

    def __init__(self, *extra_indices: SQLIndex):
        """
        :param extra_indices: indices to add
        """
        self._extra_indices = extra_indices

    def get_indices(self, fields: Iterable[SQLField]) -> Iterable[SQLIndex]:
        table_name = next((f.table_name for f in fields), None)
        for field in fields:
            if field.field_name[:5] in (
                    "siren", "siret"):
                yield SQLIndex(field.table_name, field.field_name,
                               SQLIndexType.HASH)

        for index in self._extra_indices:
            if index.table_name == table_name:
                yield index


class SireneSchemaParser:
    """
    A parser for SIRENE csv schemas.
    """

    def __init__(self, table_name: str, rows: Iterable[Mapping[str, str]],
                 type_provider: SQLTypeProvider,
                 index_provider: SQLIndexProvider):
        """
        :param table_name: the table name
        :param rows: rows of the csv file
        :param type_provider: a provider for sql types
        :param index_provider: a provider for indices
        """
        self._table_name = table_name
        self._rows = [SchemaRow(table_name, row[NAME],
                                row[TYPE], int(row[LENGTH]),
                                int(row[RANK]), row[CAPTION]) for row in rows]
        self._type_provider = type_provider
        self._index_provider = index_provider

    def get_fields(self):
        """
        :return: orderd fields of the table (names are in snake_case)
        """
        return sorted(
            SQLField(table_name=self._table_name, field_name=row.name,
                     type=self._type_provider.get_type(row), rank=row.rank,
                     length=row.length,
                     comment=row.caption)
            for row in self._rows)

    def get_indices(self) -> Iterable[SQLIndex]:
        """
        :return: indices to create for this table
        """
        for index in self._index_provider.get_indices(self.get_fields()):
            if index.table_name == self._table_name:
                yield index

    @property
    def table_name(self):
        return self._table_name


def data_sources(sirene_path: Path) -> Iterator[Source]:
    """
    :param sirene_path: the path of the files
    :return: the data sources
    """
    for data_path in sirene_path.glob("*" + ZIP_SUFFIX + ".zip"):
        table_name = data_path.stem[:-len(ZIP_SUFFIX)]
        schema_path = Path(sirene_path,
                           "dessin" + table_name.lower() + ".csv")
        yield Source(table_name, data_path, schema_path)


###########################################################
# SIRENE - POSTGRES: the glue between SIRENE and Postgres #
###########################################################

SQL_TYPE_BY_SIREN_TYPE = {
    "Liste de codes": SQLType.TEXT,
    "Date": SQLType.DATE,
    "Texte": SQLType.TEXT,
    "Numérique": SQLType.NUMERIC,
}


class PatchedSQLTypeProvider(SQLTypeProvider):
    def __init__(self, sql_type_by_sirene_type: Mapping[str, SQLType]):
        """
        :param sql_type_by_sirene_type: the default mapping
        """
        self._sql_type_by_sirene_type = sql_type_by_sirene_type

    def get_type(self, row: SchemaRow) -> SQLType:
        t = self._sql_type_by_sirene_type[row.type_name]
        if t == SQLType.DATE and row.length != 10:
            t = SQLType.TEXT

        if (row.table_name == 'StockEtablissement'
                and row.name == 'numeroVoieEtablissement'):
            t = SQLType.TEXT
        return t


class BasicSQLTypeProvider(SQLTypeProvider):
    def __init__(self, sql_type_by_sirene_type: Mapping[str, SQLType]):
        """
        :param sql_type_by_sirene_type: the default mapping
        """
        self._sql_type_by_sirene_type = sql_type_by_sirene_type

    def get_type(self, row: SchemaRow) -> SQLType:
        return self._sql_type_by_sirene_type[row.type_name]


def default_force_type(table_name: str, field_name: str) -> Optional[SQLType]:
    """
    :param table_name: the table
    :param field_name: the field
    :return: true if the field should be a text field
    """
    if (table_name == 'StockEtablissement'
            and field_name == 'numeroVoieEtablissement'):
        return SQLType.TEXT

    return None


class SireneImporter:
    """
    The importer
    """

    def __init__(self, logger: logging.Logger, sources: Iterable[Source],
                 type_provider: SQLTypeProvider,
                 index_provider: SQLIndexProvider,
                 process_names: Callable[[str], str]):
        """
        :param logger: the logger
        :param sources: the SIRENE sources
        :param type_provider: a function `field_name, table_name -> None or
                            a SQLType`
        :param process_names: a function to convert field and table names
        :param index_provider: a index provider
        """
        self._logger = logger
        self._sources = sources
        self._type_provider = type_provider
        self._index_provider = index_provider
        self._process_names = process_names

    def execute(self, executor: QueryExecutor):
        """
        Import the data
        :param executor: the executor
        """
        for source in self._sources:
            self._logger.debug("Process data file %s", source)
            try:
                parser = self._create_schema_parser(source)
                provider = self._create_provider(parser)
            except FileNotFoundError:
                self._logger.warning("CSV Schema not found: %s",
                                     source.schema_path)
                continue

            self._copy_data(source, provider, executor)

    def _create_schema_parser(self, source) -> SireneSchemaParser:
        with open(source.schema_path, 'r', encoding='utf-8') as schema:
            reader = csv.DictReader(schema)
            parser = SireneSchemaParser(source.table_name, reader,
                                        self._type_provider,
                                        self._index_provider)
        return parser

    def _create_provider(self,
                         schema_parser: SireneSchemaParser) -> QueryProvider:
        """
        Create a SQL provider.

        :param schema_parser: the parser for the schema
        :return: a new provider
        """
        table_name = self._process_names(schema_parser.table_name)
        fields = [field.process(self._process_names) for field in
                  schema_parser.get_fields()]
        indices = [index.process(self._process_names) for index in
                   schema_parser.get_indices()]
        sql_type_max_size = max(len(f.type.value) for f in fields)
        name_max_size = max(len(f.field_name) for f in fields)
        return PostgreSQLQueryProvider(table_name, fields,
                                       indices,
                                       sql_type_max_size,
                                       name_max_size)

    def _copy_data(self, source, provider, executor):
        self._logger.debug("CSV Schema found: %s", source.schema_path)
        self._logger.debug("Create table schema: %s", source.table_name)
        executor.execute_all(provider.create_table())
        self._logger.debug("Prepare copy: %s", source.table_name)
        executor.execute_all(provider.prepare_copy())
        self._logger.debug("Import data from file: %s", source.data_path)
        with zipfile.ZipFile(source.data_path, 'r') as zipdata:
            self._copy_first_entry(zipdata, provider, executor)
        self._logger.debug("After copy: %s", source.table_name)
        executor.execute_all(provider.finalize_copy())
        executor.commit()

    def _copy_first_entry(self, zipdata, provider, executor):
        f = zipdata.filelist[0]
        data_stream = zipdata.open(f)
        first_lines = [list(enumerate(data_stream.readline().strip().decode(
            "utf-8").split(","), 1)) for _ in range(3)]
        self._logger.debug("First lines are: %s", first_lines)
        data_stream.seek(0)
        executor.execute_all(provider.copy(), stream=data_stream)


########
# MISC #
########


def split_on_cat(text: str,
                 dont_split: Optional[
                     Iterable[Tuple[Optional[str], Optional[str]]]] = None
                 ) -> Iterable[str]:
    """
    Split the text on unicodedata differences. The default behavior ignores
    transitions from upper case to lower case:

    >>> list(split_on_cat("LoremIpsum"))
    ['Lorem', 'Ipsum']

    You can specifiy transisions to ignore in unicode categories. No transition
    ignored:
    >>> list(split_on_cat("LoremIpsum", ()))
    ['L', 'orem', 'I', 'psum']

    Ignore all transitions before a number:
    >>> list(split_on_cat("Lorem2Ipsum", ((None, "Nd"),)))
    ['L', 'orem2', 'I', 'psum']

    Ignore all transitions:
    >>> list(split_on_cat("LoremIpsum", ((None, None),)))
    ['LoremIpsum']

    :param text: the text
    :param dont_split: transitions (cat1, cat2) that are not a valid split
    :return: yield chunks of text
    """
    import unicodedata
    if dont_split is None:
        dont_split = (("Lu", "Ll"),)

    def split_between(lc, c):
        for ds_lc, ds_c in dont_split:
            if ((ds_lc is None or lc == ds_lc)
                    and (ds_c is None or c == ds_c)):
                return False

        return True

    previous_end = None
    last_cat = unicodedata.category(text[0])
    for i, cat in enumerate(map(unicodedata.category, text)):
        if cat != last_cat and split_between(last_cat, cat):
            yield text[previous_end:i]
            previous_end = i
        last_cat = cat
    yield text[previous_end:None]


def to_snake(text: str) -> str:
    """
    Converts a camel case text to snake case.

    >>> to_snake("LoremIpsum")
    'lorem_ipsum'
    >>> to_snake("Lorem2Ipsum")
    'lorem_2_ipsum'

    :param text: the camel case text
    :return: the snake case text
    """
    return '_'.join(split_on_cat(text)).lower()


def import_sirene(sirene_path: Path, *args,
                  type_provider: SQLTypeProvider = PatchedSQLTypeProvider(
                      SQL_TYPE_BY_SIREN_TYPE),
                  index_provider: SQLIndexProvider = SireSQLIndexProvider(
                      SQLIndex('StockEtablissement',
                               "codePostalEtablissement",
                               SQLIndexType.B_TREE)),
                  process_names: Optional[Callable[[str], str]] = to_snake,
                  dry_run: bool = False):
    """
    :param sirene_path: path to data and schemas
    :param type_provider: a provider for sql types
    :param args: a connection or nothing
    :param index_provider: a provider for indices
    :param process_names: a function to process field and table names
    :param dry_run: If true, only log the queries
    """
    logger = logging.getLogger("sirene2pg")
    if process_names is None:
        def process_names(n: str) -> str: return n

    importer = SireneImporter(logger, data_sources(sirene_path), type_provider,
                              index_provider, process_names)
    if dry_run:
        importer.execute(DryRunQueryExecutor(logger))
    else:
        importer.execute(NormalQueryExecutor(logger, args[0]))


########
# MAIN #
########
if __name__ == "__main__":
    import doctest

    doctest.testmod()
