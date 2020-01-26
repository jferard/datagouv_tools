# coding: utf-8

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
"""
A small utility to import the SIRENE database
(https://www.data.gouv.fr/fr/datasets/
base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret).
"""

import csv
import logging
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import (Mapping, Iterable, Any, Optional, Iterator,
                    Callable, TypeVar)

from datagouv_tools.sql.generic import SQLField, SQLIndex, QueryProvider, \
    QueryExecutor, FakeConnection, SQLIndexProvider, SQLTypeConverter, SQLType
from datagouv_tools.sql.mariadb import MariaDBQueryProvider, \
    MariaDBQueryExecutor
from datagouv_tools.sql.postgresql import (PostgreSQLType, PostgreSQLIndexType,
                                           PostgreSQLQueryProvider,
                                           PostgreSQLQueryExecutor)
from datagouv_tools.sql.sqlite import SQLiteQueryExecutor
from datagouv_tools.util import to_snake

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s/%(filename)s/%(funcName)s/%(lineno)d - %(levelname)s: %(message)s")


###############################################################
#  POSTGRES: these classes are independent of the SIRENE data #
###############################################################


class NormalQueryExecutor:
    """
    Execute and log the queries
    """

    def __init__(self, logger: logging.Logger, connection: Any):
        self._logger = logger
        self._connection = connection

    def execute_all(self, operations: Iterable[str], *args, **kwargs):
        cursor = self._connection.cursor()
        for sql in operations:
            self._logger.debug("Execute: %s (args=%s, stream=%s)", sql, args,
                               kwargs)
            cursor.execute(sql, *args, **kwargs)

    def commit(self):
        self._logger.debug("Commit")
        self._connection.commit()


class DryRunQueryExecutor:
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
    zipped_data_path: Path
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


class SireneSchemaParser:
    """
    A parser for SIRENE csv schemas.
    """

    def __init__(self, table_name: str, rows: Iterable[Mapping[str, str]],
                 type_provider: SQLTypeConverter,
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

class SireneSQLIndexProvider(SQLIndexProvider):
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
                               PostgreSQLIndexType.HASH)

        for index in self._extra_indices:
            if index.table_name == table_name:
                yield index


POSTGRESQL_TYPE_BY_SIREN_TYPE = {
    "Liste de codes": PostgreSQLType.TEXT,
    "Date": PostgreSQLType.DATE,
    "Texte": PostgreSQLType.TEXT,
    "Numérique": PostgreSQLType.NUMERIC,
}


class PatchedPostgreSireneTypeToSQLTypeConverter(SQLTypeConverter[SchemaRow]):
    def __init__(self, sql_type_by_sirene_type: Mapping[str, SQLType]):
        """
        :param sql_type_by_sirene_type: the default mapping
        """
        self._sql_type_by_sirene_type = sql_type_by_sirene_type

    def get_type(self, row: SchemaRow) -> SQLType:
        t = self._sql_type_by_sirene_type[row.type_name]
        if t == PostgreSQLType.DATE and row.length != 10:
            t = PostgreSQLType.TEXT

        if (row.table_name == 'StockEtablissement'
                and row.name == 'numeroVoieEtablissement'):
            t = PostgreSQLType.TEXT
        return t


class BasicSireneTypeToSQLTypeConverter(SQLTypeConverter[SchemaRow]):
    def __init__(self, sql_type_by_sirene_type: Mapping[str, PostgreSQLType]):
        """
        :param sql_type_by_sirene_type: the default mapping
        """
        self._sql_type_by_sirene_type = sql_type_by_sirene_type

    def get_type(self, row: SchemaRow) -> PostgreSQLType:
        return self._sql_type_by_sirene_type[row.type_name]


################
# The importer #
################

class SireneImporter:
    """
    The importer
    """

    def __init__(self, logger: logging.Logger, sources: Iterable[Source],
                 type_provider: SQLTypeConverter[SchemaRow],
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
            except FileNotFoundError:
                self._logger.warning("CSV Schema not found: %s",
                                     source.schema_path)
                continue

            self._copy_data(source, parser, executor)

    def _create_schema_parser(self, source: Source) -> SireneSchemaParser:
        with source.schema_path.open('r', encoding='utf-8') as schema:
            reader = csv.DictReader(schema)
            parser = SireneSchemaParser(source.table_name, reader,
                                        self._type_provider,
                                        self._index_provider)
        return parser

    def _copy_data(self, source: Source, parser: SireneSchemaParser,
                   executor: QueryExecutor):
        fields = [field.process(self._process_names) for field in
                  parser.get_fields()]
        table_name = self._process_names(parser.table_name)
        indices = [index.process(self._process_names) for index in
                   parser.get_indices()]

        self._logger.debug("CSV Schema found: %s", source.schema_path)
        self._logger.debug("Create table schema: %s", table_name)
        executor.create_table(table_name, fields)
        self._logger.debug("Prepare copy: %s", source.table_name)
        executor.prepare_copy(table_name)
        self._logger.debug("Import data from file: %s",
                           source.zipped_data_path)
        self._copy_from_zipped_file(source.zipped_data_path, parser, executor)
        self._logger.debug("After copy: %s", source.table_name)
        executor.finalize_copy(table_name)
        executor.create_indices(table_name, indices)
        executor.commit()

    def _copy_from_zipped_file(self, zipped_path: Path,
                               parser: SireneSchemaParser,
                               executor: QueryExecutor):
        # TODO: copier (PgCopier, MariaDBCopier, SqliteCopier)
        with zipfile.ZipFile(zipped_path, 'r') as zipdata:
            data_stream = self._get_stream(zipdata)
            table_name = self._process_names(parser.table_name)
            executor.copy_stream(table_name, data_stream, "utf-8",
                                 csv.unix_dialect)

    def _get_stream(self, zipdata):
        f = zipdata.filelist[0]
        data_stream = zipdata.open(f)
        first_lines = [list(enumerate(data_stream.readline().strip().decode(
            "utf-8").split(","), 1)) for _ in range(3)]
        self._logger.debug("First lines are: %s", first_lines)
        data_stream.seek(0)
        return data_stream


########
# MISC #
########

def import_sirene(sirene_path: Path, connection: Any, rdbms: str,
                  process_names: Optional[Callable[[str], str]] = to_snake):
    assert sirene_path.exists()
    logger = logging.getLogger("datagouv_tools")

    if connection is None:
        connection = FakeConnection(logger)
    if process_names is None:
        def process_names(name: str) -> str: return name

    logger.debug("Import data with following parameters:"
                 "sirene_path: %s, connection: %s, rdbms: %s", sirene_path,
                 connection, rdbms)

    if rdbms.casefold() == "postgresql":
        type_converter = PatchedPostgreSireneTypeToSQLTypeConverter(
            POSTGRESQL_TYPE_BY_SIREN_TYPE)
        index_provider = SireneSQLIndexProvider(
            SQLIndex('StockEtablissement', "codePostalEtablissement",
                     PostgreSQLIndexType.B_TREE))
        query_executor = PostgreSQLQueryExecutor(logger, connection,
                                                 PostgreSQLQueryProvider())
    elif rdbms.casefold() == "sqlite":
        type_converter = PatchedPostgreSireneTypeToSQLTypeConverter(
            POSTGRESQL_TYPE_BY_SIREN_TYPE)
        index_provider = SireneSQLIndexProvider(
            SQLIndex('StockEtablissement', "codePostalEtablissement",
                     PostgreSQLIndexType.B_TREE))
        query_executor = SQLiteQueryExecutor(logger, connection,
                                             QueryProvider())
    elif rdbms.casefold() == "mariadb":
        type_converter = PatchedPostgreSireneTypeToSQLTypeConverter(
            POSTGRESQL_TYPE_BY_SIREN_TYPE)
        index_provider = SireneSQLIndexProvider(
            SQLIndex('StockEtablissement', "codePostalEtablissement",
                     PostgreSQLIndexType.B_TREE))
        query_executor = MariaDBQueryExecutor(logger, connection,
                                              MariaDBQueryProvider())

    _import_sirene(logger, sirene_path, type_converter, index_provider,
                   query_executor, process_names)


def _import_sirene(logger: logging.Logger, sirene_path: Path,
                   type_converter: SQLTypeConverter[SchemaRow],
                   index_provider: SQLIndexProvider,
                   query_executor: QueryExecutor,
                   process_names: Callable[[str], str]):
    """
    :param sirene_path: path to data and schemas
    :param type_converter: a provider for sql types
    :param index_provider: a provider for indices
    :param query_executor: the executor
    :param process_names: a function to process field and table names
    """

    logger.debug("Import data with following parameters:"
                 "sirene_path: %s, type_converter: %s, index_provider: %s, "
                 "query_executor: %s, process_names: %s", sirene_path,
                 type_converter, index_provider,
                 query_executor, process_names)

    importer = SireneImporter(logger, data_sources(sirene_path), type_converter,
                              index_provider, process_names)
    importer.execute(query_executor)


########
# MAIN #
########
if __name__ == "__main__":
    import doctest

    doctest.testmod()
