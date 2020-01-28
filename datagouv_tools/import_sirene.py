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
                    Callable)

from datagouv_tools.import_generic import ImporterContext
from datagouv_tools.sql.generic import (SQLField, SQLIndex, QueryProvider,
                                        QueryExecutor, FakeConnection,
                                        SQLIndexProvider, SQLTypeConverter,
                                        SQLTable)
from datagouv_tools.sql.mariadb import (MariaDBQueryProvider,
                                        MariaDBQueryExecutor)
from datagouv_tools.sql.postgresql import (PostgreSQLQueryProvider,
                                           PostgreSQLQueryExecutor)
from datagouv_tools.sql.sql_type import SQLType, SQLTypes, SQLIndexTypes
from datagouv_tools.sql.sqlite import SQLiteQueryExecutor
from datagouv_tools.util import to_snake

logging.basicConfig(level=logging.DEBUG,
                    format=("%(asctime)s - %(name)s/%(filename)s/%(funcName)s/"
                            "%(lineno)d - %(levelname)s: %(message)s"))


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
                 type_converter: SQLTypeConverter,
                 index_provider: SQLIndexProvider):
        """
        :param table_name: the table name
        :param rows: rows of the csv file
        :param type_converter: a provider for sql types
        :param index_provider: a provider for indices
        """
        self._table_name = table_name
        self._rows = [SchemaRow(table_name, row[NAME],
                                row[TYPE], int(row[LENGTH]),
                                int(row[RANK]), row[CAPTION]) for row in rows]
        self._type_converter = type_converter
        self._index_provider = index_provider

    def get_fields(self) -> Iterable[SQLField]:
        """
        :return: orderd fields of the table (names are in snake_case)
        """
        return sorted(
            SQLField(table_name=self._table_name, field_name=row.name,
                     type=self._type_converter.get_type(row), rank=row.rank,
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

    def get_table(self, process_names: Callable[[str], str]
                  ) -> SQLTable:
        return SQLTable(
            name=process_names(self._table_name),
            fields=[field.process(process_names) for field in
                    self.get_fields()],
            indices=[index.process(process_names) for index in
                     self.get_indices()]

        )


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
                               SQLIndexTypes.HASH)

        for index in self._extra_indices:
            if index.table_name == table_name:
                yield index


SQL_TYPE_BY_SIRENE_TYPE = {
    "Liste de codes": SQLTypes.TEXT,
    "Date": SQLTypes.DATE,
    "Texte": SQLTypes.TEXT,
    "Numérique": SQLTypes.NUMERIC,
}


class PatchedPostgreSireneTypeToSQLTypeConverter(SQLTypeConverter[SchemaRow]):
    def __init__(self, sql_type_by_sirene_type: Mapping[str, SQLType]):
        """
        :param sql_type_by_sirene_type: the default mapping
        """
        self._sql_type_by_sirene_type = sql_type_by_sirene_type

    def get_type(self, row: SchemaRow) -> SQLType:
        t = self._sql_type_by_sirene_type[row.type_name]
        if t == SQLTypes.DATE and row.length != 10:
            t = SQLTypes.TEXT

        if (row.table_name == 'StockEtablissement'
                and row.name == 'numeroVoieEtablissement'):
            t = SQLTypes.TEXT
        return t


class BasicSireneTypeToSQLTypeConverter(SQLTypeConverter[SchemaRow]):
    def __init__(self,
                 sql_type_by_sirene_type: Mapping[str, SQLType]):
        """
        :param sql_type_by_sirene_type: the default mapping
        """
        self._sql_type_by_sirene_type = sql_type_by_sirene_type

    def get_type(self, row: SchemaRow) -> SQLType:
        return self._sql_type_by_sirene_type[row.type_name]


################
# The importer #
################

class SireneImporter:
    """
    The importer
    """

    def __init__(self, logger: logging.Logger, sources: Iterable[Source],
                 importer_context: ImporterContext,
                 process_names: Callable[[str], str], bulk_copy: bool):
        """
        :param logger: the logger
        :param sources: the SIRENE sources
        :param process_names: a function to convert field and table names
        :param bulk_copy: if True, use bulk copy if available
        """
        self._logger = logger
        self._sources = sources
        self._importer_context = importer_context
        self._process_names = process_names
        self._bulk_copy = bulk_copy

    def execute(self):
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

            self._copy_data(source, parser)

    def _create_schema_parser(self, source: Source) -> SireneSchemaParser:
        with source.schema_path.open('r', encoding='utf-8') as schema:
            reader = csv.DictReader(schema)
            parser = SireneSchemaParser(source.table_name, reader,
                                        self._importer_context.type_converter,
                                        self._importer_context.index_provider)
        return parser

    def _copy_data(self, source: Source, parser: SireneSchemaParser):
        table = parser.get_table(self._process_names)

        executor = self._importer_context.query_executor
        self._logger.debug("CSV Schema found: %s", source.schema_path)
        self._logger.debug("Create table schema: %s", table.name)
        executor.create_table(table)
        self._logger.debug("Prepare copy: %s", source.table_name)
        executor.prepare_copy(table)
        self._logger.debug("Import data from file: %s",
                           source.zipped_data_path)
        self._copy_from_zipped_file(source.zipped_data_path, parser,
                                    executor)
        self._logger.debug("After copy: %s", source.table_name)
        executor.finalize_copy(table)
        executor.create_indices(table)
        executor.commit()

    def _copy_from_zipped_file(self, zipped_path: Path,
                               parser: SireneSchemaParser,
                               executor: QueryExecutor):
        # TODO: copier (PgCopier, MariaDBCopier, SqliteCopier)
        with zipfile.ZipFile(zipped_path, 'r') as zipdata:
            data_stream = self._get_stream(zipdata)
            table = parser.get_table(self._process_names)
            if self._bulk_copy:
                executor.copy_stream(table, data_stream, "utf-8",
                                     csv.unix_dialect)
            else:
                executor.insert_all(table, data_stream, "utf-8",
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


def postgres_context(logger, connection):
    return ImporterContext(
        query_executor=PostgreSQLQueryExecutor(logger, connection,
                                               PostgreSQLQueryProvider()),
        type_converter=PatchedPostgreSireneTypeToSQLTypeConverter(
            SQL_TYPE_BY_SIRENE_TYPE),
        index_provider=SireneSQLIndexProvider(
            SQLIndex('StockEtablissement', "codePostalEtablissement",
                     SQLIndexTypes.B_TREE)),
    )


def sqlite_context(logger, connection):
    return ImporterContext(
        query_executor=SQLiteQueryExecutor(logger, connection,
                                           QueryProvider()),
        type_converter=PatchedPostgreSireneTypeToSQLTypeConverter(
            SQL_TYPE_BY_SIRENE_TYPE),
        index_provider=SireneSQLIndexProvider(
            SQLIndex('StockEtablissement', "codePostalEtablissement",
                     SQLIndexTypes.B_TREE)),
    )


def mariadb_context(logger, connection):
    return ImporterContext(
        query_executor=MariaDBQueryExecutor(logger, connection,
                                            MariaDBQueryProvider()),
        type_converter=PatchedPostgreSireneTypeToSQLTypeConverter(
            SQL_TYPE_BY_SIRENE_TYPE),
        index_provider=SireneSQLIndexProvider(
            SQLIndex('StockEtablissement', "codePostalEtablissement",
                     SQLIndexTypes.B_TREE)),
    )


_SIRENE_CONTEXT_FACTORY_BY_RDBMS = {}


def register(importer_context_factory: Callable[
    [logging.Logger, Any], ImporterContext],
             *rdbms_list: str):
    for rdbms in rdbms_list:
        _SIRENE_CONTEXT_FACTORY_BY_RDBMS[rdbms] = importer_context_factory


register(postgres_context, "pg", "postgres", "postgresql")
register(sqlite_context, "sqlite", "sqlite3")
register(mariadb_context, "maria", "mariadb", "mysql")


def import_sirene(sirene_path: Path, connection: Any, rdbms: str,
                  process_names: Optional[Callable[[str], str]] = to_snake,
                  bulk_copy: bool = True):
    """
    :param sirene_path: the path to sirene dir
    :param connection: a DB-API v2 connection
    :param rdbms: name of the RDBMS
    :param process_names: a function to process the names
    :param bulk_copy: if True, use bulk copy if available
    """
    assert sirene_path.exists()
    logger = logging.getLogger("datagouv_tools")

    if connection is None:
        connection = FakeConnection(logger)
    if process_names is None:
        def process_names(name: str) -> str: return name

    logger.debug("Import data with following parameters:"
                 "sirene_path: %s, connection: %s, rdbms: %s", sirene_path,
                 connection, rdbms)

    context_factory = _SIRENE_CONTEXT_FACTORY_BY_RDBMS.get(rdbms.casefold())
    if context_factory is None:
        raise ValueError(f"Unknown RDBMS '{rdbms}'")

    importer_context = context_factory(logger, connection)


    _import_sirene(logger, sirene_path, importer_context, process_names, bulk_copy)


def _import_sirene(logger: logging.Logger, sirene_path: Path,
                   importer_context: ImporterContext,
                   process_names: Callable[[str], str],
                   bulk_copy: bool = True):
    """
    :param sirene_path: path to data and schemas
    :param importer_context: objects to import data
    :param process_names: a function to process field and table names
    :param bulk_copy: if True, use bulk copy if available
    """

    logger.debug("Import data with following parameters:"
                 "sirene_path: %s, importer_context: %s, process_names: %s",
                 sirene_path, importer_context, process_names)

    importer = SireneImporter(logger, data_sources(sirene_path),
                              importer_context, process_names, bulk_copy)
    importer.execute()


########
# MAIN #
########
if __name__ == "__main__":
    import doctest
    doctest.testmod()
