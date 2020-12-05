# coding: utf-8
# Implementing a sorted producer/consumer queue with Multiprocessing
import logging
import queue
import tempfile
import threading
from codecs import getreader
from dataclasses import dataclass
from datetime import datetime
from logging import Logger, getLogger
from pathlib import Path
from typing import Any, Callable, Iterable
from zipfile import ZipFile

from datagouv_tools.fantoir import RecordFormat, HEADER_FORMAT, RECORD_FORMATS, \
    get_record_format, fantoir_dialect
from datagouv_tools.import_generic import (ImporterContext,
                                           ImporterThreadContext)
from datagouv_tools.sql.generic import (SQLTable, SQLField, QueryExecutor,
                                        DefaultSQLTypeConverter,
                                        SQLIndexProvider, SQLIndex,
                                        QueryProvider, FakeConnection)
from datagouv_tools.sql.mariadb import (MariaDBQueryExecutor,
                                        MariaDBQueryProvider)
from datagouv_tools.sql.postgresql import (PostgreSQLQueryExecutor,
                                           PostgreSQLQueryProvider)
from datagouv_tools.sql.sql_type import SQLTypes, SQLIndexTypes
from datagouv_tools.sql.sqlite import SQLiteQueryExecutor
from datagouv_tools.util import CSVStream

create_thread = threading.Thread
logging.basicConfig(level=logging.DEBUG,
                    format=("%(asctime)s - %(name)s/%(file)s/%(funcName)s/"
                            "%(lineno)d - %(levelname)s: %(message)s"))


###########
# THREADS #
###########

def _import_with_threads(path_zipped, importer_context: ImporterThreadContext):
    consumer_thread_by_name = {}
    thread_info_by_name = {}
    for record_format in RECORD_FORMATS:
        csv_stream = CSVStream(record_format.name, record_format.header,
                               queue.Queue())
        thread_info = ThreadInfo(record_format, csv_stream)
        thread = create_thread(
            target=consumer_factory(importer_context, thread_info))
        thread_info_by_name[record_format.name] = thread_info
        consumer_thread_by_name[record_format.name] = thread

    producer_thread = create_thread(
        target=dispatch_records_factory(path_zipped, thread_info_by_name))

    for thread in consumer_thread_by_name.values():
        thread.start()
    producer_thread.start()
    # wait here
    for thread in consumer_thread_by_name.values():
        thread.join()


def dispatch_records_factory(path, thread_info_by_name):
    def dispatch_records():
        def send_to(name, csv_line):
            thread_info_by_name[name].csv_stream.send(csv_line)

        logger = getLogger("datagouv_tools")

        dispatcher = Dispatcher(logger, send_to)
        dispatcher.dispatch(path)

        for name in thread_info_by_name:
            send_to(name, None)

    return dispatch_records


@dataclass
class ThreadInfo:
    # connection: Any
    record_format: RecordFormat
    csv_stream: CSVStream


def consumer_factory(import_context: ImporterThreadContext,
                     thread_info: ThreadInfo):
    record_format = thread_info.record_format

    def copy_table(query_executor, table):
        dialect = fantoir_dialect
        query_executor.copy_stream(table, thread_info.csv_stream, 'latin-1',
                                   dialect)

    def consumer():
        query_executor = import_context.new_executor()
        write_table(query_executor, record_format, copy_table)
        query_executor.close()

    return consumer


########
# TEMP #
########

def _import_with_temp(path_zipped: Path, importer_context: ImporterContext):
    temp_path_by_name = dispatch_to_temp(path_zipped)
    for record_format in RECORD_FORMATS:
        if record_format == HEADER_FORMAT:
            continue
        write_table_from_temp(importer_context, record_format,
                              temp_path_by_name[record_format.name])


def dispatch_to_temp(path_zipped: Path):
    """
    Dispatch lines to
    :param path_zipped:
    :return:
    """
    temp_dir = tempfile.gettempdir()
    path_by_name = {
        record_format.name: Path(temp_dir, f"{record_format.name}.csv") for
        record_format in RECORD_FORMATS}
    stream_by_name = {name: open(path, "w", encoding="ascii") for name, path in
                      path_by_name.items()}

    def send_to(name, csv_line):
        stream_by_name[name].write(csv_line)

    logger = getLogger()

    dispatcher = Dispatcher(logger, send_to)
    dispatcher.dispatch(path_zipped)
    for stream in stream_by_name.values():
        stream.close()

    return path_by_name


def write_table_from_temp(importer_context, record_format, path):
    executor = importer_context.query_executor

    def copy_table(executor, table):
        dialect = fantoir_dialect
        with open(path, "rb") as stream:
            executor.copy_stream(table, stream, 'latin-1', dialect)

    write_table(executor, record_format, copy_table)


###########
# GENERIC #
###########


def get_table(record_format: RecordFormat) -> SQLTable:
    fields = [SQLField(record_format.name, field, SQLTypes.TEXT, i)
              for i, field in enumerate(record_format.header)]
    return SQLTable(record_format.name, fields, [])


class Dispatcher:
    """
    A class to dispatch lines to different destinations, depending on
    the line.
    """

    def __init__(self, logger: Logger, send_to: Callable[[str, str], None]):
        """

        :param logger:
        :param send_to: a function to send a line
        """
        self._logger = logger
        self._send_to = send_to

    def dispatch(self, path):
        #        source = path.open('rb')
        with ZipFile(path, 'r') as zipped:
            first = zipped.infolist()[0]
            with zipped.open(first) as stream:
                self._dispatch_byte_stream(stream)
        #        source.close()

    def _dispatch_byte_stream(self, stream):
        f = getreader('ascii')(stream)
        count = 0
        t = datetime.now()
        for line in f:
            if line[:10] == '9999999999':  # last line
                continue
            count += 1
            if count % 500_000 == 0:
                new_t = datetime.now()
                self._logger.debug(
                    f"{new_t} / {count} lines read in ({new_t - t})")
                t = new_t
            line = line.rstrip("\n")
            record_format = get_record_format(line)
            if record_format is not None and record_format:
                csv_line = record_format.to_row(line)
                self._send_to(record_format.name, csv_line)


def write_table(executor: QueryExecutor, record_format: RecordFormat,
                copy_table: Callable[[QueryExecutor, SQLTable], None]):
    table = get_table(record_format)
    executor.create_table(table)
    executor.prepare_copy(table)
    executor.commit()
    copy_table(executor, table)
    executor.finalize_copy(table)
    executor.commit()
    executor.create_indices(table)
    executor.commit()


########
# MISC #
########
class FantoirSQLIndexProvider(SQLIndexProvider):
    def get_indices(self, fields: Iterable[SQLField]) -> Iterable[SQLIndex]:
        for field in fields:
            if field.starts_with("code_"):
                yield SQLIndex(field.table_name, field.field_name,
                               SQLIndexTypes.HASH)


def postgres_context(logger, connection):
    return ImporterContext(
        query_executor=PostgreSQLQueryExecutor(logger, connection,
                                               PostgreSQLQueryProvider()),
        type_converter=DefaultSQLTypeConverter(),
        index_provider=FantoirSQLIndexProvider(),
    )


def postgres_thread_context(logger, new_connection: Callable[[], Any]):
    return ImporterThreadContext(
        new_executor=lambda: PostgreSQLQueryExecutor(logger, new_connection(),
                                                     PostgreSQLQueryProvider()
                                                     ),
        type_converter=DefaultSQLTypeConverter(),
        index_provider=FantoirSQLIndexProvider(),
    )


def sqlite_context(logger, connection):
    return ImporterContext(
        query_executor=SQLiteQueryExecutor(logger, connection,
                                           QueryProvider()),
        type_converter=DefaultSQLTypeConverter(),
        index_provider=FantoirSQLIndexProvider(),
    )


# Couldn't make sqlite_thread_context work.

def mariadb_context(logger, connection):
    return ImporterContext(
        query_executor=MariaDBQueryExecutor(logger, connection,
                                            MariaDBQueryProvider()),
        type_converter=DefaultSQLTypeConverter(),
        index_provider=FantoirSQLIndexProvider(),
    )


def mariadb_thread_context(logger, new_connection):
    return ImporterThreadContext(
        new_executor=lambda: MariaDBQueryExecutor(logger, new_connection(),
                                                  MariaDBQueryProvider()),
        type_converter=DefaultSQLTypeConverter(),
        index_provider=FantoirSQLIndexProvider(),
    )


_FANTOIR_CONTEXT_FACTORY_BY_RDBMS = {}


def register(importer_context_factory: Callable[
    [logging.Logger, Any], ImporterContext],
             *rdbms_list: str):
    for rdbms in rdbms_list:
        _FANTOIR_CONTEXT_FACTORY_BY_RDBMS[rdbms] = importer_context_factory


_FANTOIR_THREAD_CONTEXT_FACTORY_BY_RDBMS = {}


def register_thread(importer_thread_context_factory: Callable[
    [logging.Logger, Callable[[], Any]], ImporterThreadContext],
                    *rdbms_list: str):
    for rdbms in rdbms_list:
        _FANTOIR_THREAD_CONTEXT_FACTORY_BY_RDBMS[
            rdbms] = importer_thread_context_factory


register(postgres_context, "pg", "postgres", "postgresql")
register_thread(postgres_thread_context, "pg", "postgres", "postgresql")
register(sqlite_context, "sqlite", "sqlite3")
register(mariadb_context, "maria", "mariadb", "mysql")
register_thread(mariadb_thread_context, "maria", "mariadb", "mysql")


def import_fantoir(connection, fantoir_path, rdbms):
    logger = logging.getLogger("datagouv_tools")
    if connection is None:
        connection = FakeConnection(logger)
    logger.debug("Import data with following parameters:"
                 "fantoir_path: %s, connection: %s, rdbms: %s",
                 fantoir_path,
                 connection, rdbms)
    context_factory = _FANTOIR_CONTEXT_FACTORY_BY_RDBMS.get(
        rdbms.casefold())
    if context_factory is None:
        raise ValueError(f"Unknown RDBMS '{rdbms}'")
    importer_context = context_factory(logger, connection)
    _import_with_temp(fantoir_path, importer_context)


def import_fantoir_thread(new_connection, fantoir_path, rdbms):
    logger = logging.getLogger("datagouv_tools")
    logger.debug("Import data with following parameters:"
                 "fantoir_path: %s, rdbms: %s",
                 fantoir_path, rdbms)
    context_factory = _FANTOIR_THREAD_CONTEXT_FACTORY_BY_RDBMS.get(
        rdbms.casefold())
    if context_factory is None:
        raise ValueError(f"Unknown RDBMS '{rdbms}'")
    importer_thread_context = context_factory(logger, new_connection)
    _import_with_threads(fantoir_path, importer_thread_context)


if __name__ == "__main__":
    import doctest

    doctest.testmod()
