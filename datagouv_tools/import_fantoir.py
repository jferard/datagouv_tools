# coding: utf-8
# Implementing a sorted producer/consumer queue with Multiprocessing
import csv
import logging
import queue
import tempfile
import threading
from dataclasses import dataclass
from datetime import datetime
from io import TextIOWrapper
from itertools import islice
from logging import Logger, getLogger
from pathlib import Path
from typing import Sequence, Any, Callable
from zipfile import ZipFile

import pg8000

from datagouv_tools.import_generic import ImporterArgs
from datagouv_tools.sql.generic import EmptySQLIndexProvider, \
    DefaultSQLTypeConverter, SQLTable, SQLField, QueryExecutor
from datagouv_tools.sql.postgresql import PostgreSQLQueryExecutor, \
    PostgreSQLQueryProvider
from datagouv_tools.sql.sql_type import SQLTypes
from datagouv_tools.util import CSVStream, to_standard

create_thread = threading.Thread
logging.basicConfig(level=logging.DEBUG,
                    format=("%(asctime)s - %(name)s/%(filename)s/%(funcName)s/"
                            "%(lineno)d - %(levelname)s: %(message)s"))


def postgres_args(logger, connection):
    return ImporterArgs(
        query_executor=PostgreSQLQueryExecutor(logger, connection,
                                               PostgreSQLQueryProvider()),
        type_converter=DefaultSQLTypeConverter(),
        index_provider=EmptySQLIndexProvider(),
    )

###########
# FANTOIR #
###########


@dataclass(unsafe_hash=True)
class FantoirField:
    start: int
    length: int
    type: str
    description: str
    is_filler: bool = False

    @property
    def end(self):
        return self.start + self.length

    @property
    def slice(self):
        return slice(self.start - 1, self.start - 1 + self.length)

    @property
    def db_name(self):
        return to_standard(self.description)


@dataclass
class RecordFormat:
    name: str
    fields: Sequence[FantoirField]

    def __post_init__(self):
        self._n_s = [(field.db_name, field.slice) for field in self.fields if
                     not field.is_filler]
        self.header = [f.db_name for f in self.fields if not f.is_filler]

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return isinstance(other, RecordFormat) and self.name == other.name

    def format(self, line):
        return {"record_type": self.name, **{n: line[s] for n, s in self._n_s}}

    def to_line(self, record):
        return "\t".join(record[k].strip() for k in self.header) + "\n"


HEADER_FORMAT = RecordFormat("header", [
    FantoirField(1, 10, 'X', '', True),
    FantoirField(12, 25, 'X', 'Libellé du centre de production du fichier'),
    FantoirField(37, 8, 'X', 'Date de situation du fichier'),  # AAAAMMJJ
    FantoirField(45, 8, 'X', 'Date de production du fichier'),  # AAAAQQQ
    FantoirField(52, 98, 'X', '', True),
])

DIRECTION_FORMAT = RecordFormat("direction", [
    FantoirField(1, 2, 'X', 'Code département'),
    FantoirField(3, 1, 'X', 'Code direction'),
    FantoirField(4, 8, 'X', '', True),
    FantoirField(12, 30, 'X', 'Libellé Direction'),
    FantoirField(42, 109, 'X', '', True),
])

COMMUNE_FORMAT = RecordFormat("commune", [
    FantoirField(1, 2, 'X', 'Code département'),
    FantoirField(3, 1, 'X', 'Code direction'),
    FantoirField(4, 3, 'X', 'Code commune'),
    FantoirField(7, 4, 'X', '', True),
    FantoirField(11, 1, 'X', 'Clé RIVOLI'),  # Contrôle la validité de COMM
    FantoirField(12, 30, 'X', 'Libellé Commune'),
    FantoirField(42, 1, 'X', '', True),
    FantoirField(43, 1, 'X', 'Type de la commune'),  # N : rurale, R : recensée
    FantoirField(44, 2, 'X', '', True),
    FantoirField(46, 1, 'X', 'Caractère RUR'),
    # 3 : pseudo-recensée, blanc sinon'
    FantoirField(47, 3, 'X', '', True),
    FantoirField(50, 1, 'X', 'Caractère de population'),
    # blanc si < 3000 hab, * sinon
    FantoirField(51, 2, 'X', '', True),
    FantoirField(53, 7, '9', 'Population réelle'),
    FantoirField(60, 7, '9', 'Population à part'),
    FantoirField(67, 7, '9', 'Population fictive'),
    FantoirField(74, 1, 'X', 'Caractère dannulation'),
    # Q : annulation avec transfert
    FantoirField(75, 7, '9', 'Date dannulation'),
    FantoirField(82, 7, '9', 'Date de création de larticle'),
    FantoirField(89, 42, 'X', '', True),
])

VOIE_FORMAT = RecordFormat("voie", [
    FantoirField(1, 2, 'X', 'Code département'),
    FantoirField(3, 1, 'X', 'Code direction'),
    FantoirField(4, 3, 'X', 'Code commune'),
    FantoirField(7, 4, 'X', 'Identifiant de la voie dans la commune'),
    FantoirField(11, 1, 'X', 'Clé RIVOLI'),
    FantoirField(12, 4, 'X', 'Code nature de voie'),
    FantoirField(16, 26, 'X', 'Libellé voie'),
    FantoirField(42, 1, 'X', '', True),
    FantoirField(43, 1, 'X', 'Type de la commune'),  # N : rurale, R : recensée
    FantoirField(44, 2, 'X', '', True),
    FantoirField(46, 1, 'X', 'Caractère RUR'),
    # 3 : pseudo-recensée, blanc sinon
    FantoirField(47, 2, 'X', '', True),
    FantoirField(49, 1, 'X', 'Caractère de voie'),  # 1 : privée, 0 : publique
    FantoirField(50, 1, 'X', 'Caractère de population'),
    # blanc si < 3000 hab, * sinon
    FantoirField(51, 9, 'X', '', True),
    FantoirField(60, 7, '9', 'Population à part'),
    FantoirField(67, 7, '9', 'Population fictive'),
    FantoirField(74, 1, 'X', 'Caractère dannulation'),
    # O : sans transfert, Q : avec
    FantoirField(75, 7, '9', 'Date dannulation'),
    FantoirField(82, 7, '9', 'Date de création de larticle'),
    FantoirField(89, 15, 'X', '', True),
    FantoirField(104, 5, 'X', 'Code identifiant MAJIC de la voie'),
    FantoirField(109, 1, 'X', 'Type de voie'),
    # 1 : voie, 2 : ensemble immobilier, 3 : lieu-dit, 4 :pseudo-voie, 5 : voie provisoire
    FantoirField(110, 1, 'X', 'Caractère du lieu_dit'),
    # 1 : lieu-dit bâti, 0 sinon
    FantoirField(111, 2, 'X', '', True),
    FantoirField(113, 8, 'X',
                 'Dernier mot entièrement alphabétique du libellé de la voie'),
    FantoirField(121, 30, 'X', '', True),
])

RECORD_FORMATS = (HEADER_FORMAT, DIRECTION_FORMAT, COMMUNE_FORMAT, VOIE_FORMAT)


def get_first_empty_slice_by_record():
    first_empty_slice_by_record = {}
    for record_format in RECORD_FORMATS:
        for field in record_format.fields:
            if field.is_filler:
                first_empty_slice_by_record[record_format] = field.slice
                break
    return first_empty_slice_by_record


FIRST_EMPTY_SLICE_BY_RECORD = get_first_empty_slice_by_record()


def get_record_format(line):
    for record_format in RECORD_FORMATS:
        s = FIRST_EMPTY_SLICE_BY_RECORD[record_format]
        data = line[s].strip(' \x00')
        if not data:
            return record_format
    return None


###########
# THREADS #
###########

def import_with_threads(path_zipped, get_args: Callable[[], ImporterArgs]):
    consumer_thread_by_name = {}
    thread_info_by_name = {}
    for record_format in RECORD_FORMATS:
        importer_args = get_args()
        csv_stream = CSVStream(record_format.name, record_format.header,
                               queue.Queue())
        thread_info = ThreadInfo(importer_args.query_executor, record_format, csv_stream)
        thread = create_thread(
            target=consumer_factory(importer_args, thread_info))
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


def dispatch_records_factory(path, consumer_thread_by_name):
    def dispatch_records():
        def send_to(name, csv_line):
            consumer_thread_by_name[name].csv_stream.send(csv_line)

        logger = getLogger()

        dispatcher = Dispatcher(logger, send_to)
        dispatcher.dispatch(path)

        for name in consumer_thread_by_name:
            send_to(name, None)

    return dispatch_records


def consumer_factory(importer_args: ImporterArgs, thread_info):
    executor = importer_args.query_executor

    record_format = thread_info.record_format
    connection = thread_info.connection

    def copy_table(table):
        dialect = get_dialect()
        executor.copy_stream(table, thread_info.csv_stream, 'latin-1',
                             dialect)

    def consumer():
        write_table(executor, record_format, copy_table)
        executor.close()

    return consumer


@dataclass
class ThreadInfo:
    connection: Any
    record_format: RecordFormat
    csv_stream: CSVStream


########
# TEMP #
########

def import_with_temp(path_zipped, importer_args):
    temp_path_by_name = dispatch_to_temp(path_zipped)
    for record_format in RECORD_FORMATS:
        if record_format == HEADER_FORMAT:
            continue
        write_table_from_temp(importer_args, record_format,
                              temp_path_by_name[record_format.name])


def dispatch_to_temp(path):
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
    dispatcher.dispatch(path)
    for stream in stream_by_name.values():
        stream.close()

    return path_by_name


def write_table_from_temp(importer_args, record_format, path):
    executor = importer_args.query_executor

    def copy_table(table):
        dialect = get_dialect()
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
        with ZipFile(path, 'r') as zipped:
            first = zipped.infolist()[0]
            with zipped.open(first) as stream:
                self._dispatch_byte_stream(stream)

    def _dispatch_byte_stream(self, stream):
        f = TextIOWrapper(stream, 'ascii')
        count = 0
        t = datetime.now()
        for line in islice(f, 1000):
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
                record = record_format.format(line)
                csv_line = record_format.to_line(record)
                self._send_to(record_format.name, csv_line)


def write_table(executor: QueryExecutor, record_format: RecordFormat,
                copy_table: Callable[[SQLTable], None]):
    table = get_table(record_format)
    executor.create_table(table)
    executor.prepare_copy(table)
    executor.commit()
    copy_table(table)
    executor.finalize_copy(table)
    executor.commit()
    executor.create_indices(table)
    executor.commit()


def get_dialect():
    dialect = csv.unix_dialect
    dialect.delimiter = "\t"
    dialect.quotechar = "\b"
    return dialect


if __name__ == "__main__":
    import doctest
    doctest.testmod()