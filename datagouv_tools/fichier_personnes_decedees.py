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
import datetime as dt
import enum
import logging
import re
from typing import Optional, Tuple, List, Iterable, Callable, Any, cast, \
    Dict

from datagouv_tools.import_generic import ImporterContext
from datagouv_tools.sql.generic import FakeConnection, QueryProvider, \
    DefaultSQLTypeConverter, SQLIndexProvider, SQLField, SQLIndex, SQLTable
from datagouv_tools.sql.sql_type import SQLIndexTypes, SQLTypes
from datagouv_tools.sql.sqlite import SQLiteQueryExecutor

NOMS_PRENOMS_PATTERN = re.compile(r"^([^*]+)\*([^/]+)(/\s*)?$")


class LenientDate:
    def __init__(self, year: int, month: int, day: int):
        self.year = year
        self.month = month
        self.day = day

    def as_date(self) -> dt.date:
        return dt.date(self.year, self.day, self.day)

    def __repr__(self) -> str:
        return "LenientDate({}, {}, {})".format(self.year, self.month, self.day)


class Sex(enum.Enum):
    M = 1
    F = 2
    UNKNOWN = 2


class Deces:
    def __init__(
            self, nom: str, prenoms: List[str], sexe: Sex,
            date_naiss: LenientDate, code_lieu_naiss: str, commune_naiss: str,
            pays_naiss: str, date_deces: LenientDate, code_lieu_deces: str,
            no_acte_deces: str
    ):
        self.nom = nom
        self.prenoms = prenoms
        self.sexe = sexe
        self.date_naiss = date_naiss
        self.code_lieu_naiss = code_lieu_naiss
        self.commune_naiss = commune_naiss
        self.pays_naiss = pays_naiss
        self.date_deces = date_deces
        self.code_lieu_deces = code_lieu_deces
        self.no_acte_deces = no_acte_deces

    def __repr__(self) -> str:
        return "{}({})".format(self.__class__.__name__, self.__dict__)


class InseeLineParser:
    def parse(self, line: str) -> Optional[Deces]:
        nom_prenoms = line[:80].strip()
        nom, prenoms = self._parse_nom_prenoms(nom_prenoms)
        sexe = self._parse_sex(line[80:81])
        date_naiss = self._parse_date(line[81:89])
        code_lieu_naiss = line[89:94]
        commune_naiss = line[94:124].strip()
        pays_naiss = line[124:154].strip()
        date_deces = self._parse_date(line[154:162])
        code_lieu_deces = line[162:167]
        no_acte_deces = line[167:176].strip()
        d = Deces(nom, prenoms, sexe, date_naiss, code_lieu_naiss,
                  commune_naiss, pays_naiss, date_deces,
                  code_lieu_deces, no_acte_deces)
        return d

    def _parse_nom_prenoms(self, s: str) -> Tuple[str, List[str]]:
        m = NOMS_PRENOMS_PATTERN.match(s)
        if m:
            nom = m.group(1)
            prenoms = m.group(2).split()
        else:
            nom = s
            prenoms = []
        return nom, prenoms

    def _parse_sex(self, s: str) -> Sex:
        if s == "1":
            return Sex.M
        elif s == "2":
            return Sex.F
        else:
            return Sex.UNKNOWN

    def _parse_date(self, s: str) -> LenientDate:
        try:
            return LenientDate(int(s[:4]), int(s[4:6]), int(s[6:]))
        except ValueError:
            return LenientDate(0, 0, 0)


_DECES_CONTEXT_FACTORY_BY_RDBMS = cast(
    Dict[str, Callable[[logging.Logger, Any], ImporterContext]], {}
)


class DecesSQLIndexProvider(SQLIndexProvider):
    def get_indices(self, fields: Iterable[SQLField]) -> Iterable[SQLIndex]:
        for field in fields:
            if field.field_name == "nom":
                yield SQLIndex(field.table_name, field.field_name,
                               SQLIndexTypes.HASH)


def sqlite_context(logger, connection):
    return ImporterContext(
        query_executor=SQLiteQueryExecutor(logger, connection,
                                           QueryProvider()),
        type_converter=DefaultSQLTypeConverter(),
        index_provider=DecesSQLIndexProvider(),
    )


def register(importer_context_factory: Callable[
    [logging.Logger, Any], ImporterContext],
             *rdbms_list: str):
    for rdbms in rdbms_list:
        _DECES_CONTEXT_FACTORY_BY_RDBMS[rdbms] = importer_context_factory


register(sqlite_context, "sqlite", "sqlite3")


DECES_TABLE = SQLTable(
    name="deces",
    fields=[
        SQLField("deces", "nom", SQLTypes.TEXT),
        SQLField("deces", "prenom1", SQLTypes.TEXT),
        SQLField("deces", "prenom2", SQLTypes.TEXT),
        SQLField("deces", "prenom3", SQLTypes.TEXT),
        SQLField("deces", "prenom4", SQLTypes.TEXT),
        SQLField("deces", "prenom5", SQLTypes.TEXT),
        SQLField("deces", "prenom6", SQLTypes.TEXT),
        SQLField("deces", "prenom7", SQLTypes.TEXT),
        SQLField("deces", "prenom8", SQLTypes.TEXT),
    ],
    indices=[]
)


def import_deces(connection, deces_path, rdbms):
    logger = logging.getLogger("datagouv_tools")
    if connection is None:
        connection = FakeConnection(logger)
    logger.debug("Import data with following parameters:"
                 "deces_path: %s, connection: %s, rdbms: %s",
                 deces_path,
                 connection, rdbms)
    context_factory = _DECES_CONTEXT_FACTORY_BY_RDBMS.get(
        rdbms.casefold())
    if context_factory is None:
        raise ValueError(f"Unknown RDBMS '{rdbms}'")
    importer_context = context_factory(logger, connection)

    executor = importer_context.query_executor
    executor.create_table(DECES_TABLE)
    executor.prepare_copy(DECES_TABLE)

    def _typed_reader():
        with deces_path.open("r", encoding="utf-8") as s:
            for line in s:
                d = InseeLineParser().parse(line)
                yield (d.nom,) + tuple(
                    d.prenoms[:8] + [None] * (8 - len(d.prenoms)))

    executor.insert_rows(DECES_TABLE, _typed_reader())
    executor.finalize_copy(DECES_TABLE)
    executor.create_indices(DECES_TABLE)
    executor.commit()
