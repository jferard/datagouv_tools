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
#
#
import csv
from _csv import QUOTE_ALL
from dataclasses import dataclass
from typing import Sequence

from datagouv_tools.util import to_standard


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
    # 1 : voie, 2 : ensemble immobilier, 3 : lieu-dit,
    # 4 :pseudo-voie, 5 : voie provisoire
    FantoirField(110, 1, 'X', 'Caractère du lieu_dit'),
    # 1 : lieu-dit bâti, 0 sinon
    FantoirField(111, 2, 'X', '', True),
    FantoirField(113, 8, 'X',
                 'Dernier mot entièrement alphabétique du libellé de la voie'),
    FantoirField(121, 30, 'X', '', True),
])
RECORD_FORMATS = (HEADER_FORMAT, DIRECTION_FORMAT, COMMUNE_FORMAT, VOIE_FORMAT)


def get_record_format(line):
    if line[0] == '\x00':
        return HEADER_FORMAT
    elif line[3] == ' ':
        return DIRECTION_FORMAT
    elif line[7] == ' ':
        return COMMUNE_FORMAT
    else:
        return VOIE_FORMAT


class fantoir_dialect(csv.Dialect):
    delimiter = '\t'
    quotechar = '\b'
    doublequote = True
    skipinitialspace = False
    lineterminator = '\n'
    quoting = QUOTE_ALL


def parse(filename):
    with open(filename, encoding="ascii") as f:
        yield from FantoirParser(f).parse()


class FantoirParser:
    def __init__(self, f):
        self._f = f

    def parse(self):
        for line in self._f:
            if line[:10] == '9999999999':  # last line
                continue
            line = line.rstrip("\n")
            record_format = get_record_format(line)
            if record_format is not None and record_format:
                record = record_format.format(line)
                yield record

