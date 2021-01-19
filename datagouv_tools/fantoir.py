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
from csv import QUOTE_ALL
from typing import Sequence, Mapping, Union, Generator, Iterator, Any, io

from datagouv_tools.util import to_standard

NATURE_VOIE_BY_CODE = {
    'ACH': 'ANCIEN CHEMIN',
    'AER': 'AERODROME',
    'AERG': 'AEROGARE',
    'AGL': 'AGGLOMERATION',
    'AIRE': 'AIRE',
    'ALL': 'ALLEE',
    'ANGL': 'ANGLE',
    'ARC': 'ARCADE',
    'ART': 'ANCIENNE ROUTE',
    'AUT': 'AUTOROUTE',
    'AV': 'AVENUE',
    'BASE': 'BASE',
    'BD': 'BOULEVARD',
    'BER': 'BERGE',
    'BORD': 'BORD',
    'BRE': 'BARRIERE',
    'BRG': 'BOURG',
    'BRTL': 'BRETELLE',
    'BSN': 'BASSIN',
    'CAE': 'CARRIERA',
    'CALL': 'CALLE, CALLADA',
    'CAMI': 'CAMIN',
    'CAMP': 'CAMP',
    'CAN': 'CANAL',
    'CAR': 'CARREFOUR',
    'CARE': 'CARRIERE',
    'CASR': 'CASERNE',
    'CC': 'CHEMIN COMMUNAL',
    'CD': 'CHEMIN DEPARTEMENTAL',
    'CF': 'CHEMIN FORESTIER',
    'CHA': 'CHASSE',
    'CHE': 'CHEMIN',
    'CHEM': 'CHEMINEMENT',
    'CHL': 'CHALET',
    'CHP': 'CHAMP',
    'CHS': 'CHAUSSEE',
    'CHT': 'CHATEAU',
    'CHV': 'CHEMIN VICINAL',
    'CITE': 'CITE',
    'CIVE': 'COURSIVE',
    'CLOS': 'CLOS',
    'CLR': 'COULOIR',
    'COIN': 'COIN',
    'COL': 'COL',
    'COR': 'CORNICHE',
    'CORO': 'CORON',
    'COTE': 'COTE',
    'COUR': 'COUR',
    'CPG': 'CAMPING',
    'CR': 'CHEMIN RURAL',
    'CRS': 'COURS',
    'CRX': 'CROIX',
    'CTR': 'CONTOUR',
    'CTRE': 'CENTRE',
    'DARS': 'DARSE, DARCE',
    'DEVI': 'DEVIATION',
    'DIG': 'DIGUE',
    'DOM': 'DOMAINE',
    'DRA': 'DRAILLE',
    'DSC': 'DESCENTE',
    'ECA': 'ECART',
    'ECL': 'ECLUSE',
    'EMBR': 'EMBRANCHEMENT',
    'EMP': 'EMPLACEMENT',
    'ENC': 'ENCLOS',
    'ENV': 'ENCLAVE',
    'ESC': 'ESCALIER',
    'ESP': 'ESPLANADE',
    'ESPA': 'ESPACE',
    'ETNG': 'ETANG',
    'FD': 'FOND',
    'FG': 'FAUBOURG',
    'FON': 'FONTAINE',
    'FOR': 'FORET',
    'FORT': 'FORT',
    'FOS': 'FOSSE',
    'FRM': 'FERME',
    'GAL': 'GALERIE',
    'GARE': 'GARE',
    'GBD': 'GRAND BOULEVARD',
    'GPL': 'GRANDE PLACE',
    'GR': 'GRANDE RUE',
    'GREV': 'GREVE',
    'HAB': 'HABITATION',
    'HAM': 'HAMEAU',
    'HIP': 'HIPPODROME',
    'HLE': 'HALLE',
    'HLG': 'HALAGE',
    'HLM': 'HLM',
    'HTR': 'HAUTEUR',
    'ILE': 'ILE',
    'ILOT': 'ILOT',
    'IMP': 'IMPASSE',
    'JARD': 'JARDIN',
    'JTE': 'JETEE',
    'LAC': 'LAC',
    'LEVE': 'LEVEE',
    'LICE': 'LICES',
    'LIGN': 'LIGNE',
    'LOT': 'LOTISSEMENT',
    'MAIL': 'MAIL',
    'MAIS': 'MAISON',
    'MAR': 'MARCHE',
    'MARE': 'MARE',
    'MAS': 'MAS',
    'MNE': 'MORNE',
    'MRN': 'MARINA',
    'MTE': 'MONTEE',
    'NTE': 'NOUVELLE ROUTE',
    'PAE': 'PETITE AVENUE',
    'PARC': 'PARC',
    'PAS': 'PASSAGE',
    'PASS': 'PASSE',
    'PCH': 'PETIT CHEMIN',
    'PCHE': 'PORCHE',
    'PHAR': 'PHARE',
    'PIST': 'PISTE',
    'PKG': 'PARKING',
    'PL': 'PLACE',
    'PLA': 'PLACA',
    'PLAG': 'PLAGE',
    'PLAN': 'PLAN',
    'PLCI': 'PLACIS',
    'PLE': 'PASSERELLE',
    'PLN': 'PLAINE',
    'PLT': 'PLATEAU',
    'PNT': 'POINTE',
    'PONT': 'PONT',
    'PORQ': 'PORTIQUE',
    'PORT': 'PORT',
    'POST': 'POSTE',
    'POT': 'POTERNE',
    'PROM': 'PROMENADE',
    'PRT': 'PETITE ROUTE',
    'PRV': 'PARVIS',
    'PTA': 'PETITE ALLEE',
    'PTE': 'PORTE',
    'PTR': 'PETITE RUE',
    'PTTE': 'PLACETTE',
    'QUA': 'QUARTIER',
    'QUAI': 'QUAI',
    'RAC': 'RACCOURCI',
    'REM': 'REMPART',
    'RES': 'RESIDENCE',
    'RIVE': 'RIVE',
    'RLE': 'RUELLE',
    'ROC': 'ROCADE',
    'RPE': 'RAMPE',
    'RPT': 'ROND-POINT',
    'RTD': 'ROTONDE',
    'RTE': 'ROUTE',
    'RUE': 'RUE',
    'RUET': 'RUETTE',
    'RUIS': 'RUISSEAU',
    'RULT': 'RUELLETTE',
    'RVE': 'RAVINE',
    'SAS': 'SAS',
    'SEN': 'SENTIER, SENTE',
    'SQ': 'SQUARE',
    'STDE': 'STADE',
    'TER': 'TERRE',
    'TOUR': 'TOUR',
    'TPL': 'TERRE-PLEIN',
    'TRA': 'TRAVERSE',
    'TRAB': 'TRABOULE',
    'TRN': 'TERRAIN',
    'TRT': 'TERTRE',
    'TSSE': 'TERRASSE',
    'TUN': 'TUNNEL',
    'VAL': 'VAL',
    'VALL': 'VALLON, VALLEE',
    'VC': 'VOIE COMMUNALE',
    'VCHE': 'VIEUX CHEMIN',
    'VEN': 'VENELLE',
    'VGE': 'VILLAGE',
    'VIA': 'VIA',
    'VIAD': 'VIADUC',
    'VIL': 'VILLE',
    'VLA': 'VILLA',
    'VOIE': 'VOIE',
    'VOIR': 'VOIRIE',
    'VOUT': 'VOUTE',
    'VOY': 'VOYEUL',
    'VTE': 'VIEILLE ROUTE',
    'ZA': 'ZA',
    'ZAC': 'ZAC',
    'ZAD': 'ZAD',
    'ZI': 'ZI',
    'ZONE': 'ZONE',
    'ZUP': 'ZUP'
}

CODE_BY_NATURE_VOIE = {v: k for k, vs in NATURE_VOIE_BY_CODE.items() for v in vs.split(", ")}


def nature_voie(code: str) -> str:
    """
    >>> nature_voie('RPT')
    'ROND-POINT'

    :param code: code voie
    :return: nature voie or code voie itself
    """
    return NATURE_VOIE_BY_CODE.get(code, code)


def code_voie(nat_voie: str) -> str:
    """
    >>> code_voie('ROND-POINT')
    'RPT'

    :param nat_voie: nature voie
    :return: code voie or nat voie.
    """
    return CODE_BY_NATURE_VOIE.get(nat_voie, nat_voie)


class FantoirField:
    """
    A field of a Fantoir record_mapping
    """

    def __init__(self, start: int, length: int, type: str, description: str,
                 is_filler: bool = False):
        self.start = start
        self.length = length
        self.type = type
        self.description = description
        self.is_filler = is_filler
        self._db_name = to_standard(self.description)

    @property
    def end(self):
        return self.start + self.length

    @property
    def slice(self):
        return slice(self.start - 1, self.start - 1 + self.length)

    @property
    def db_name(self):
        return self._db_name


class RecordFormat:
    """"
    The format of a Fantoir record_mapping: header, direction, commune...
    """

    def __init__(self, name: str, fields: Sequence[FantoirField]):
        self.name = name
        self.fields = fields
        self.header = [f.db_name for f in self.fields if not f.is_filler]
        self._slices = [f.slice for f in self.fields if not f.is_filler]
        self._slice_by_name = {f.db_name: f.slice for f in self.fields if
                               not f.is_filler}

    def to_dict(self, line: str) -> Mapping[str, str]:
        ret = {n: line[s] for n, s in zip(self.header, self._slices)}
        ret["record_type"] = self.name
        return ret

    def to_row(self, line: str) -> str:
        return "\t".join([line[self._slices[i]].strip() for i, k in
                          enumerate(self.header)]) + "\n"

    def get(self, line: str, item: Union[int, slice, str]) -> str:
        if isinstance(item, (int, slice)):
            return line[self._slices[item]]
        elif isinstance(item, str):
            return line[self._slice_by_name[item]]
        else:
            raise TypeError

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, other) -> bool:
        return isinstance(other, RecordFormat) and self.name == other.name


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


def get_record_format(line: str) -> RecordFormat:
    if line[0] == '\x00':
        return HEADER_FORMAT
    elif line[3] == ' ':
        return DIRECTION_FORMAT
    elif line[7] == ' ':
        return COMMUNE_FORMAT
    else:
        return VOIE_FORMAT


class Record:
    def __init__(self, record_format: RecordFormat, line: str):
        self._line = line
        self._record_format = record_format

    def to_dict(self) -> Mapping[str, str]:
        return self._record_format.to_dict(self._line)

    def to_row(self) -> str:
        return self._record_format.to_row(self._line)

    def get_type(self) -> str:
        return self._record_format.name

    def __getitem__(self, item) -> str:
        return self._record_format.get(self._line, item)


class fantoir_dialect(csv.Dialect):
    delimiter = '\t'
    quotechar = '\b'
    doublequote = True
    skipinitialspace = False
    lineterminator = '\n'
    quoting = QUOTE_ALL


def parse(file: Union[str, io.TextIO]) -> Iterator[Record]:
    if hasattr(file, 'read'):
        yield from FantoirParser(file).parse()
    else:
        with open(file, encoding="ascii") as f:
            yield from FantoirParser(f).parse()


class FantoirParser:
    def __init__(self, f: Iterator[str]):
        self._f = f

    def parse(self) -> Iterator[Record]:
        for line in self._f:
            if line[:10] == '9999999999':  # last line
                continue
            line = line.rstrip("\n")
            record_format = get_record_format(line)
            if record_format is not None and record_format:
                yield Record(record_format, line)
