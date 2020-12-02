# encoding: utf-8
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
import logging
import sqlite3
import unittest
from pathlib import Path
from unittest.mock import Mock, call

from datagouv_tools.import_fantoir import (import_fantoir,
                                           import_fantoir_thread)
from datagouv_tools.fantoir import HEADER_FORMAT, DIRECTION_FORMAT, \
    COMMUNE_FORMAT, VOIE_FORMAT, get_first_empty_slice_by_record, \
    get_record_format
from datagouv_tools.sql.generic import FakeConnection

SKIP_IT = True

if not SKIP_IT:
    import mysql.connector as mariadb
    import pg8000


class TestImportFantoir(unittest.TestCase):
    def setUp(self):
        self.path = Path(
            Path(__file__).parent, "resources", "fantoir",
            r"Fichier national FANTOIR (situation octobre 2019)_reduit.zip")

    @unittest.skipIf(SKIP_IT, "integration test")
    def test_import_temp_pg(self):
        connection = pg8000.connect(user="postgres", password="postgres",
                                    database="sirene")
        rdbms = "pg"
        fantoir_path = self.path
        import_fantoir(connection, fantoir_path, rdbms)
        connection.close()

    def test_import_sqlite(self):
        connection = sqlite3.connect("fantoir.db")
        rdbms = "sqlite"
        fantoir_path = self.path
        import_fantoir(connection, fantoir_path, rdbms)
        connection.close()

    def test_import_thread(self):
        logger = logging.getLogger("datagouv_tools")
        logger.setLevel(logging.INFO)
        rdbms = "pg"
        fantoir_path = self.path
        logger: logging.Logger = Mock()
        import_fantoir_thread(
            lambda: FakeConnection(logger), fantoir_path, rdbms)

        for message in [
            'DRY RUN: cursor.execute(DROP TABLE IF EXISTS header)',
            'DRY RUN: cursor.execute(DROP TABLE IF EXISTS direction)',
            'DRY RUN: cursor.execute(DROP TABLE IF EXISTS commune)',
            'DRY RUN: cursor.execute(DROP TABLE IF EXISTS voie)',
            'DRY RUN: cursor.execute(CREATE TABLE header (\n'
            '    libelle_du_centre_de_production_du_fichier text,\n'
            '    date_de_situation_du_fichier               text,\n'
            '    date_de_production_du_fichier              text\n'
            '))',
            'DRY RUN: cursor.execute(CREATE TABLE direction (\n'
            '    code_departement  text,\n'
            '    code_direction    text,\n'
            '    libelle_direction text\n))',
            'DRY RUN: cursor.execute(CREATE TABLE commune (\n'
            '    code_departement             text,\n'
            '    code_direction               text,\n'
            '    code_commune                 text,\n'
            '    cle_rivoli                   text,\n'
            '    libelle_commune              text,\n'
            '    type_de_la_commune           text,\n'
            '    caractere_rur                text,\n'
            '    caractere_de_population      text,\n'
            '    population_reelle            text,\n'
            '    population_a_part            text,\n'
            '    population_fictive           text,\n'
            '    caractere_dannulation        text,\n'
            '    date_dannulation             text,\n'
            '    date_de_creation_de_larticle text\n'
            '))',
            'DRY RUN: cursor.execute(CREATE TABLE voie (\n'
            '    code_departement                        '
            '                   text,\n'
            '    code_direction                          '
            '                   text,\n'
            '    code_commune                            '
            '                   text,\n'
            '    identifiant_de_la_voie_dans_la_commune  '
            '                   text,\n'
            '    cle_rivoli                              '
            '                   text,\n'
            '    code_nature_de_voie                     '
            '                   text,\n'
            '    libelle_voie                            '
            '                   text,\n'
            '    type_de_la_commune                      '
            '                   text,\n'
            '    caractere_rur                           '
            '                   text,\n'
            '    caractere_de_voie                       '
            '                   text,\n'
            '    caractere_de_population                 '
            '                   text,\n'
            '    population_a_part                       '
            '                   text,\n'
            '    population_fictive                      '
            '                   text,\n'
            '    caractere_dannulation                   '
            '                   text,\n'
            '    date_dannulation                        '
            '                   text,\n'
            '    date_de_creation_de_larticle            '
            '                   text,\n'
            '    code_identifiant_majic_de_la_voie       '
            '                   text,\n'
            '    type_de_voie                            '
            '                   text,\n'
            '    caractere_du_lieu_dit                   '
            '                   text,\n'
            '    dernier_mot_entierement_alphabetique_du_'
            'libelle_de_la_voie text\n'
            '))',
            'DRY RUN: cursor.execute(TRUNCATE header)',
            'DRY RUN: cursor.execute(TRUNCATE direction)',
            'DRY RUN: cursor.execute(TRUNCATE commune)',
            'DRY RUN: cursor.execute(TRUNCATE voie)',
            'DRY RUN: cursor.execute(ANALYZE header)',
            'DRY RUN: cursor.execute(ANALYZE direction)',
            'DRY RUN: cursor.execute(ANALYZE commune)',
            'DRY RUN: cursor.execute(ANALYZE voie)',
            'DRY RUN: connection.commit()',
            'DRY RUN: connection.close()'
        ]:
            self.assertTrue(call.info(message) in logger.mock_calls)

    @unittest.skipIf(SKIP_IT, "integration test")
    def test_import_mariadb(self):
        connection = mariadb.connect(user="sirene", password="yourpass",
                                     database="sirene")
        rdbms = "mariadb"
        fantoir_path = self.path
        import_fantoir(connection, fantoir_path, rdbms)
        connection.close()

    @unittest.skipIf(SKIP_IT, "integration test")
    def test_import_thread_pg(self):
        rdbms = "pg"
        fantoir_path = self.path
        import_fantoir_thread(
            lambda: pg8000.connect(user="postgres", password="postgres",
                                   database="sirene"), fantoir_path, rdbms)

    @unittest.skipIf(SKIP_IT, "integration test")
    def test_import_thread_mariadb(self):
        rdbms = "mariadb"
        fantoir_path = self.path
        import_fantoir_thread(
            lambda: mariadb.connect(user="sirene", password="yourpass",
                                    database="sirene"), fantoir_path, rdbms)

    def test_get_first_empty_slice_by_record(self):
        self.assertEqual({HEADER_FORMAT: slice(0, 10, None),
                          DIRECTION_FORMAT: slice(3, 11, None),
                          COMMUNE_FORMAT: slice(6, 10, None),
                          VOIE_FORMAT: slice(41, 42, None), },
                         get_first_empty_slice_by_record())

    def test_get_record_format(self):
        for line, expected in [
            ('\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00ENEVERS'
             '                  2019110120193080000000', HEADER_FORMAT),
            ('010        AIN                                             '
             '00000000000000 00000000000000', DIRECTION_FORMAT),
            ("010001    WL'ABERGEMENT-CLEMENCIAT        N  3      0000825"
             "00000000000000 00000001987001", COMMUNE_FORMAT),
            ('010001A008WLOT BELLEVUE                   N  3  0          '
             '00000000000000 00000002001351               000592   '
             'BELLEVUE', VOIE_FORMAT)]:
            self.assertEqual(expected, get_record_format(line))


if __name__ == '__main__':
    unittest.main()
