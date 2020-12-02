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
import io
import unittest
import zipfile

from datagouv_tools.fantoir import parse


class TestFantoir(unittest.TestCase):
    def test(self):
        data = zipfile.ZipFile(
            "resources/fantoir/Fichier national FANTOIR (situation octobre 2019)_reduit.zip").read(
            "-")
        for r in parse(io.StringIO(data.decode("ascii"))):
            if r["record_type"] == "direction":
                self.assertEqual(
                    ['code_departement', 'code_direction', 'libelle_direction',
                     'record_type'], list(r))
            elif r["record_type"] == "commune":
                self.assertEqual(
                    ['code_departement',
                     'code_direction',
                     'code_commune',
                     'cle_rivoli',
                     'libelle_commune',
                     'type_de_la_commune',
                     'caractere_rur',
                     'caractere_de_population',
                     'population_reelle',
                     'population_a_part',
                     'population_fictive',
                     'caractere_dannulation',
                     'date_dannulation',
                     'date_de_creation_de_larticle',
                     'record_type'], list(r))
            elif r["record_type"] == "voie":
                self.assertEqual(
                    ['code_departement',
                     'code_direction',
                     'code_commune',
                     'identifiant_de_la_voie_dans_la_commune',
                     'cle_rivoli',
                     'code_nature_de_voie',
                     'libelle_voie',
                     'type_de_la_commune',
                     'caractere_rur',
                     'caractere_de_voie',
                     'caractere_de_population',
                     'population_a_part',
                     'population_fictive',
                     'caractere_dannulation',
                     'date_dannulation',
                     'date_de_creation_de_larticle',
                     'code_identifiant_majic_de_la_voie',
                     'type_de_voie',
                     'caractere_du_lieu_dit',
                     'dernier_mot_entierement_alphabetique_du_libelle_de_la_voie',
                     'record_type'], list(r))


if __name__ == '__main__':
    unittest.main()
