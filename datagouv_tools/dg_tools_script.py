#!usr/bin/python
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
import argparse
from pathlib import Path

from datagouv_tools.import_fantoir import import_fantoir
from datagouv_tools.import_sirene import import_sirene

parser = argparse.ArgumentParser(description='Import sirene/fantoir')
parser.add_argument('-s', '--source', default='sirene',
                    help='source: sirene|fantoir')
parser.add_argument('-r', '--rdbms', default="pg",
                    help='rdbms: pg|sqlite|mariadb')
parser.add_argument('--database', help='database name', required=True)
parser.add_argument('--user', help='user name')
parser.add_argument('--password', help='user password')
parser.add_argument('-p', '--path',
                    help='path to directory (sirene) of to file (fantoir)')


def main():
    args = parser.parse_args()

    kwargs = {}
    if args.user is not None:
        kwargs["user"] = args.user
    if args.password is not None:
        kwargs["password"] = args.password

    if args.rdbms in ("pg", "postgres", "postgresql"):
        import pg8000
        connection = pg8000.connect(database=args.database, **kwargs)
    elif args.rdbms in ("maria", "mariadb", "mysql"):
        import mysql.connector as mariadb
        connection = mariadb.connect(database=args.database, **kwargs)
    elif args.rdbms in ("sqlite", "sqlite3"):
        import sqlite3
        connection = sqlite3.connect(args.database)
    else:
        raise ValueError("Unknown RDBMS {}".format(args.rdbms))

    path = Path(args.path)
    if args.source == "sirene":
        try:
            import_sirene(connection, path, args.rdbms)
        finally:
            connection.commit()
            connection.close()
    elif args.source == "fantoir":
        try:
            import_fantoir(connection, path, args.rdbms)
        finally:
            connection.commit()
            connection.close()
    else:
        raise ValueError("Unknown source {}".format(args.source))
