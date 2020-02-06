DataGouv Tools. An utility to import  some data.gouv.fr data to PostgreSQL and other DBMS.
     Copyright (C) 2020 J. Férard <https://github.com/jferard>

License: GPL v3


Goal
~~~~

`The SIRENE database <https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/>`_
(a comprehensive database on the companies registred in France) and the `FANTOIR file <https://www.data.gouv.fr/fr/datasets/fichier-fantoir-des-voies-et-lieux-dits/>`_
are available on https://www.data.gouv.fr.

The *DataGouv Tools* aims to facilitate the import of those data in a RDBMS.

Supported RDBMS
~~~~~~~~~~~~~~~
* PostgreSQL
* SQLite
* MariaDB

Install
~~~~~~~
0. Download DataGouv Tools in a `datagouv_tools` directory.

1. Install Python 3.7 and virtualenv.

Ubuntu:

.. code:: bash

    ...datagouv_tools$ sudo apt-get install python3-virtualenv python3.7

Windows: see https://python.org Python 3.7 download and :

.. code:: bash

    ...datagouv_tools> pip install virtualenv

2. Create and activate a virtual env:

.. code:: bash

    ...datagouv_tools$ mkdir venv
    ...datagouv_tools$ virtualenv venv
    ...datagouv_tools$ source venv/bin/activate

3. Install the `datagouv_tools` module :

.. code:: bash

    (venv) ...datagouv_tools$ python3.7 setup.py install



SIRENE
~~~~~~
1. Download CSV and ZIP files from https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/

2. And run:

.. code:: bash

    ...$ datagouv_tools -s sirene -r pg --database sirene --user sirene --password yourpass -p /path/to/sirene/directory

Or in a program:

.. code:: python

        path = Path(r"/path/to/sirene/directory")
        connection = pg8000.connect(user="sirene", password="yourpass",
                                    database="sirene")
        try:
            import_sirene(path, connection, "postgresql")
        finally:
            connection.commit()
            connection.close()

FANTOIR
~~~~~~~
1. Dowload ZIP file from https://www.data.gouv.fr/fr/datasets/fichier-fantoir-des-voies-et-lieux-dits/

2. And run:

.. code:: bash

    ...$ datagouv_tools -s fantoir -r pg --database sirene --user sirene --password yourpass -p /path/to/fantoir.zip

Or in a program:

.. code:: python

        path = Path(r"/path/to/FANTOIR.zip")
        connection = pg8000.connect(user="postgres", password="postgres",
                                    database="sirene")
        try:
            import_fantoir(connection, fantoir_path, "postgresql")
        finally:
            connection.close()


Note on Ubuntu
~~~~~~~~~~~~~~
PostgreSQL
----------
To run the script on Ubuntu, you have to create a database `sirene` and a user `sirene`:

.. code:: bash

    ...$ sudo -u postgres psql
    psql (10.10 (Ubuntu 10.10-0ubuntu0.18.04.1))
    Type "help" for help.

    postgres=# CREATE DATABASE sirene;
    CREATE DATABASE
    postgres=# CREATE USER sirene;
    CREATE ROLE
    postgres=# GRANT ALL ON DATABASE sirene TO sirene;
    GRANT
    postgres=# \q

MariaDB
-------

.. code:: bash

    ...$ sudo mariadb
    [sudo] Mot de passe de jferard :
    Welcome to the MariaDB monitor.  Commands end with ; or \g.
    Your MariaDB connection id is 32
    Server version: 10.1.43-MariaDB-0ubuntu0.18.04.1 Ubuntu 18.04

    Copyright (c) 2000, 2018, Oracle, MariaDB Corporation Ab and others.

    Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

    MariaDB [(none)]> CREATE DATABASE `sirene`;
    Query OK, 1 row affected (0.01 sec)

    MariaDB [(none)]> CREATE USER 'sirene'@localhost IDENTIFIED BY 'yourpass';
    Query OK, 0 rows affected (0.01 sec)

    MariaDB [(none)]> GRANT ALL privileges ON `sirene`.* TO 'sirene'@localhost;
    Query OK, 0 rows affected (0.00 sec)

    MariaDB [sirene]> GRANT FILE ON *.* TO 'sirene'@'localhost';
    Query OK, 0 rows affected (0.01 sec)


Testing
~~~~~~~
If needed:

.. code:: bash

    ...$ tox

Or:

.. code:: bash

    ...$ venv/bin/pip install pytest
    ...$ venv/bin/pip install pytest-cov
    ...$ flake8 --exclude=venv && venv/bin/python3.7 -m pytest --cov-report term-missing --cov=import_sirene  && venv/bin/python3.7 -m pytest --cov-report term-missing --cov-append --doctest-modules import_sirene.py --cov=import_sirene

Or:

.. code:: bash

    ...$ python3.7 -m pytest --cov-report term-missing --cov=datagouv_tools  && python3.7 -m pytest --cov-report term-missing --cov-append --doctest-modules datagouv_tools --cov=datagouv_tools && flake8 --exclude=venv,.eggs


