DataGouv Tools. An utility to import  some data.gouv.fr data to PostgreSQL and other DBMS.
     Copyright (C) 2020 J. Férard <https://github.com/jferard>

License: GPL v3

`English version of README <README-en.rst>`_


Objectif
~~~~~~~~
`La base Sirene des entreprises et de leurs établissements <https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/>`_
et le `Fichier FANTOIR des voies et lieux-dits <https://www.data.gouv.fr/fr/datasets/fichier-fantoir-des-voies-et-lieux-dits/>`_
sont disponibles sur le site https://www.data.gouv.fr. Leur exploitation sous la forme CSV n'est
pas aisée. Le projet *DataGouv Tools* a pour but de faciliter l'import des ces données dans un SGBD.

SGDB supportés
~~~~~~~~~~~~~~
* PostgreSQL
* SQLite
* MariaDB

Installation
~~~~~~~~~~~~
0. Téléchargez DataGouv Tools dans un répertoire `datagouv_tools`.

1. Installez Python 3.7 et virtualenv.

Ubuntu:

.. code:: bash

    ...datagouv_tools$ sudo apt-get install python3-virtualenv python3.7

Windows: voir https://python.org pour télécharger Python 3.7 et :

.. code:: bash

    ...datagouv_tools> pip install virtualenv

2. Créez et activez un environnement virtuel:

.. code:: bash

    ...datagouv_tools$ mkdir venv
    ...datagouv_tools$ virtualenv venv
    ...datagouv_tools$ source venv/bin/activate

3. Installez le module `datagouv_tools`:

.. code:: bash

    (venv) ...datagouv_tools$ python3.7 setup.py install


SIRENE
~~~~~~
1. Téléchargez les fichiers CSV et ZIP depuis https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/

2. Executez :

.. code:: bash

    ...$ datagouv_tools -s sirene -r pg --database sirene --user sirene --password yourpass -p /path/to/sirene/directory

D'autres options sont disponibles:

* ``-r mariadb`` pour MariaDB
* ``-r sqlite`` pour SQLite

Ou bien dans un programme :

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
1. Téléchargez le fichier ZIP depuis https://www.data.gouv.fr/fr/datasets/fichier-fantoir-des-voies-et-lieux-dits/

2. Exécutez :

.. code:: bash

    ...$ datagouv_tools -s fantoir -r pg --database sirene --user sirene --password yourpass -p /path/to/fantoir.zip

D'autres options sont disponibles:

* ``-r mariadb`` pour MariaDB
* ``-r sqlite`` pour SQLite

Ou bien dans un programme :

.. code:: python

        path = Path(r"/path/to/FANTOIR.zip")
        connection = pg8000.connect(user="postgres", password="postgres",
                                    database="sirene")
        try:
            import_fantoir(connection, fantoir_path, "postgresql")
        finally:
            connection.close()


Note sur Ubuntu
~~~~~~~~~~~~~~~
PostgreSQL
----------
Pour réaliser l'import sous Ubuntu, vous devez créer une base de données `sirene` et un utilisateur `sirene`:

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


Tester
~~~~~~
Si nécessaire :

.. code:: bash

    ...$ tox

Ou :

.. code:: bash

    ...$ venv/bin/pip install pytest
    ...$ venv/bin/pip install pytest-cov
    ...$ flake8 --exclude=venv && venv/bin/python3.7 -m pytest --cov-report term-missing --cov=import_sirene  && venv/bin/python3.7 -m pytest --cov-report term-missing --cov-append --doctest-modules import_sirene.py --cov=import_sirene

Ou :

.. code:: bash

    ...$ python3.7 -m pytest --cov-report term-missing --cov=datagouv_tools  && python3.7 -m pytest --cov-report term-missing --cov-append --doctest-modules datagouv_tools --cov=datagouv_tools && flake8 --exclude=venv,.eggs


