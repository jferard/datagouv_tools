Sirene2pg. An utility to import the SIRENE database to PostgreSQL
     Copyright (C) 2020 J. Férard <https://github.com/jferard>

License: GPL v3

The SIRENE database is a comprehensive database on the companies registred in France.

Usage
~~~~~
1. Download CSV and ZIP files from https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/

2. Execute

.. code:: python

        path = Path(r"/path/to/SIRENE")
        connection = pg8000.connect(user="sirene", password="yourpass",
                                    database="sirene")
        try:
            import_sirene(path, connection)
        finally:
            connection.commit()
            connection.close()



Note on Ubuntu
~~~~~~~~~~~~~~
To run the script on Ubuntu, you have to create a database `sirene` and a user `sirene`:

.. code:: bash

    jferard@jferard-Z170XP-SLI:~/prog/python/sirene2pg$ sudo -u postgres psql
    psql (10.10 (Ubuntu 10.10-0ubuntu0.18.04.1))
    Type "help" for help.

    postgres=# CREATE DATABASE sirene;
    CREATE DATABASE
    postgres=# CREATE USER sirene;
    CREATE ROLE
    postgres=# GRANT ALL ON DATABASE sirene TO sirene;
    GRANT
    postgres=# \q

And to run your script as sytem `sirene` user.

.. code:: bash

    jferard@jferard-Z170XP-SLI:~/prog/python/sirene2pg$ sudo -u sirene venv/bin/python3.7 -m unittest import_sirene_test.py
