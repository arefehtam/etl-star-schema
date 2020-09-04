================
etl-start-schema
================

    Etl for converting OLTP database to a Star Scheam warehouse


    * Free software: GNU General Public License v3
    * Documentation: Not exists


Getting Started
---------------
This code is tested  on
    * Postgresl  version: 10.6
    * Python version: 3.7
#. First, install all needed requirement:
    pip3 install -r requirements_dev.txt
#. Check `config.ini` for database is correct. Also csv files are accessible from project root path(default is in `data`).
#. Run `application.py` to see end-to-end process


Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
