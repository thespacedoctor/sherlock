sherlock 
=========================

*A python package and command-line tools to contextually classify astronomical transient sources. Sherlock mines a library of historical and on-going survey data to attempt to identify the source of a transient event, and predict the classification of the event based on the associated crossmatched data*.

Here's a summary of what's included in the python package:

.. include:: /classes_and_functions.rst

Command-Line Usage
==================

.. code-block:: bash 
   
    
    # SHERLOCK #
    : INFERING TRANSIENT-SOURCE CLASSIFICATIONS FROM SPATIALLY CROSS-MATCHED CATALOGUED SOURCES :
    =============================================================================================
    
    Documentation for sherlock can be found here: http://sherlock.readthedocs.org/en/stable
    
    .. todo ::
    
        - docuument cl_utils module
        - tidy usage text
    
    Usage:
        sherlock init
        sherlock <ra> <dec> [-s <pathToSettingsFile>]
        sherlock match [--update] [-s <pathToSettingsFile>]
        sherlock clean [-s <pathToSettingsFile>]
        sherlock wiki [-s <pathToSettingsFile>]
        sherlock import ned <ra> <dec> <radiusArcsec> [-s <pathToSettingsFile>]
        sherlock import cat <cat_name> <pathToDataFile> <cat_version> [-s <pathToSettingsFile>]
        sherlock import stream <stream_name> [-s <pathToSettingsFile>]
    
    Options:
        init                    setup the sherlock settings file for the first time
        match                   XXXX
        clean                   XXXX
        wiki                    XXXX
        import                  XXXX
        ned                     use the online NED database as the source catalogue
        cat                     import a static catalogue into the crossmatch catalogues database
        stream                  download/stream new data from a give source catalogue into the sherlock crossmatch catalogues database
    
    
        ra                      the right-ascension coordinate with which to perform a conesearch (sexegesimal or decimal degrees)
        dec                     the declination coordinate with which to perform a conesearch (sexegesimal or decimal degrees)
        radiusArcsec            radius in arcsec of the footprint to download from the online NED database
        cat_name                name of the catalogue being imported. The following catalogues can be imported:
                                    * ``veron``: Veron AGN/QSO catalogue
                                        http://cdsarc.u-strasbg.fr/viz-bin/Cat?VII/258
                                    * ``milliquas``: Million Quasars Catalog
                                        http://heasarc.gsfc.nasa.gov/w3browse/all/milliquas.html
                                    * ``ned_d``: NED's Master List of Redshift-Independent Extragalactic Distances
                                        https://ned.ipac.caltech.edu/Library/Distances/
        stream_name             name of the stream to import into the crossmatch catalogues database. The following streams can be imported:
                                    * ``ifs``: Multi Unit Spectroscopic Explorer (MUSE) IFS galaxy catalogue (L. Galbany)
                                        http://www.das.uchile.cl/~lgalbany/LG/research.html
    
        -h, --help              show this help message
        -v, --version           show version
        -s, --settings          the settings file
        -l, --transientlistId   the id of the transient list to classify
        -u, --update            update the transient database with new classifications and crossmatches
    
    
    .. todo ::
    
        - document this module
    
    

Installation
============

The easiest way to install sherlock us to use ``pip``:

.. code:: bash

    pip install qub-sherlock

Or you can clone the `github repo <https://github.com/thespacedoctor/sherlock>`__ and install from a local version of the code:

.. code:: bash

    git clone git@github.com:thespacedoctor/sherlock.git
    cd sherlock
    python setup.py install

To upgrade to the latest version of sherlock use the command:

.. code:: bash

    pip install qub-sherlock --upgrade


Documentation
=============

Documentation for sherlock is hosted by `Read the Docs <http://sherlock.readthedocs.org/en/stable/>`__ (last `stable version <http://sherlock.readthedocs.org/en/stable/>`__ and `latest version <http://sherlock.readthedocs.org/en/latest/>`__).

Command-Line Tutorial
=====================

Before you begin using sherlock you will need to populate some custom settings within the sherlock settings file.

To setup the default settings file at ``~/.config/sherlock/sherlock.yaml`` run the command:

.. code-block:: bash 
    
    sherlock init

This should create and open the settings file; follow the instructions in the file to populate the missing settings values (usually given an ``XXX`` placeholder). 

The Settings File
-----------------

Before you begin you will need to run the following code once to set a login-path for each of your mysql servers:

.. code-block:: bash 

    mysql_config_editor set --login-path=<uniqueLoginName> --host=localhost --user=<myUsername> --password --port=<port>

This stores your credentials in an encrypted file located at '~/.mylogin.cnf'.
Use `mysql_config_editor print --all` to see all of the login-paths set.


.. todo::

    - add tutorial

