sherlock 
=========================

.. image:: https://readthedocs.org/projects/qub-sherlock/badge/
    :target: http://qub-sherlock.readthedocs.io/en/latest/?badge
    :alt: Documentation Status

.. image:: https://cdn.rawgit.com/thespacedoctor/sherlock/master/coverage.svg
    :target: https://cdn.rawgit.com/thespacedoctor/sherlock/master/htmlcov/index.html
    :alt: Coverage Status

*A python package and command-line tools to contextually classify astronomical transient sources. Sherlock mines a library of historical and on-going survey data to attempt to identify the source of a transient event, and predict the classification of the event based on the associated crossmatched data*.





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
        sherlock info [-s <pathToSettingsFile>]
        sherlock [-N] dbmatch [-f --update] [-s <pathToSettingsFile>]
        sherlock [-vN] match -- <ra> <dec> [<pathToSettingsFile>] 
        sherlock clean [-s <pathToSettingsFile>]
        sherlock wiki [-s <pathToSettingsFile>]
        sherlock import ned <ra> <dec> <radiusArcsec> [-s <pathToSettingsFile>]
        sherlock import cat <cat_name> <pathToDataFile> <cat_version> [-s <pathToSettingsFile>]
        sherlock import stream <stream_name> [-s <pathToSettingsFile>]
    
    Options:
        init                    setup the sherlock settings file for the first time
        match                   XXXX
        dbmatch                 database match
        clean                   XXXX
        wiki                    XXXX
        import                  XXXX
        ned                     use the online NED database as the source catalogue
        cat                     import a static catalogue into the crossmatch catalogues database
        stream                  download/stream new data from a give source catalogue into the sherlock crossmatch catalogues database
        info                    print an overview of the current catalogues, views and streams in the sherlock database ready for crossmatching
    
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
    
        -N, --skipNedUpdate     do not update the NED database before classification
        -f, --fast              faster but errors in crossmatch table ingest my be misses
        -h, --help              show this help message
        -s, --settings          the settings file
        -v, --verbose           print more details to stdout
        -l, --transientlistId   the id of the transient list to classify
        -u, --update            update the transient database with new classifications and crossmatches
    

Documentation
=============

Documentation for sherlock is hosted by `Read the Docs <http://sherlock.readthedocs.org/en/stable/>`__ (last `stable version <http://sherlock.readthedocs.org/en/stable/>`__ and `latest version <http://sherlock.readthedocs.org/en/latest/>`__).

Installation
============

The easiest way to install sherlock is to use ``pip``:

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



.. todo::

    - make a note about how to setup mysql login paths and have them associated with the database setting in the sherlock settings file

.. code:: bash

    mysql_config_editor set --login-path=xxx --host=127.0.0.1 --user=myuser --password --port=xxx


Development
-----------

If you want to tinker with the code, then install in development mode.
This means you can modify the code from your cloned repo:

.. code:: bash

    git clone git@github.com:thespacedoctor/sherlock.git
    cd sherlock
    python setup.py develop

`Pull requests <https://github.com/thespacedoctor/sherlock/pulls>`__
are welcomed!

Sublime Snippets
~~~~~~~~~~~~~~~~

If you use `Sublime Text <https://www.sublimetext.com/>`_ as your code editor, and you're planning to develop your own python code with sherlock, you might find `my Sublime Snippets <https://github.com/thespacedoctor/sherlock-Sublime-Snippets>`_ useful. 

Issues
------

Please report any issues
`here <https://github.com/thespacedoctor/sherlock/issues>`__.

License
=======

Copyright (c) 2016 David Young

Permission is hereby granted, free of charge, to any person obtaining a
copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be included
in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

