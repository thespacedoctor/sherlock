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
        sherlock [-NA] dbmatch [-f --update] [-s <pathToSettingsFile>]
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
        cat                     import a static catalogue into the sherlock-catalogues database
        stream                  download/stream new data from a give source catalogue into the sherlock sherlock-catalogues database
        info                    print an overview of the current catalogues, views and streams in the sherlock database ready for crossmatching
    
        ra                      the right-ascension coordinate with which to perform a conesearch (sexegesimal or decimal degrees)
        dec                     the declination coordinate with which to perform a conesearch (sexegesimal or decimal degrees)
        radiusArcsec            radius in arcsec of the footprint to download from the online NED database
        cat_name                name of the catalogue being imported (veron|milliquas|ned_d)                          
        stream_name             name of the stream to import into the sherlock-catalogues database (ifs)
    
        -N, --skipNedUpdate     do not update the NED database before classification
        -A, --skipAnnotation    do not update the peak magnitudes and human readable text annotations of objects (can eat up some time)
        -f, --fast              faster but errors in crossmatch table ingest my be misses
        -h, --help              show this help message
        -s, --settings          the settings file
        -v, --verbose           print more details to stdout
        -l, --transientlistId   the id of the transient list to classify
        -u, --update            update the transient database with new classifications and crossmatches
    
