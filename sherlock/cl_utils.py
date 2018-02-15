#!/usr/local/bin/python
# encoding: utf-8
"""
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
    sherlock [-NA] dbmatch [--update] [-s <pathToSettingsFile>]
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
    -h, --help              show this help message
    -s, --settings          the settings file
    -v, --verbose           print more details to stdout
    -l, --transientlistId   the id of the transient list to classify
    -u, --update            update the transient database with new classifications and crossmatches
"""
################# GLOBAL IMPORTS ####################
import sys
import os
os.environ['TERM'] = 'vt100'
import readline
import glob
import pickle
from docopt import docopt
from fundamentals import tools, times
from fundamentals.renderer import list_of_dictionaries
from database_cleaner import database_cleaner
from commonutils import update_wiki_pages
from subprocess import Popen, PIPE, STDOUT
from sherlock.imports import milliquas
from sherlock.imports import veron as veronImporter
from sherlock.imports import marshall as marshallImporter
from sherlock.imports import ifs as ifsImporter
from sherlock.imports import ned_d as nedImporter
from sherlock.imports import ned as nedStreamImporter
from sherlock.commonutils import update_wiki_pages
from sherlock import transient_classifier
# from ..__init__ import *


def main(arguments=None):
    """
    The main function used when ``cl_utils.py`` is run as a single script from the cl, or when installed as a cl command

    .. todo ::

        - update key arguments values and definitions with defaults
        - update return values and definitions
        - update usage examples and text
        - update docstring text
        - check sublime snippet exists
        - clip any useful text to docs mindmap
        - regenerate the docs and check redendering of this docstring
    """
    # setup the command-line util settings

    su = tools(
        arguments=arguments,
        docString=__doc__,
        logLevel="DEBUG",
        options_first=False,
        projectName="sherlock"
    )
    arguments, settings, log, dbConn = su.setup()

    # unpack remaining cl arguments using `exec` to setup the variable names
    # automatically
    for arg, val in arguments.iteritems():
        if arg[0] == "-":
            varname = arg.replace("-", "") + "Flag"
        else:
            varname = arg.replace("<", "").replace(">", "")
            if varname == "import":
                varname = "iimport"
        if isinstance(val, str) or isinstance(val, unicode):
            exec(varname + " = '%s'" % (val,))
        else:
            exec(varname + " = %s" % (val,))
        if arg == "--dbConn":
            dbConn = val
        log.debug('%s = %s' % (varname, val,))

    ## START LOGGING ##
    startTime = times.get_now_sql_datetime()
    log.debug(
        '--- STARTING TO RUN THE cl_utils.py AT %s' %
        (startTime,))

    # call the worker function
    # x-if-settings-or-database-credientials
    if init:
        from os.path import expanduser
        home = expanduser("~")
        filepath = home + "/.config/sherlock/sherlock.yaml"
        cmd = """open %(filepath)s""" % locals()
        p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
        try:
            cmd = """open %(filepath)s""" % locals()
            p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
        except:
            pass
        try:
            cmd = """start %(filepath)s""" % locals()
            p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
        except:
            pass

    if match or dbmatch:
        if verboseFlag:
            verbose = 2
        else:
            verbose = 1

        if skipNedUpdateFlag:
            updateNed = False
        else:
            updateNed = True

        if skipAnnotationFlag:
            updateAnnotations = False
        else:
            updateAnnotations = True

        classifier = transient_classifier.transient_classifier(
            log=log,
            settings=settings,
            ra=ra,
            dec=dec,
            name=False,
            verbose=verbose,
            update=updateFlag,
            updateNed=updateNed,
            updateAnnotations=updateAnnotations
        )
        classifier.classify()

    if clean:
        cleaner = database_cleaner(
            log=log,
            settings=settings
        )
        cleaner.clean()
    if wiki:
        updateWiki = update_wiki_pages(
            log=log,
            settings=settings
        )
        updateWiki.update()

    if iimport and ned:
        ned = nedStreamImporter(
            log=log,
            settings=settings,
            coordinateList=["%(ra)s %(dec)s" % locals()],
            radiusArcsec=radiusArcsec
        )
        ned.ingest()
    if iimport and cat:
        if cat_name == "milliquas":
            catalogue = milliquas(
                log=log,
                settings=settings,
                pathToDataFile=pathToDataFile,
                version=cat_version,
                catalogueName=cat_name
            )
            catalogue.ingest()
        if cat_name == "veron":
            catalogue = veronImporter(
                log=log,
                settings=settings,
                pathToDataFile=pathToDataFile,
                version=cat_version,
                catalogueName=cat_name
            )
            catalogue.ingest()

        if "ned_d" in cat_name:
            catalogue = nedImporter(
                log=log,
                settings=settings,
                pathToDataFile=pathToDataFile,
                version=cat_version,
                catalogueName=cat_name
            )
            catalogue.ingest()
    if iimport and stream:
        if "marshall" in stream_name:
            stream = marshallImporter(
                log=log,
                settings=settings,
            )
            stream.ingest()
        if "ifs" in stream_name:
            stream = ifsImporter(
                log=log,
                settings=settings
            )
            stream.ingest()
    if not init and not match and not clean and not wiki and not iimport and ra:

        classifier = transient_classifier.transient_classifier(
            log=log,
            settings=settings,
            ra=ra,
            dec=dec,
            name=False,
            verbose=verboseFlag
        )
        classifier.classify()

    if info:
        print "sherlock-catalogues"
        wiki = update_wiki_pages(
            log=log,
            settings=settings
        )
        table = list(wiki._get_table_infos(trimmed=True))

        dataSet = list_of_dictionaries(
            log=log,
            listOfDictionaries=table
        )
        tableData = dataSet.reST(filepath=None)
        print tableData
        print

        print "Crossmatch Streams"
        table = list(wiki._get_stream_view_infos(trimmed=True))
        dataSet = list_of_dictionaries(
            log=log,
            listOfDictionaries=table
        )
        tableData = dataSet.reST(filepath=None)
        print tableData
        print

        print "Views on Catalogues and Streams"

        table = list(wiki._get_view_infos(trimmed=True))
        dataSet = list_of_dictionaries(
            log=log,
            listOfDictionaries=table
        )
        tableData = dataSet.reST(filepath=None)
        print tableData

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = times.get_now_sql_datetime()
    runningTime = times.calculate_time_difference(startTime, endTime)
    log.debug('-- FINISHED ATTEMPT TO RUN THE cl_utils.py AT %s (RUNTIME: %s) --' %
              (endTime, runningTime, ))

    return


if __name__ == '__main__':
    main()
