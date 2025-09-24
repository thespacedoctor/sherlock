#!/usr/bin/env python
# encoding: utf-8
"""
Documentation for sherlock can be found here: http://sherlock.readthedocs.org

.. todo ::

    - docuument cl_utils module
    - tidy usage text

Usage:
    sherlock init
    sherlock info [-s <pathToSettingsFile>]
    sherlock [-NA] dbmatch [--update] [-s <pathToSettingsFile>]
    sherlock [-bN] match -- <ra> <dec> [<pathToSettingsFile>] 
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
    cat_name                name of the catalogue being imported (veron|ned_d)                          
    stream_name             name of the stream to import into the sherlock-catalogues database (ifs)

    -N, --skipNedUpdate     do not update the NED database before classification
    -A, --skipMagUpdate     do not update the peak magnitudes and human readable text annotations of objects (can eat up some time)
    -h, --help              show this help message
    -s, --settings          the settings file
    -b, --verbose           print more details to stdout
    -u, --update            update the transient database with new classifications and crossmatches
    -v, --version           print the version of sherlock
"""
from __future__ import print_function
from __future__ import absolute_import
import readline
try:
    from sherlock.transient_classifier import transient_classifier
else:
    from sherlock import transient_classifier
from sherlock.commonutils import update_wiki_pages
from sherlock.imports import ned as nedStreamImporter
from sherlock.imports import ned_d as nedImporter
from sherlock.imports import ifs as ifsImporter
from sherlock.imports import veron as veronImporter
# from .commonutils import update_wiki_pages
# from .database_cleaner import database_cleaner
from fundamentals.renderer import list_of_dictionaries
from subprocess import Popen, PIPE, STDOUT
from fundamentals import tools, times
from docopt import docopt
import pickle
import glob

import sys
import os
os.environ['TERM'] = 'vt100'


def tab_complete(text, state):
    return (glob.glob(text + '*') + [None])[state]


def main(arguments=None):
    """
    *The main function used when `cl_utils.py` is run as a single script from the cl, or when installed as a cl command*
    """
    # setup the command-line util settings
    su = tools(
        arguments=arguments,
        docString=__doc__,
        logLevel="WARNING",
        options_first=False,
        distributionName="qub-sherlock",
        projectName="sherlock",
        defaultSettingsFile=True
    )
    arguments, settings, log, dbConn = su.setup()

    # tab completion for raw_input
    readline.set_completer_delims(' \t\n;')
    readline.parse_and_bind("tab: complete")
    readline.set_completer(tab_complete)

    # UNPACK REMAINING CL ARGUMENTS USING `EXEC` TO SETUP THE VARIABLE NAMES
    # AUTOMATICALLY
    a = {}
    for arg, val in list(arguments.items()):
        if arg[0] == "-":
            varname = arg.replace("-", "") + "Flag"
        else:
            varname = arg.replace("<", "").replace(">", "")
        a[varname] = val
        if arg == "--dbConn":
            dbConn = val
            a["dbConn"] = val
        log.debug('%s = %s' % (varname, val,))

    ## START LOGGING ##
    startTime = times.get_now_sql_datetime()
    log.info(
        '--- STARTING TO RUN THE cl_utils.py AT %s' %
        (startTime,))

    # set options interactively if user requests
    if "interactiveFlag" in a and a["interactiveFlag"]:

        # load previous settings
        moduleDirectory = os.path.dirname(__file__) + "/resources"
        pathToPickleFile = "%(moduleDirectory)s/previousSettings.p" % locals()
        try:
            with open(pathToPickleFile):
                pass
            previousSettingsExist = True
        except:
            previousSettingsExist = False
        previousSettings = {}
        if previousSettingsExist:
            previousSettings = pickle.load(open(pathToPickleFile, "rb"))

        # x-raw-input
        # x-boolean-raw-input
        # x-raw-input-with-default-value-from-previous-settings

        # save the most recently used requests
        pickleMeObjects = []
        pickleMe = {}
        theseLocals = locals()
        for k in pickleMeObjects:
            pickleMe[k] = theseLocals[k]
        pickle.dump(pickleMe, open(pathToPickleFile, "wb"))

    if a["init"]:
        from os.path import expanduser
        home = expanduser("~")
        filepath = home + "/.config/sherlock/sherlock.yaml"
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
        return

    init = a["init"]
    match = a["match"]
    dbmatch = a["dbmatch"]
    clean = a["clean"]
    wiki = a["wiki"]
    iimport = a["import"]
    ned = a["ned"]
    cat = a["cat"]
    stream = a["stream"]
    info = a["info"]
    ra = a["ra"]
    dec = a["dec"]
    radiusArcsec = a["radiusArcsec"]
    cat_name = a["cat_name"]
    cat_version = a["cat_version"]
    stream_name = a["stream_name"]
    pathToDataFile = a["pathToDataFile"]
    skipNedUpdateFlag = a["skipNedUpdateFlag"]
    skipMagUpdateFlag = a["skipMagUpdateFlag"]
    settingsFlag = a["settingsFlag"]
    verboseFlag = a["verboseFlag"]
    updateFlag = a["updateFlag"]

    if not ra:
        ra = False
        dec = False

    # CALL FUNCTIONS/OBJECTS
    if match or dbmatch:
        if verboseFlag:
            verbose = 2
        else:
            verbose = 1

        if skipNedUpdateFlag:
            updateNed = False
        else:
            updateNed = True

        if skipMagUpdateFlag:
            updatePeakMags = False
        else:
            updatePeakMags = True

        classifier = transient_classifier(
            log=log,
            settings=settings,
            ra=ra,
            dec=dec,
            name=False,
            verbose=verbose,
            update=updateFlag,
            updateNed=updateNed,
            updatePeakMags=updatePeakMags
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
        if "ifs" in stream_name:
            stream = ifsImporter(
                log=log,
                settings=settings
            )
            stream.ingest()
    if not init and not match and not clean and not wiki and not iimport and ra:

        classifier = transient_classifier(
            log=log,
            settings=settings,
            ra=ra,
            dec=dec,
            name=False,
            verbose=verboseFlag
        )
        classifier.classify()

    if info:
        print("sherlock-catalogues")
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
        print(tableData)
        print()

        print("Crossmatch Streams")
        table = list(wiki._get_stream_view_infos(trimmed=True))
        dataSet = list_of_dictionaries(
            log=log,
            listOfDictionaries=table
        )
        tableData = dataSet.reST(filepath=None)
        print(tableData)
        print()

        print("Views on Catalogues and Streams")

        table = list(wiki._get_view_infos(trimmed=True))
        dataSet = list_of_dictionaries(
            log=log,
            listOfDictionaries=table
        )
        tableData = dataSet.reST(filepath=None)
        print(tableData)

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = times.get_now_sql_datetime()
    runningTime = times.calculate_time_difference(startTime, endTime)
    log.info('-- FINISHED ATTEMPT TO RUN THE cl_utils.py AT %s (RUNTIME: %s) --' %
             (endTime, runningTime, ))

    return


if __name__ == '__main__':
    main()
