#!/usr/local/bin/python
# encoding: utf-8
"""
importers_clutils.py
====================
:Summary:
    CL Utils for the Sherlock importers

:Author:
    David Young

:Date Created:
    August 25, 2015

:dryx syntax:
    - ``_someObject`` = a 'private' object that should only be changed for debugging

:Notes:
    - If you have any questions requiring this script/module please email me: d.r.young@qub.ac.uk

:Tasks:
    @review: when complete pull all general functions and classes into dryxPython

Usage:
    sherlock-importers cat <cat_name> <pathToDataFile> <cat_version> [-s <pathToSettingsFile>]
    sherlock-importers stream <stream_name> [-s <pathToSettingsFile>]

    -h, --help            show this help message
    -v, --version         show version
    -s, --settings        the settings file
"""
################# GLOBAL IMPORTS ####################
import sys
import os
os.environ['TERM'] = 'vt100'
import readline
import glob
import pickle
from docopt import docopt
from dryxPython import logs as dl
from dryxPython import commonutils as dcu
from dryxPython.projectsetup import setup_main_clutil
from sherlock.imports import milliquas as milliquasImporter
from sherlock.imports import veron as veronImporter
from sherlock.imports import tmp_sdss as sdssImporter
from sherlock.imports import pessto_marshall_streams as pesstoImporter
from sherlock.imports import ifs_galaxies as ifsImporter
from sherlock.imports.ned_d import ned_d as nedImporter


def tab_complete(text, state):
    return (glob.glob(text + '*') + [None])[state]


def main(arguments=None):
    """
    The main function used when ``importers_clutils.py`` is run as a single script from the cl, or when installed as a cl command
    """
    # setup the command-line util settings
    su = setup_main_clutil(
        arguments=arguments,
        docString=__doc__,
        logLevel="WARNING",
        options_first=False,
        projectName="sherlock"
    )
    arguments, settings, log, dbConn = su.setup()

    # tab completion for raw_input
    readline.set_completer_delims(' \t\n;')
    readline.parse_and_bind("tab: complete")
    readline.set_completer(tab_complete)

    # unpack remaining cl arguments using `exec` to setup the variable names
    # automatically
    for arg, val in arguments.iteritems():
        if arg[0] == "-":
            varname = arg.replace("-", "") + "Flag"
        else:
            varname = arg.replace("<", "").replace(">", "")
        if isinstance(val, str) or isinstance(val, unicode):
            exec(varname + " = '%s'" % (val,))
        else:
            exec(varname + " = %s" % (val,))
        if arg == "--dbConn":
            dbConn = val
        log.debug('%s = %s' % (varname, val,))

    ## START LOGGING ##
    startTime = dcu.get_now_sql_datetime()
    log.info(
        '--- STARTING TO RUN THE importers_clutils.py AT %s' %
        (startTime,))

    # set options interactively if user requests
    if "interactiveFlag" in locals() and interactiveFlag:

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

    # call the worker function
    # x-if-settings-or-database-credientials
    if cat:
        if cat_name == "milliquas":
            testObject = milliquasImporter(
                log=log,
                settings=settings,
                pathToDataFile=pathToDataFile,
                version=cat_version,
                catalogueName=cat_name
            )
            testObject.get()
        if cat_name == "veron":
            testObject = veronImporter(
                log=log,
                settings=settings,
                pathToDataFile=pathToDataFile,
                version=cat_version,
                catalogueName=cat_name
            )
            testObject.get()
        if "sdss" in cat_name:
            testObject = sdssImporter(
                log=log,
                settings=settings,
                pathToDataFile=pathToDataFile,
                version=cat_version,
                catalogueName=cat_name
            )
            testObject.get()
        if "ned_d" in cat_name:
            testObject = nedImporter(
                log=log,
                settings=settings,
                pathToDataFile=pathToDataFile,
                version=cat_version,
                catalogueName=cat_name
            )
            testObject.get()

    elif stream:
        if "pessto" in stream_name:
            testObject = pesstoImporter(
                log=log,
                settings=settings,
                pathToDataFile=pathToDataFile,
                version=cat_version,
                catalogueName=cat_name
            )
            testObject.get()
        if "ifs" in stream_name:
            testObject = ifsImporter(
                log=log,
                settings=settings,
                pathToDataFile=pathToDataFile,
                version=cat_version,
                catalogueName=cat_name
            )
            testObject.get()

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = dcu.get_now_sql_datetime()
    runningTime = dcu.calculate_time_difference(startTime, endTime)
    log.info('-- FINISHED ATTEMPT TO RUN THE importers_clutils.py AT %s (RUNTIME: %s) --' %
             (endTime, runningTime, ))

    return


###################################################################
# CLASSES                                                         #
###################################################################
# xt-class-module-worker-tmpx
# xt-class-tmpx


###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################
# xt-worker-def

# use the tab-trigger below for new function
# xt-def-with-logger

###################################################################
# PRIVATE (HELPER) FUNCTIONS                                      #
###################################################################

if __name__ == '__main__':
    main()
