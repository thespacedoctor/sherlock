#!/usr/local/bin/python
# encoding: utf-8
"""
# SHERLOCK #
: INFERING TRANSIENT-SOURCE CLASSIFICATIONS FROM SPATIALLY CROSS-MATCHED CATALOGUED SOURCES :
=============================================================================================

Usage:
    sherlock match [--update] [-s <pathToSettingsFile>]
    sherlock clean [-s <pathToSettingsFile>]
    sherlock wiki [-s <pathToSettingsFile>]

    -h, --help            show this help message
    -v, --version         show version
    -s, --settings        the settings file
    -l, --transientlistId          the id of the transient list to classify
    -u, --update          update the transient database with new classifications and crossmatches
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
from classifier import classifier
from cleanup_database_tables import cleanup_database_tables
from commonutils import update_wiki_pages
# from ..__init__ import *


def tab_complete(text, state):
    return (glob.glob(text + '*') + [None])[state]


def main(arguments=None):
    """
    The main function used when ``cl_utils.py`` is run as a single script from the cl, or when installed as a cl command
    """
    # setup the command-line util settings
    su = setup_main_clutil(
        arguments=arguments,
        docString=__doc__,
        logLevel="DEBUG",
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
    log.debug(
        '--- STARTING TO RUN THE cl_utils.py AT %s' %
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

    if match:
        sherlock = classifier(
            log=log,
            settings=settings,
            update=updateFlag
        )
        sherlock.get()
    if clean:
        cleaner = cleanup_database_tables(
            log=log,
            settings=settings
        )
        cleaner.get()
    if wiki:
        updateWiki = update_wiki_pages(
            log=log,
            settings=settings
        )
        updateWiki.get()

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = dcu.get_now_sql_datetime()
    runningTime = dcu.calculate_time_difference(startTime, endTime)
    log.debug('-- FINISHED ATTEMPT TO RUN THE cl_utils.py AT %s (RUNTIME: %s) --' %
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
