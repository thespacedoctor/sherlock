#!/usr/local/bin/python
# encoding: utf-8
"""
veron.py
============
:Summary:
    Import veron catagloue from plain text file

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
"""
################# GLOBAL IMPORTS ####################
import sys
import os
os.environ['TERM'] = 'vt100'
import readline
import glob
import pickle
import codecs
import string

import re
from docopt import docopt
from dryxPython import mysql as dms
from dryxPython import logs as dl
from dryxPython import commonutils as dcu
from dryxPython.projectsetup import setup_main_clutil
from ._base_importer import _base_importer


class veron(_base_importer):

    """
    The worker class for the veron module

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary
        - ``pathToDataFIle`` -- path to the veron data file
        - ``version`` -- version of the veron catalogue
        - ``catalogueName`` -- the name of the catalogue

    **Todo**
        - @review: when complete, clean veron class
        - @review: when complete add logging
        - @review: when complete, decide whether to abstract class to another module
    """
    # Initialisation
    # 1. @flagged: what are the unique attrributes for each object? Add them
    # to __init__

    # 4. @flagged: what actions does each object have to be able to perform? Add them here
    # Method Attributes
    def get(self):
        """get the veron object

        **Return:**
            - ``veron``

        **Todo**
            - @review: when complete, clean get method
            - @review: when complete add logging
        """
        self.log.info('starting the ``get`` method')

        self.dictList = self.create_dictionary_of_veron()
        self.add_data_to_database_table()
        self.add_htmids_to_database_table()

        self.log.info('completed the ``get`` method')
        return veron

    def create_dictionary_of_veron(
            self):
        """create dictionary of veron

        **Key Arguments:**
            # -

        **Return:**
            - None

        **Todo**
            - @review: when complete, clean create_dictionary_of_veron method
            - @review: when complete add logging
        """
        self.log.info('starting the ``create_dictionary_of_veron`` method')

        dictList = []
        lines = string.split(self.catData, '\n')

        totalCount = len(lines)
        count = 0
        switch = 0
        for line in lines:
            if (len(line) == 0 or line[0] in ["#", " "]) and switch == 0:
                continue
            else:
                switch = 1
            count += 1
            if count > 1:
                # Cursor up one line and clear line
                sys.stdout.write("\x1b[1A\x1b[2K")
            print "%(count)s / %(totalCount)s veron data added to memory" % locals()

            if count == 1:
                theseKeys = []
                someKeys = string.split(line, '|')
                for key in someKeys:
                    if key == "_RAJ2000":
                        key = "raDeg"
                    if key == "_DEJ2000":
                        key = "decDeg"
                    if key == "Cl":
                        key = "class"
                    if key == "nR":
                        key = "not_radio"
                    if key == "Name":
                        key = "name"
                    if key == "l_z":
                        key = "redshift_flag"
                    if key == "z":
                        key = "redshift"
                    if key == "Sp":
                        key = "spectral_classification"
                    if key == "n_Vmag":
                        key = "magnitude_filter"
                    if key == "Vmag":
                        key = "magnitude"
                    if key == "B-V":
                        key = "B_V"
                    if key == "U-B":
                        key = "U_B"
                    if key == "Mabs":
                        key = "abs_magnitude"
                    theseKeys.append(key)
                continue

            if count in [2, 3]:
                continue

            thisDict = {}
            theseValues = string.split(line, '|')

            for k, v in zip(theseKeys, theseValues):
                v = v.strip()
                if len(v) == 0 or v == "-":
                    v = None
                thisDict[k] = v
            dictList.append(thisDict)

        self.log.info(
            'completed the ``create_dictionary_of_veron`` method')
        return dictList

    # use the tab-trigger below for new method
    # xt-class-method

    # 5. @flagged: what actions of the base class(es) need ammending? ammend them here
    # Override Method Attributes
    # method-override-tmpx

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
