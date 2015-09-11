#!/usr/local/bin/python
# encoding: utf-8
"""
pessto_marshall_streams.py
============
:Summary:
    Import PESSTO Marshall tables into crossmatch catalogues

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

# xdocopt-usage-tempx
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
import yaml

import re
from docopt import docopt
from dryxPython import mysql as dms
from dryxPython import logs as dl
from dryxPython import commonutils as dcu
from dryxPython.projectsetup import setup_main_clutil
from ._base_importer import _base_importer


class pessto_marshall_streams(_base_importer):

    """
    The worker class for the pessto_marshall_streams module

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary
        - ``pathToDataFIle`` -- path to the pessto_marshall_streams data file
        - ``version`` -- version of the pessto_marshall_streams catalogue


    **Todo**
        - @review: when complete, clean pessto_marshall_streams class
        - @review: when complete add logging
        - @review: when complete, decide whether to abstract class to another module
    """
    # Initialisation
    # 1. @flagged: what are the unique attrributes for each object? Add them
    # to __init__

    # 4. @flagged: what actions does each object have to be able to perform? Add them here
    # Method Attributes
    def get(self):
        """get the pessto_marshall_streams object

        **Return:**
            - ``pessto_marshall_streams``

        **Todo**
            - @review: when complete, clean get method
            - @review: when complete add logging
        """
        self.log.info('starting the ``get`` method')

        yamlFilePath = '/'.join(string.split(__file__, '/')
                                [:-1]) + "/resources/pessto_marshall_table_selects.yaml"

        stream = file(yamlFilePath, 'r')
        pesstoQueries = yaml.load(stream)
        stream.close()

        self.primaryIdColumnName = "primaryId"
        self.raColName = "raDeg"
        self.declColName = "decDeg"

        for k, v in pesstoQueries["pessto queries"].iteritems():
            self.dbTableName = k
            self.databaseInsertbatchSize = 500
            self.marshallQuery = v
            self.dictList = self.create_dictionary_of_pessto_marshall_streams()
            self.add_data_to_database_table()
            self.add_htmids_to_database_table()
            self._update_database_helper_table()

        self.log.info('completed the ``get`` method')
        return pessto_marshall_streams

    def create_dictionary_of_pessto_marshall_streams(
            self):
        """create dictionary of pessto_marshall_streams

        **Key Arguments:**
            # -

        **Return:**
            - None

        **Todo**
            - @review: when complete, clean create_dictionary_of_pessto_marshall_streams method
            - @review: when complete add logging
        """
        self.log.info(
            'starting the ``create_dictionary_of_pessto_marshall_streams`` method')

        dictList = []
        tableName = self.dbTableName

        rows = dms.execute_mysql_read_query(
            sqlQuery=self.marshallQuery,
            dbConn=self.pmDbConn,
            log=self.log
        )

        totalCount = len(rows)
        count = 0

        for row in rows:
            count += 1
            if count > 1:
                # Cursor up one line and clear line
                sys.stdout.write("\x1b[1A\x1b[2K")
            print "%(count)s / %(totalCount)s `%(tableName)s` data added to memory" % locals()
            dictList.append(dict(row))

        self.log.info(
            'completed the ``create_dictionary_of_pessto_marshall_streams`` method')
        return dictList

    # use the tab-trigger below for new method
    def _update_database_helper_table(
            self):
        """ update dataasbe helper table

        **Key Arguments:**
            # -

        **Return:**
            - None

        **Todo**
            - @review: when complete, clean _update_database_helper_table method
            - @review: when complete add logging
        """
        self.log.info('starting the ``_update_database_helper_table`` method')

        tableName = self.dbTableName

        sqlQuery = u"""
            update tcs_helper_catalogue_tables_info set last_updated = now() where table_name = "%(tableName)s";
        """ % locals()

        dms.execute_mysql_write_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )

        self.log.info('completed the ``_update_database_helper_table`` method')
        return None

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
