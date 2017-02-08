#!/usr/local/bin/python
# encoding: utf-8
"""
*The base importer for sherlock imports*

:Author:
    David Young

:Date Created:
    November 18, 2016

.. todo ::

    - document this module
"""
################# GLOBAL IMPORTS ####################
import sys
import os
os.environ['TERM'] = 'vt100'
import readline
import glob
import pickle
import codecs
import re
import string
from datetime import datetime, date, time
from docopt import docopt
from fundamentals.mysql import insert_list_of_dictionaries_into_database_tables, directory_script_runner, writequery
from fundamentals.renderer import list_of_dictionaries
from HMpTy.mysql import add_htm_ids_to_mysql_database_table


class _base_importer():

    """
    The base importer object used to import new catalgues into sherlock's crossmatch catalogues database

    **Key Arguments:**
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary
        - ``pathToDataFIle`` -- path to the _base_importer data file
        - ``version`` -- version of the _base_importer catalogue
        - ``catalogueName`` -- name of the catalogue
        - ``coordinateList`` -- list of coordinates (needed for some streamed tables)
        - ``radiusArcsec`` - - the radius in arcsec with which to perform the initial NED conesearch. Default * False*
    """
    # INITIALISATION

    def __init__(
            self,
            log,
            settings=False,
            pathToDataFile=False,
            version=False,
            catalogueName="",
            coordinateList=[],
            radiusArcsec=False
    ):
        self.log = log
        log.debug("instansiating a new '_base_importer' object")
        self.settings = settings
        self.pathToDataFile = pathToDataFile
        self.version = version
        self.catalogueName = catalogueName
        self.coordinateList = coordinateList
        self.radiusArcsec = radiusArcsec
        # xt-self-arg-tmpx

        # INITIAL ACTIONS
        # SETUP DATABASE CONNECTIONS
        # SETUP ALL DATABASE CONNECTIONS
        from sherlock import database
        db = database(
            log=self.log,
            settings=self.settings
        )
        dbConns, dbVersions = db.connect()
        self.transientsDbConn = dbConns["transients"]
        self.cataloguesDbConn = dbConns["catalogues"]
        self.pmDbConn = dbConns["marshall"]

        # OPEN THE FILE TO IMPORT THE DATA FROM
        if pathToDataFile:
            pathToReadFile = pathToDataFile
            try:
                self.log.debug("attempting to open the file %s" %
                               (pathToReadFile,))
                readFile = codecs.open(pathToReadFile, mode='r')
                self.catData = readFile.read()
                readFile.close()
            except IOError, e:
                message = 'could not open the file %s' % (pathToReadFile,)
                self.log.critical(message)
                raise IOError(message)
            readFile.close()
        else:
            self.catData = None

        # GET THE VERSION TO APPEND TO THE DATABASE TABLE NAME FOR THE
        # CATALOGUE
        if self.version:
            self.version = "_v" + \
                self.version.replace(" ", "").replace(
                    "v", "").replace(".", "_")
        else:
            self.version = ""
        version = self.version

        # BUILD THE DATABASE TABLE NAME
        self.dbTableName = "tcs_cat_%(catalogueName)s%(version)s" % locals()

        # SOME DEFAULT OBJECT ATTRIBUTES THAT CAN BE SUPERSEDED
        self.primaryIdColumnName = "primaryId"
        self.databaseInsertbatchSize = 2500
        self.raColName = "raDeg"
        self.declColName = "decDeg"
        self.uniqueKeyList = [self.raColName, "decDeg"]

        # DATETIME REGEX - EXPENSIVE OPERATION, LET"S JUST DO IT ONCE
        self.reDatetime = re.compile('^[0-9]{4}-[0-9]{2}-[0-9]{2}T')

        return None

    def _add_data_to_database_table(
            self,
            dictList,
            createStatement=False):
        """use the mysql sucker built into fundamentals package to import data into the crsossmatch catalogues mysql database (via insert scripts)

        **Key Arguments:**
            - ``dictList`` - a list of dictionaries containing all the rows in the milliquas catalogue
            - ``createStatement`` - the table's mysql create statement (used to generate table if it does not yet exist in database)

        **Usage:**

            .. code-block:: python 

                self._add_data_to_database_table(
                    dictList=dictList,
                    createStatement=createStatement
                )
        """
        self.log.info('starting the ``_add_data_to_database_table`` method')

        if len(dictList) == 0:
            return

        # RECURSIVELY CREATE MISSING DIRECTORIES
        if not os.path.exists("/tmp/myinserts/"):
            os.makedirs("/tmp/myinserts/")
        dataSet = list_of_dictionaries(
            log=self.log,
            listOfDictionaries=dictList,
            reDatetime=self.reDatetime
        )

        now = datetime.now()
        now = now.strftime("%Y%m%dt%H%M%S")
        filepath = "/tmp/myinserts/" + self.dbTableName + "-" + now + ".sql"
        mysqlData = dataSet.mysql(
            tableName=self.dbTableName, filepath=filepath, createStatement=createStatement)

        directory_script_runner(
            log=self.log,
            pathToScriptDirectory="/tmp/myinserts/",
            databaseName=self.settings["database settings"][
                "static catalogues"]["db"],
            loginPath=self.settings["database settings"][
                "static catalogues"]["loginPath"],
            successRule="delete",
            failureRule="failed"
        )

        self._add_htmids_to_database_table()
        self._update_database_helper_table()

        self.log.info('completed the ``_add_data_to_database_table`` method')
        return None

    def _add_htmids_to_database_table(
            self):
        """add htmids to database table once all the data has been imported
        """
        self.log.info('starting the ``add_htmids_to_database_table`` method')

        tableName = self.dbTableName

        self.log.info("Adding HTMIds to %(tableName)s" % locals())

        add_htm_ids_to_mysql_database_table(
            raColName=self.raColName,
            declColName=self.declColName,
            tableName=self.dbTableName,
            dbConn=self.cataloguesDbConn,
            log=self.log,
            primaryIdColumnName=self.primaryIdColumnName
        )

        self.log.info('completed the ``add_htmids_to_database_table`` method')
        return None

    def _update_database_helper_table(
            self):
        """ update database helper table
        """
        self.log.info('starting the ``_update_database_helper_table`` method')

        tableName = self.dbTableName

        sqlQuery = u"""
            update tcs_helper_catalogue_tables_info set last_updated = now() where table_name = "%(tableName)s";
        """ % locals()

        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
        )

        self.log.info('completed the ``_update_database_helper_table`` method')
        return None

    # use the tab-trigger below for new method
    # xt-class-method
