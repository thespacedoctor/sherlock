#!/usr/local/bin/python
# encoding: utf-8
"""
*The base importer for sherlock catalogue imports*

:Author:
    David Young

.. todo ::

    - document this module
"""
from __future__ import print_function
from builtins import str
from builtins import object
import sys
import os
os.environ['TERM'] = 'vt100'
import readline
import glob
import pickle
import codecs
import re
import string
from sherlock.database_cleaner import database_cleaner
from datetime import datetime, date, time
from docopt import docopt
from fundamentals.mysql import insert_list_of_dictionaries_into_database_tables, directory_script_runner, writequery
from fundamentals.renderer import list_of_dictionaries
from HMpTy.mysql import add_htm_ids_to_mysql_database_table


class _base_importer(object):
    """
    *The base importer object used to import new catalgues into sherlock-catalogues database*

    **Key Arguments**

    - ``log`` -- logger
    - ``settings`` -- the settings dictionary
    - ``pathToDataFIle`` -- path to the file containing the data to import
    - ``version`` -- version number of the catalogue to be imported (e.g. DR12)
    - ``catalogueName`` -- name of the catalogue to be imported
    - ``coordinateList`` -- list of coordinates (needed for some streamed tables)
    - ``radiusArcsec`` -- the radius in arcsec with which to perform the initial NED conesearch. Default *False*


    **Usage**

    To use this base class to write a new importer, create your class like so:

        ```python
        class newImporter(_base_importer):
            ...
        ```

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
        self.myPid = str(os.getpid())
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

        # OPEN THE FILE TO IMPORT THE DATA FROM
        if pathToDataFile:
            pathToReadFile = pathToDataFile
            try:
                self.log.debug("attempting to open the file %s" %
                               (pathToReadFile,))
                readFile = codecs.open(pathToReadFile, mode='r')
                self.catData = readFile.read()
                readFile.close()
            except IOError as e:
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

    def add_data_to_database_table(
            self,
            dictList,
            createStatement=False):
        """*Import data in the list of dictionaries in the requested database table*

        Also adds HTMIDs and updates the sherlock-catalogue database helper table with the time-stamp of when the imported catlogue was last updated

        **Key Arguments**

        - ``dictList`` - a list of dictionaries containing all the rows in the catalogue to be imported
        - ``createStatement`` - the table's mysql create statement (used to generate table if it does not yet exist in database). Default *False*


        **Usage**

        ```python
        self.add_data_to_database_table(
            dictList=dictList,
            createStatement=createStatement
        )
        ```


        .. todo ::

            - Write a checklist for creating a new sherlock database importer
        """
        self.log.debug('starting the ``add_data_to_database_table`` method')

        if len(dictList) == 0:
            return

        myPid = self.myPid
        dbTableName = self.dbTableName

        if createStatement:
            writequery(
                log=self.log,
                sqlQuery=createStatement,
                dbConn=self.cataloguesDbConn,
            )

        insert_list_of_dictionaries_into_database_tables(
            dbConn=self.cataloguesDbConn,
            log=self.log,
            dictList=dictList,
            dbTableName=dbTableName,
            uniqueKeyList=[],
            dateModified=True,
            dateCreated=True,
            batchSize=10000,
            replace=True,
            dbSettings=self.settings["database settings"][
                "static catalogues"]
        )

        self._add_htmids_to_database_table()

        cleaner = database_cleaner(
            log=self.log,
            settings=self.settings
        )
        cleaner._update_tcs_helper_catalogue_tables_info_with_new_tables()

        self._update_database_helper_table()

        if "ned_stream" not in dbTableName:
            print("""Now:

    - [ ] edit the `%(dbTableName)s` row in the sherlock catalogues database adding relevant column mappings, catalogue version number etc
    - [ ] retire any previous version of this catlogue in the database. Renaming the catalogue-table by appending `legacy_` and also change the name in the `tcs_helper_catalogue_tables_info` table
    - [ ] dupliate views from the previous catalogue version to point towards the new version and then delete the old views
    - [ ] run the command `sherlock clean [-s <pathToSettingsFile>]` to clean up helper tables
    - [ ] switch out the old catalogue table/views in your sherlock search algorithms in the yaml settings files
    - [ ] run a test batch of transients to make sure catalogue is installed as expected

    """ % locals())

        self.log.debug('completed the ``add_data_to_database_table`` method')
        return None

    def _add_htmids_to_database_table(
            self):
        """*Add HTMIDs to database table once all the data has been imported (HTM Levels 10,13,16)*

        **Usage**

        ```python
        self._add_htmids_to_database_table()
        ```

        """
        self.log.debug('starting the ``add_htmids_to_database_table`` method')

        tableName = self.dbTableName

        self.log.info("Adding HTMIds to %(tableName)s" % locals())

        add_htm_ids_to_mysql_database_table(
            raColName=self.raColName,
            declColName=self.declColName,
            tableName=self.dbTableName,
            dbConn=self.cataloguesDbConn,
            log=self.log,
            primaryIdColumnName=self.primaryIdColumnName,
            dbSettings=self.settings["database settings"]["static catalogues"]
        )

        self.log.debug('completed the ``add_htmids_to_database_table`` method')
        return None

    def _update_database_helper_table(
            self):
        """*Update the sherlock catalogues database helper table with the time-stamp of when this catlogue was last updated*

        **Usage**

        ```python
        self._update_database_helper_table()
        ```

        """
        self.log.debug('starting the ``_update_database_helper_table`` method')

        tableName = self.dbTableName

        sqlQuery = u"""
            update tcs_helper_catalogue_tables_info set last_updated = now() where table_name = "%(tableName)s";
        """ % locals()

        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
        )

        self.log.debug(
            'completed the ``_update_database_helper_table`` method')
        return None

    # use the tab-trigger below for new method
    # xt-class-method
