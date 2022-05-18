#!/usr/local/bin/python
# encoding: utf-8
"""
*Import ned stream into sherlock-catalogues database*

:Author:
    David Young
"""
from __future__ import print_function, division
from ._base_importer import _base_importer
from fundamentals.mysql import directory_script_runner, readquery, writequery
from fundamentals.renderer import list_of_dictionaries
from astrocalc.coords import unit_conversion
from fundamentals.mysql import insert_list_of_dictionaries_into_database_tables
from HMpTy.mysql import add_htm_ids_to_mysql_database_table
from neddy import namesearch, conesearch
from docopt import docopt
from datetime import datetime, date, time
import re
import string
import codecs
import pickle
import glob
import readline

from past.utils import old_div
import sys
import os
os.environ['TERM'] = 'vt100'


class ned(_base_importer):
    """
    *Using a list of coordinates, query the online* `NED <https://ned.ipac.caltech.edu/>`_ *database and import sources found within a given search radius of each of the loctions into the sherlock-catalogues database*

    The code:

        1. Uses the list of transient coordinates and queries NED (conesearch) for the results within the given search radius
        2. Creates the `tcs_cat_ned_stream` table if it doesn't exist
        3. Adds the resulting matched NED IDs/Names to the `tcs_cat_ned_stream` table
        4. Updates the NED query history table
        5. Queris NED via NED IDs (object search) for the remaining source metadata to be added to the `tcs_cat_ned_stream` table

    Note it's up to the user to filter the input coordinate list by checking whether or not the same area of the sky has been imported into the `tcs_cat_ned_stream` table recently (by checking the `tcs_helper_ned_query_history` table)

    **Key Arguments**

    - ``dbConn`` -- mysql database connection
    - ``log`` -- logger
    - ``settings`` -- the settings dictionary
    - ``coordinateList`` -- list of coordinates (a list of strings with RA and DEC space separated)
    - ``radiusArcsec`` - - the radius in arcsec with which to perform the initial NED conesearch. Default * False*


    **Usage**

    To import the ned catalogue stream, run the following:


      ```python
      from sherlock.imports import ned
      ```

        stream = ned(
            log=log,
            settings=settings,
            coordinateList=["23.12323 -12.34343","345.43234 45.26789"],
            radiusArcsec=180
        )
        stream.ingest()

    .. todo ::

        - test this code is still working after changes
        - add option to filter coordinate list via the `tcs_helper_ned_query_history` table
        - check sublime snippet exists
        - clip any useful text to docs mindmap
    """
    # INITIALISATION

    def ingest(self):
        """*Perform conesearches of the online NED database and import the results into a the sherlock-database*

        The code:

            1. uses the list of transient coordinates and queries NED for the results within the given search radius
            2. Creates the `tcs_cat_ned_stream` table if it doesn't exist
            3. Adds the resulting NED IDs/Names to the `tcs_cat_ned_stream` table
            4. Updates the NED query history table
            5. Queris NED via NED IDs for the remaining source metadata to be added to the `tcs_cat_ned_stream` table

        **Usage**

        Having setup the NED object with a coordinate list and cone-search radius, run the `ingest()` method

        ```python
        stream.ingest()
        ```


        .. todo ::

            - check sublime snippet exists
            - clip any useful text to docs mindmap
            - regenerate the docs and check redendering of this docstring
        """
        self.log.debug('starting the ``ingest`` method')

        if not self.radiusArcsec:
            self.log.error(
                'please give a radius in arcsec with which to preform the initial NED conesearch' % locals())
            sys.exit(0)

        # VARIABLES
        # SIZE OF NUMBER OF ROWS TO INSERT INTO DATABASE TABLE AT ANY ONE GO
        self.databaseInsertbatchSize = 10000

        # THE DATABASE TABLE TO STREAM THE NED DATA INTO
        self.dbTableName = "tcs_cat_ned_stream"

        dictList = self._create_dictionary_of_ned()

        tableName = self.dbTableName

        createStatement = """CREATE TABLE IF NOT EXISTS `%(tableName)s` (
  `primaryId` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'An internal counter',
  `ned_name` varchar(150) NOT NULL,
  `redshift` double DEFAULT NULL,
  `dateCreated` datetime DEFAULT CURRENT_TIMESTAMP,
  `dateLastModified` datetime DEFAULT CURRENT_TIMESTAMP,
  `updated` varchar(45) DEFAULT '0',
  `major_diameter_arcmin` double DEFAULT NULL,
  `ned_notes` varchar(700) DEFAULT NULL,
  `object_type` varchar(100) DEFAULT NULL,
  `redshift_err` double DEFAULT NULL,
  `redshift_quality` varchar(100) DEFAULT NULL,
  `magnitude_filter` varchar(10) DEFAULT NULL,
  `minor_diameter_arcmin` double DEFAULT NULL,
  `morphology` varchar(50) DEFAULT NULL,
  `hierarchy` varchar(50) DEFAULT NULL,
  `galaxy_morphology` varchar(50) DEFAULT NULL,
  `radio_morphology` varchar(50) DEFAULT NULL,
  `activity_type` varchar(50) DEFAULT NULL,
  `raDeg` double DEFAULT NULL,
  `decDeg` double DEFAULT NULL,
  `eb_v` double DEFAULT NULL,
  `htm16ID` bigint(20) DEFAULT NULL,
  `download_error` tinyint(1) DEFAULT '0',
  `htm10ID` bigint(20) DEFAULT NULL,
  `htm13ID` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`primaryId`),
  UNIQUE KEY `ned_name` (`ned_name`),
  KEY `idx_htm16ID` (`htm16ID`),
  KEY `raDeg` (`raDeg`),
  KEY `downloadError` (`download_error`),
  KEY `idx_htm10ID` (`htm10ID`),
  KEY `idx_htm13ID` (`htm13ID`)
) ENGINE=MyISAM AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
""" % locals()

        self.add_data_to_database_table(
            dictList=dictList,
            createStatement=createStatement
        )

        self._update_ned_query_history()
        self._download_ned_source_metadata()

        self.log.debug('completed the ``ingest`` method')
        return None

    def _create_dictionary_of_ned(
            self):
        """*Create a list of dictionaries containing all the object ids (NED names) in the ned stream*

        **Return**

        - ``dictList`` - a list of dictionaries containing all the object ids (NED names) in the ned stream


        **Usage**

        ```python
        dictList = stream._create_dictionary_of_ned()
        ```

        """
        self.log.debug(
            'starting the ``_create_dictionary_of_ned`` method')

        # GET THE NAMES (UNIQUE IDS) OF THE SOURCES WITHIN THE CONESEARCH FROM
        # NED
        names, searchParams = conesearch(
            log=self.log,
            radiusArcsec=self.radiusArcsec,
            nearestOnly=False,
            unclassified=True,
            quiet=False,
            listOfCoordinates=self.coordinateList,
            outputFilePath=False,
            verbose=False
        ).get_crossmatch_names()

        dictList = []
        dictList[:] = [{"ned_name": n} for n in names]

        self.log.debug(
            'completed the ``_create_dictionary_of_ned`` method')
        return dictList

    def _update_ned_query_history(
            self):
        """*Update the database helper table to give details of the ned cone searches performed*

        *Usage:*

            ```python
            stream._update_ned_query_history()
            ```
        """
        self.log.debug('starting the ``_update_ned_query_history`` method')

        myPid = self.myPid

        # ASTROCALC UNIT CONVERTER OBJECT
        converter = unit_conversion(
            log=self.log
        )

        # UPDATE THE DATABASE HELPER TABLE TO GIVE DETAILS OF THE NED CONE
        # SEARCHES PERFORMED
        dataList = []
        for i, coord in enumerate(self.coordinateList):
            if isinstance(coord, ("".__class__, u"".__class__)):
                ra = coord.split(" ")[0]
                dec = coord.split(" ")[1]
            elif isinstance(coord, tuple) or isinstance(coord, list):
                ra = coord[0]
                dec = coord[1]

            dataList.append(
                {"raDeg": ra,
                 "decDeg": dec,
                 "arcsecRadius": self.radiusArcsec}
            )

        if len(dataList) == 0:
            return None

        # CREATE TABLE IF NOT EXIST
        createStatement = """CREATE TABLE IF NOT EXISTS `tcs_helper_ned_query_history` (
  `primaryId` bigint(20) NOT NULL AUTO_INCREMENT,
  `raDeg` double DEFAULT NULL,
  `decDeg` double DEFAULT NULL,
  `dateCreated` datetime DEFAULT CURRENT_TIMESTAMP,
  `dateLastModified` datetime DEFAULT CURRENT_TIMESTAMP,
  `updated` varchar(45) DEFAULT '0',
  `arcsecRadius` int(11) DEFAULT NULL,
  `dateQueried` datetime DEFAULT CURRENT_TIMESTAMP,
  `htm16ID` bigint(20) DEFAULT NULL,
  `htm13ID` int(11) DEFAULT NULL,
  `htm10ID` int(11) DEFAULT NULL,
  PRIMARY KEY (`primaryId`),
  KEY `idx_htm16ID` (`htm16ID`),
  KEY `dateQueried` (`dateQueried`),
  KEY `dateHtm16` (`dateQueried`,`htm16ID`),
  KEY `idx_htm10ID` (`htm10ID`),
  KEY `idx_htm13ID` (`htm13ID`)
) ENGINE=MyISAM AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
        """
        writequery(
            log=self.log,
            sqlQuery=createStatement,
            dbConn=self.cataloguesDbConn
        )

        # USE dbSettings TO ACTIVATE MULTIPROCESSING
        insert_list_of_dictionaries_into_database_tables(
            dbConn=self.cataloguesDbConn,
            log=self.log,
            dictList=dataList,
            dbTableName="tcs_helper_ned_query_history",
            uniqueKeyList=[],
            dateModified=True,
            batchSize=10000,
            replace=True,
            dbSettings=self.settings["database settings"][
                "static catalogues"]
        )

        # INDEX THE TABLE FOR LATER SEARCHES
        add_htm_ids_to_mysql_database_table(
            raColName="raDeg",
            declColName="decDeg",
            tableName="tcs_helper_ned_query_history",
            dbConn=self.cataloguesDbConn,
            log=self.log,
            primaryIdColumnName="primaryId",
            dbSettings=self.settings["database settings"]["static catalogues"]
        )

        self.log.debug('completed the ``_update_ned_query_history`` method')
        return None

    def _download_ned_source_metadata(
            self):
        """*Query NED using the names of the NED sources in our local database to retrieve extra metadata*

        *Usage:*

            ```python
            stream._download_ned_source_metadata()
            ```
        """
        self.log.debug('starting the ``_download_ned_source_metadata`` method')

        self.dbTableName = "tcs_cat_ned_stream"

        total, batches = self._count_ned_sources_in_database_requiring_metadata()

        print(
            "%(total)s galaxies require metadata. Need to send %(batches)s batch requests to NED." % locals())

        self.log.info(
            "%(total)s galaxies require metadata. Need to send %(batches)s batch requests to NED." % locals())

        totalBatches = self.batches
        thisCount = 0

        # FOR EACH BATCH, GET THE GALAXY IDs, QUERY NED AND UPDATE THE DATABASE
        # THEN RECOUNT TO DETERMINE IF THERE ARE REMAINING SOURCES TO GRAB
        # METADATA FOR
        while self.total:
            thisCount += 1
            self._get_ned_sources_needing_metadata()
            self._do_ned_namesearch_queries_and_add_resulting_metadata_to_database(
                thisCount)
            self._count_ned_sources_in_database_requiring_metadata()

        self.log.debug(
            'completed the ``_download_ned_source_metadata`` method')
        return None

    def _get_ned_sources_needing_metadata(
            self):
        """*Get the names of 50000 or less NED sources that still require metabase in the database*

        **Return**

        - ``len(self.theseIds)`` -- the number of NED IDs returned


        *Usage:*

            ```python
            numberSources = stream._get_ned_sources_needing_metadata()
            ```
        """
        self.log.debug(
            'starting the ``_get_ned_sources_needing_metadata`` method')

        tableName = self.dbTableName

        # SELECT THE DATA FROM NED TABLE
        sqlQuery = u"""
            select ned_name from %(tableName)s where raDeg is null and (download_error != 1 or download_error is null) limit 50000;
        """ % locals()
        sqlQuery = u"""
            select ned_name from %(tableName)s where  (download_error != 1 or download_error is null) limit 50000;
        """ % locals()
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            quiet=False
        )

        self.theseIds = []
        self.theseIds[:] = [r["ned_name"].replace('"', '\\"') for r in rows]

        self.log.debug(
            'completed the ``_get_ned_sources_needing_metadata`` method')

        return len(self.theseIds)

    def _do_ned_namesearch_queries_and_add_resulting_metadata_to_database(
            self,
            batchCount):
        """*Query NED via name searcha and add result metadata to database*

        **Key Arguments**

        - ``batchCount`` - the index number of the batch sent to NED (only needed for printing to STDOUT to give user idea of progress)


        *Usage:*

            ```python
            numberSources = stream._do_ned_namesearch_queries_and_add_resulting_metadata_to_database(batchCount=10)
            ```
        """
        self.log.debug(
            'starting the ``_do_ned_namesearch_queries_and_add_resulting_metadata_to_database`` method')

        # ASTROCALC UNIT CONVERTER OBJECT
        converter = unit_conversion(
            log=self.log
        )
        tableName = self.dbTableName

        # QUERY NED WITH BATCH
        totalCount = len(self.theseIds)
        print("requesting metadata from NED for %(totalCount)s galaxies (batch %(batchCount)s)" % locals())
        # QUERY THE ONLINE NED DATABASE USING NEDDY'S NAMESEARCH METHOD
        search = namesearch(
            log=self.log,
            names=self.theseIds,
            quiet=True
        )
        results = search.get()
        print("results returned from ned -- starting to add to database" % locals())

        # CLEAN THE RETURNED DATA AND UPDATE DATABASE
        totalCount = len(results)
        count = 0
        sqlQuery = ""
        dictList = []
        for thisDict in results:
            thisDict["tableName"] = tableName
            count += 1
            for k, v in list(thisDict.items()):
                if not v or len(v) == 0:
                    thisDict[k] = "null"
                if k in ["major_diameter_arcmin", "minor_diameter_arcmin"] and (":" in v or "?" in v or "<" in v):
                    thisDict[k] = v.replace(":", "").replace(
                        "?", "").replace("<", "")
                if isinstance(v, ("".__class__, u"".__class__)) and '"' in v:
                    thisDict[k] = v.replace('"', '\\"')
            if "Input name not" not in thisDict["input_note"] and "Same object as" not in thisDict["input_note"]:
                try:
                    thisDict["raDeg"] = converter.ra_sexegesimal_to_decimal(
                        ra=thisDict["ra"]
                    )
                    thisDict["decDeg"] = converter.dec_sexegesimal_to_decimal(
                        dec=thisDict["dec"]
                    )
                except:
                    name = thisDict["input_name"]
                    self.log.warning(
                        "Could not convert the RA & DEC for the %(name)s NED source" % locals())
                    continue
                thisDict["eb_v"] = thisDict["eb-v"]
                thisDict["ned_name"] = thisDict["input_name"]
                row = {}
                for k in ["redshift_quality", "redshift", "hierarchy", "object_type", "major_diameter_arcmin", "morphology", "magnitude_filter", "ned_notes", "eb_v", "raDeg", "radio_morphology", "activity_type", "minor_diameter_arcmin", "decDeg", "redshift_err", "ned_name"]:
                    if thisDict[k] == "null":
                        row[k] = None
                    else:
                        row[k] = thisDict[k]

                if '"' in thisDict["ned_name"]:
                    print(thisDict)
                    print(thisDict["ned_name"])
                    sys.exit(0)

                dictList.append(row)

        self.add_data_to_database_table(
            dictList=dictList,
            createStatement="""SET SESSION sql_mode="";"""
        )

        theseIds = ("\", \"").join(self.theseIds)

        sqlQuery = u"""
            update %(tableName)s set download_error = 1 where ned_name in ("%(theseIds)s");
        """ % locals()
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
        )

        print("%(count)s/%(totalCount)s galaxy metadata batch entries added to database" % locals())
        if count < totalCount:
            # Cursor up one line and clear line
            sys.stdout.write("\x1b[1A\x1b[2K")

        sqlQuery = u"""
            update tcs_helper_catalogue_tables_info set last_updated = now() where table_name = "%(tableName)s"
        """ % locals()
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
        )

        self.log.debug(
            'completed the ``_do_ned_namesearch_queries_and_add_resulting_metadata_to_database`` method')
        return None

    def _count_ned_sources_in_database_requiring_metadata(
            self):
        """*Count the sources in the NED table requiring metadata*

        **Return**

        - ``self.total``, ``self.batches`` -- total number of galaxies needing metadata & the number of batches required to be sent to NED


        *Usage:*

            ```python
            totalRemaining, numberOfBatches = stream._count_ned_sources_in_database_requiring_metadata()
            ```
        """
        self.log.debug(
            'starting the ``_count_ned_sources_in_database_requiring_metadata`` method')

        tableName = self.dbTableName

        # sqlQuery = u"""
        #     select count(*) as count from %(tableName)s where raDeg is null and (download_error != 1 or download_error is null)
        # """ % locals()
        sqlQuery = u"""
            select count(*) as count from %(tableName)s where (download_error != 1 or download_error is null)
        """ % locals()
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            quiet=False
        )
        self.total = rows[0]["count"]
        self.batches = int(old_div(self.total, 50000.)) + 1

        if self.total == 0:
            self.batches = 0

        self.log.debug(
            'completed the ``_count_ned_sources_in_database_requiring_metadata`` method')
        return self.total, self.batches

    # use the tab-trigger below for new method
    # xt-class-method
