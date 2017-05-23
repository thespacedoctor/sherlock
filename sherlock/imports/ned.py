#!/usr/local/bin/python
# encoding: utf-8
"""
*import ned stream into sherlock's crossmatch catalogues database*

:Author:
    David Young

:Date Created:
    December 13, 2016
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
from datetime import datetime, date, time
from docopt import docopt
from neddy import namesearch, conesearch
from HMpTy.mysql import add_htm_ids_to_mysql_database_table
from astrocalc.coords import unit_conversion
from fundamentals.renderer import list_of_dictionaries
from fundamentals.mysql import directory_script_runner, readquery, writequery
from ._base_importer import _base_importer


class ned(_base_importer):

    """
    *importer object for the* `NED <https://ned.ipac.caltech.edu/>`_ *galaxy stream*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary
        - ``coordinateList`` -- list of coordinates (a list of strings with RA and DEC space separated)
        - ``radiusArcsec`` - - the radius in arcsec with which to perform the initial NED conesearch. Default * False*

    **Usage:**

      To import the ned catalogue stream, run the following:

      .. code-block:: python 

        from sherlock.imports import ned
        stream = ned(
            log=log,
            settings=settings,
            coordinateList=["23.12323 -12.34343","345.43234 45.26789"],
            radiusArcsec=180
        )
        stream.ingest()
    """
    # INITIALISATION

    def ingest(self):
        """perform conesearches of the online NED database and import the results into a local database table

        **Usage:**

            Having setup the NED object with a coordinate list and cone-search radius, run the `ingest()` method

            .. code-block:: python

                ned.ingest()
        """
        self.log.info('starting the ``ingest`` method')

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

        self._add_data_to_database_table(
            dictList=dictList,
            createStatement=createStatement
        )

        self._update_ned_query_history()
        self._download_ned_source_metadata()

        self.log.info('completed the ``ingest`` method')
        return None

    def _create_dictionary_of_ned(
            self):
        """create a list of dictionaries containing all the rows in the ned stream

        **Return:**
            - ``dictList`` - a list of dictionaries containing all the rows in the ned stream
        """
        self.log.info(
            'starting the ``_create_dictionary_of_ned`` method')

        # GET THE NAMES (UNIQUE IDS) OF THE SOURCES WITHIN THE CONESEARCH
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

        self.log.info(
            'completed the ``_create_dictionary_of_ned`` method')
        return dictList

    def _update_ned_query_history(
            self):
        """*update the database helper table to give details of the ned cone searches performed*
        """
        self.log.info('starting the ``_update_ned_query_history`` method')

        # ASTROCALC UNIT CONVERTER OBJECT
        converter = unit_conversion(
            log=self.log
        )

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

        # UPDATE THE DATABASE HELPER TABLE TO GIVE DETAILS OF THE NED CONE
        # SEARCHES PERFORMED
        dataList = []
        for i, coord in enumerate(self.coordinateList):
            if isinstance(coord, str):
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

        dataSet = list_of_dictionaries(
            log=self.log,
            listOfDictionaries=dataList,
            reDatetime=self.reDatetime
        )
        # Recursively create missing directories
        if not os.path.exists("/tmp/mysql-inserts"):
            os.makedirs("/tmp/mysql-inserts")
        now = datetime.now()
        now = now.strftime("%Y%m%dt%H%M%S")
        mysqlData = dataSet.mysql(
            tableName="tcs_helper_ned_query_history", filepath="/tmp/mysql-inserts/ned-updates-%(now)s.sql" % locals(), createStatement=createStatement)

        directory_script_runner(
            log=self.log,
            pathToScriptDirectory="/tmp/mysql-inserts",
            databaseName=self.settings["database settings"][
                "static catalogues"]["db"],
            loginPath=self.settings["database settings"][
                "static catalogues"]["loginPath"],
            successRule="delete",
            failureRule="failed"
        )

        # INDEX THE TABLE FOR LATER SEARCHES
        add_htm_ids_to_mysql_database_table(
            raColName="raDeg",
            declColName="decDeg",
            tableName="tcs_helper_ned_query_history",
            dbConn=self.cataloguesDbConn,
            log=self.log,
            primaryIdColumnName="primaryId"
        )

        self.log.info('completed the ``_update_ned_query_history`` method')
        return None

    def _download_ned_source_metadata(
            self):
        """query NED using the names of the NED sources in our local database to retrieve extra metadata
        """
        self.log.info('starting the ``_download_ned_source_metadata`` method')

        self.dbTableName = "tcs_cat_ned_stream"

        total, batches = self._count_ned_sources_in_database_requiring_metadata()

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

        self.log.info('completed the ``_download_ned_source_metadata`` method')
        return None

    def _get_ned_sources_needing_metadata(
            self):
        """get the names of 50000 or less NED sources that still require metabase in the database

        **Return:**
            - ``len(self.theseIds)`` -- the number of NED IDs returned
        """
        self.log.info(
            'starting the ``_get_ned_sources_needing_metadata`` method')

        tableName = self.dbTableName

        # SELECT THE DATA FROM NED TABLE
        sqlQuery = u"""
            select ned_name from %(tableName)s where raDeg is null and (download_error != 1 or download_error is null) limit 50000;
        """ % locals()
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            quiet=False
        )

        self.theseIds = []
        self.theseIds[:] = [r["ned_name"] for r in rows]

        self.log.info(
            'completed the ``_get_ned_sources_needing_metadata`` method')

        return len(self.theseIds)

    def _do_ned_namesearch_queries_and_add_resulting_metadata_to_database(
            self,
            batchCount):
        """ query ned and add results to database

        **Key Arguments:**
            - ``batchCount`` - the index number of the batch sent to NED
        """
        self.log.info(
            'starting the ``_do_ned_namesearch_queries_and_add_resulting_metadata_to_database`` method')

        # ASTROCALC UNIT CONVERTER OBJECT
        converter = unit_conversion(
            log=self.log
        )
        tableName = self.dbTableName

        # QUERY NED WITH BATCH
        totalCount = len(self.theseIds)
        print "requesting metadata from NED for %(totalCount)s galaxies (batch %(batchCount)s)" % locals()
        # QUERY THE ONLINE NED DATABASE USING NEDDY'S NAMESEARCH METHOD
        search = namesearch(
            log=self.log,
            names=self.theseIds,
            quiet=True
        )
        results = search.get()
        print "results returned from ned -- starting to add to database" % locals()

        # CLEAN THE RETURNED DATA AND UPDATE DATABASE
        totalCount = len(results)
        count = 0
        sqlQuery = ""
        dictList = []
        for thisDict in results:
            thisDict["tableName"] = tableName
            count += 1
            for k, v in thisDict.iteritems():
                if not v or len(v) == 0:
                    thisDict[k] = "null"
                if k in ["major_diameter_arcmin", "minor_diameter_arcmin"] and (":" in v or "?" in v or "<" in v):
                    thisDict[k] = v.replace(":", "").replace(
                        "?", "").replace("<", "")
                if isinstance(v, str) and '"' in v:
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

                dictList.append(row)

        self._add_data_to_database_table(
            dictList=dictList,
            createStatement="""SET SESSION sql_mode="";"""
        )

        theseIds = ("\", \"").join(self.theseIds)

        sqlQuery = u"""
            update %(tableName)s set download_error = 1 where ned_name = ("%(theseIds)s");
        """ % locals()
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
        )

        print "%(count)s/%(totalCount)s galaxy metadata batch entries added to database" % locals()
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

        self.log.info(
            'completed the ``_do_ned_namesearch_queries_and_add_resulting_metadata_to_database`` method')
        return None

    def _count_ned_sources_in_database_requiring_metadata(
            self):
        """count the sources in the NED table requiring metadata

        **Return:**
            - ``self.total``, ``self.batches`` -- total number of galaxies needing metadata & the number of batches required to be sent to NED
        """
        self.log.info(
            'starting the ``_count_ned_sources_in_database_requiring_metadata`` method')

        tableName = self.dbTableName

        sqlQuery = u"""
            select count(*) as count from %(tableName)s where raDeg is null and (download_error != 1 or download_error is null)
        """ % locals()
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            quiet=False
        )
        self.total = rows[0]["count"]
        self.batches = int(self.total / 50000.) + 1

        if self.total == 0:
            self.batches = 0

        self.log.info(
            'completed the ``_count_ned_sources_in_database_requiring_metadata`` method')
        return self.total, self.batches

    # use the tab-trigger below for new method
    # xt-class-method
