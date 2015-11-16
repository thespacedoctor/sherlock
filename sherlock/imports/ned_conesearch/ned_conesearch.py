#!/usr/local/bin/python
# encoding: utf-8
"""
ned_conesearch.py
=================
:Summary:
    Build and import NED-D table in datasbe

:Author:
    David Young

:Date Created:
    September 7, 2015

:dryx syntax:
    - ``_someObject`` = a 'private' object that should only be changed for debugging

:Notes:
    - If you have any questions requiring this script/module please email me: d.r.young@qub.ac.uk
"""
################# GLOBAL IMPORTS ####################
import sys
import os
os.environ['TERM'] = 'vt100'
import readline
import glob
import pickle
import csv
import time
from docopt import docopt
from dryxPython import logs as dl
from dryxPython import commonutils as dcu
from dryxPython import mysql as dms
from dryxPython import astrotools as dat
from neddy import namesearch
from dryxPython.projectsetup import setup_main_clutil
from .._base_importer import _base_importer
from . import *


class ned_conesearch(_base_importer):

    """
    The worker class for the ned_conesearch importer module
    """

    def get(self):
        """import the NED-D database into the database and do other cleanup tricks.
        """
        self.log.info('starting the ``get`` method')

        self.dbTableName = "tcs_cat_ned_stream"

        self._get_ned_names()
        self._update_ned_query_history()
        self.databaseInsertbatchSize = 10000
        self.get_metadata_for_galaxies()
        self.add_htmids_to_database_table()

        self.log.info('completed the ``get`` method')
        return ned_conesearch

    def get_metadata_for_galaxies(
            self):
        """get metadata for galaxies
        """
        self.log.info('starting the ``get_metadata_for_galaxies`` method')

        self.batchSize = 5000.

        total, batches = self._count_galaxies_requiring_metadata()
        print "%(total)s NED sources require metadata. Need to send %(batches)s batch requests to NED." % locals()

        totalBatches = self.batches
        thisCount = 0

        # FOR EACH BATCH, GET THE GALAXY IDs, QUERY NED AND UPDATE THE DATABASE
        while self.total:
            thisCount += 1
            self._get_single_batch_of_galaxies_needing_metadata()
            self._query_ned_and_add_results_to_database(thisCount)
            total, batches = self._count_galaxies_requiring_metadata()
            print "%(total)s NED sources still require metadata." % locals()

        self.log.info('completed the ``get_metadata_for_galaxies`` method')
        return None

    def _count_galaxies_requiring_metadata(
            self):
        """ count galaxies requiring metadata

        **Return:**
            - ``self.total``, ``self.batches`` -- total number of galaxies needing metadata & the number of batches required to be sent to NED
        """
        self.log.info(
            'starting the ``_count_galaxies_requiring_metadata`` method')

        tableName = self.dbTableName

        sqlQuery = u"""
            select count(*) as count from %(tableName)s where raDeg is null and download_error != 1
        """ % locals()
        rows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )
        self.total = rows[0]["count"]
        self.batches = int(self.total / self.batchSize) + 1

        if self.total == 0:
            self.batches = 0

        self.log.info(
            'completed the ``_count_galaxies_requiring_metadata`` method')
        return self.total, self.batches

    def _get_single_batch_of_galaxies_needing_metadata(
            self):
        """ get batch of galaxies needing metadata

        **Return:**
            - ``len(self.theseIds)`` -- the number of NED IDs returned
        """
        self.log.info(
            'starting the ``_get_single_batch_of_galaxies_needing_metadata`` method')

        bs = int(self.batchSize)
        tableName = self.dbTableName

        print "Requesting the next %(bs)s sources requiring metadata from the %(tableName)s table" % locals()

        # SELECT THE DATA FROM NED TABLE
        self.theseIds = []

        sqlQuery = u"""
            select ned_name from %(tableName)s where raDeg is null and download_error != 1 limit %(bs)s;
        """ % locals()
        rows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )
        for row in rows:
            self.theseIds.append(row["ned_name"])

        self.log.info(
            'completed the ``_get_single_batch_of_galaxies_needing_metadata`` method')

        return len(self.theseIds)

    def _query_ned_and_add_results_to_database(
            self,
            batchCount):
        """ query ned and add results to database

        **Key Arguments:**
            - ``batchCount`` - the index number of the batch sent to NED
        """
        self.log.info(
            'starting the ``_query_ned_and_add_results_to_database`` method')

        from datetime import datetime, date, time
        tableName = self.dbTableName

        # QUERY NED WITH BATCH
        totalCount = len(self.theseIds)
        print "requesting metadata from NED for %(totalCount)s galaxies (batch %(batchCount)s)" % locals()
        search = namesearch(
            log=self.log,
            names=self.theseIds,
            quiet=True
        )
        results = search.get()
        numResults = len(results)
        print "%(numResults)s results returned from ned -- starting to add to database ..." % locals()

        # CLEAN THE RETURNED DATA AND UPDATE DATABASE
        totalCount = len(results)
        count = 0
        sqlQuery = ""
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
                thisDict[
                    "raDeg"] = dat.ra_sexegesimal_to_decimal.ra_sexegesimal_to_decimal(thisDict["ra"])
                thisDict[
                    "decDeg"] = dat.declination_sexegesimal_to_decimal.declination_sexegesimal_to_decimal(thisDict["dec"])

                sqlQuery += u"""
                    update %(tableName)s
                        set redshift_quality = "%(redshift_quality)s",
                        redshift = %(redshift)s,
                        hierarchy = "%(hierarchy)s",
                        object_type = "%(object_type)s",
                        major_diameter_arcmin = %(major_diameter_arcmin)s,
                        morphology = "%(morphology)s",
                        magnitude_filter = "%(magnitude_filter)s",
                        ned_notes = "%(ned_notes)s",
                        eb_v = %(eb-v)s,
                        raDeg = %(raDeg)s,
                        radio_morphology = "%(radio_morphology)s",
                        activity_type = "%(activity_type)s",
                        minor_diameter_arcmin = %(minor_diameter_arcmin)s,
                        decDeg = %(decDeg)s,
                        redshift_err = %(redshift_err)s
                    where ned_name = "%(input_name)s";\n
                """ % thisDict
        sqlQuery = sqlQuery.replace('"null"', 'null')

        if len(sqlQuery) != 0:

            startTime = dcu.get_now_sql_datetime()
            print "Executing the sql query ..."
            rows = dms.execute_mysql_read_query(
                sqlQuery=sqlQuery,
                dbConn=self.cataloguesDbConn,
                log=self.log
            )
            endTime = dcu.get_now_sql_datetime()
            runningTime = dcu.calculate_time_difference(startTime, endTime)
            percent = (float(count) / float(totalCount)) * 100.
            # sys.stdout.write("\x1b[1A")
            print "%(count)s / %(totalCount)s (%(percent)1.1f%%) message" % locals()
            print "... and finished (time taken: %(runningTime)s) " % locals()
        else:
            for thisId in self.theseIds:
                sqlQuery = u"""
                    update %(tableName)s set download_error = 1 where ned_name = "%(thisId)s"
                """ % locals()
                dms.execute_mysql_write_query(
                    sqlQuery=sqlQuery,
                    dbConn=self.cataloguesDbConn,
                    log=self.log
                )

        print "%(count)s/%(totalCount)s galaxy metadata batch entries added to database" % locals()
        if count < totalCount:
            # Cursor up one line and clear line
            sys.stdout.write("\x1b[1A\x1b[2K")

        sqlQuery = u"""
            update tcs_helper_catalogue_tables_info set last_updated = now() where table_name = "%(tableName)s"
        """ % locals()
        dms.execute_mysql_write_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )

        self.log.info(
            'completed the ``_query_ned_and_add_results_to_database`` method')
        return None

    def _get_ned_names(
            self):
        """ get ned names

        **Key Arguments:**
            # -

        **Return:**
            - None

        **Todo**
            - @review: when complete, clean _get_ned_names method
            - @review: when complete add logging
        """
        self.log.info('starting the ``_get_ned_names`` method')

        from neddy import conesearch

        tableName = self.dbTableName

        numSearches = len(self.coordinateList)
        if numSearches:
            print "Requesting the names of the NED sources found within %(numSearches)s conesearch areas" % locals()

        names, searchParams = conesearch(
            log=self.log,
            radiusArcsec=self.settings["ned stream search radius arcec"],
            nearestOnly=False,
            unclassified=True,
            quiet=False,
            listOfCoordinates=self.coordinateList,
            outputFilePath=False,
            verbose=False
        ).get_crossmatch_names()

        manyValueList = []
        for n in names:
            manyValueList.append((n,))

        numResults = len(manyValueList)
        if numSearches:
            print "Inserting the names of the %(numResults)s matched NED sources into `tcs_cat_ned_stream` table" % locals()
            print ""

        sqlQuery = u"""
            INSERT ignore into tcs_cat_ned_stream (ned_name, dateCreated) values (%s, now())
        """
        dms.execute_mysql_write_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log,
            manyValueList=manyValueList)

        self.log.info('completed the ``_get_ned_names`` method')
        return None

    # use the tab-trigger below for new method
    def _update_ned_query_history(
            self):
        """ update ned query history

        **Key Arguments:**
            - ``coordinateList`` - the coordinates that where queried

        **Return:**
            - None

        **Todo**
            - @review: when complete, clean _update_ned_query_history method
            - @review: when complete add logging
        """
        self.log.info('starting the ``_update_ned_query_history`` method')

        from dryxPython import mysql as dms

        manyValueList = []
        radius = self.settings["ned stream search radius arcec"]
        from datetime import datetime, date, time
        now = datetime.now()
        now = now.strftime("%Y-%m-%d %H:%M:%S")

        for i, coord in enumerate(self.coordinateList):
            coord = coord.split(" ")
            manyValueList.append((coord[0], coord[1], radius, now))

        num = len(self.coordinateList)
        if num:
            print "Updating the `tcs_helper_ned_query_history` for the %(num)s new initial conesearches just performed" % locals()

        sqlQuery = u"""
            INSERT into tcs_helper_ned_query_history (raDeg, decDeg, arcsecRadius, dateQueried) values (%s, %s, %s, %s)
        """
        dms.execute_mysql_write_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log,
            manyValueList=manyValueList
        )

        dms.add_HTMIds_to_mysql_tables.add_HTMIds_to_mysql_tables(
            raColName="raDeg",
            declColName="decDeg",
            tableName="tcs_helper_ned_query_history",
            dbConn=self.cataloguesDbConn,
            log=self.log,
            primaryIdColumnName="primaryId"
        )

        self.log.info('completed the ``_update_ned_query_history`` method')
        return None

    # use the tab-trigger below for new method
    # xt-class-method
