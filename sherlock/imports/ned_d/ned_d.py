#!/usr/local/bin/python
# encoding: utf-8
"""
ned_d.py
========
:Summary:
    Build and import NED-D table in datasbe

:Author:
    David Young

:Date Created:
    September 7, 2015

:dryx syntax:
    - ``_someObject`` = a 'private' object that should only be changed for debugging

:Notes:
    - If you have any questions requiring this script/module please email me: davidrobertyoung@gmail.com
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
from fundamentals import tools, times
from .._base_importer import _base_importer
from . import *


class ned_d(_base_importer):

    """
    The worker class for the ned_d importer module
    """

    def get(self):
        """import the NED-D database into the database and do other cleanup tricks.
        """
        self.log.info('starting the ``get`` method')

        self.databaseInsertbatchSize = 10000
        self.uniqueKeyList = ["galaxy_index_id", "dist_index_id"]
        self.generate_the_database_table()
        self.dictList = self.create_dictionary_of_nedd()
        self.add_data_to_database_table()
        self.clean_up_columns()
        self.create_the_master_view()
        self.get_metadata_for_galaxies()
        self.add_htmids_to_database_table()
        self._update_sdss_coverage()

        self.log.info('completed the ``get`` method')
        return ned_d

    def generate_the_database_table(
            self):
        """generate the NED-D database table
        """
        self.log.info('starting the ``generate_the_database_table`` method')

        tableName = self.dbTableName

        sqlQuery = u"""
            CREATE TABLE `%(tableName)s` (
              `primaryId` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'An internal counter',
              `Method` varchar(150) DEFAULT NULL,
              `dateCreated` datetime DEFAULT NULL,
              `dist_derived_from_sn` varchar(150) DEFAULT NULL,
              `dist_in_ned_flag` varchar(10) DEFAULT NULL,
              `dist_index_id` mediumint(9) DEFAULT NULL,
              `dist_mod` double DEFAULT NULL,
              `dist_mod_err` double DEFAULT NULL,
              `dist_mpc` double DEFAULT NULL,
              `galaxy_index_id` mediumint(9) DEFAULT NULL,
              `hubble_const` double DEFAULT NULL,
              `lmc_mod` double DEFAULT NULL,
              `notes` varchar(500) DEFAULT NULL,
              `primary_ned_id` varchar(150) DEFAULT NULL,
              `redshift` double DEFAULT NULL,
              `ref` varchar(150) DEFAULT NULL,
              `ref_date` int(11) DEFAULT NULL,
              `master_row` tinyint(4) DEFAULT '0',
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
              `in_ned` tinyint(4) DEFAULT NULL,
              `raDeg` double DEFAULT NULL,
              `decDeg` double DEFAULT NULL,
              `eb_v` double DEFAULT NULL,
              `sdss_coverage` TINYINT DEFAULT NULL,
              PRIMARY KEY (`primaryId`),
              UNIQUE KEY `galaxy_index_id_dist_index_id` (`galaxy_index_id`,`dist_index_id`)
            ) ENGINE=MyISAM AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
        """ % locals()
        try:
            dms.execute_mysql_write_query(
                sqlQuery=sqlQuery,
                dbConn=self.cataloguesDbConn,
                log=self.log
            )
        except:
            self.log.info(
                '`%(tableName)s` table already exists - continuing as normal' % locals())

        self.log.info('completed the ``generate_the_database_table`` method')
        return None

    def create_dictionary_of_nedd(
            self):
        """create a list of dictionaries for the returned ned data
        """
        self.log.info('starting the ``import_nedcsv_to_database`` method')

        count = 0
        with open(self.pathToDataFile, 'rb') as csvFile:
            csvReader = csv.reader(
                csvFile, dialect='excel', delimiter=',', quotechar='"')
            totalRows = sum(1 for row in csvReader)
        csvFile.close()
        totalCount = totalRows

        with open(self.pathToDataFile, 'rb') as csvFile:
            csvReader = csv.reader(
                csvFile, dialect='excel', delimiter=',', quotechar='"')
            theseKeys = []
            dictList = []
            for row in csvReader:
                if len(theseKeys) == 0:
                    totalRows -= 1
                if "Exclusion Code" in row and "Hubble const." in row:
                    for i in row:
                        if i == "redshift (z)":
                            theseKeys.append("redshift")
                        elif i == "Hubble const.":
                            theseKeys.append("hubble_const")
                        elif i == "G":
                            theseKeys.append("galaxy_index_id")
                        elif i == "err":
                            theseKeys.append("dist_mod_err")
                        elif i == "D (Mpc)":
                            theseKeys.append("dist_mpc")
                        elif i == "Date (Yr. - 1980)":
                            theseKeys.append("ref_date")
                        elif i == "REFCODE":
                            theseKeys.append("ref")
                        elif i == "Exclusion Code":
                            theseKeys.append("dist_in_ned_flag")
                        elif i == "Adopted LMC modulus":
                            theseKeys.append("lmc_mod")
                        elif i == "m-M":
                            theseKeys.append("dist_mod")
                        elif i == "Notes":
                            theseKeys.append("notes")
                        elif i == "SN ID":
                            theseKeys.append("dist_derived_from_sn")
                        elif i == "method":
                            theseKeys.append("dist_method")
                        elif i == "Galaxy ID":
                            theseKeys.append("primary_ned_id")
                        elif i == "D":
                            theseKeys.append("dist_index_id")
                        else:
                            theseKeys.append(i)
                    continue

                if len(theseKeys):
                    count += 1
                    if count > 1:
                        # Cursor up one line and clear line
                        sys.stdout.write("\x1b[1A\x1b[2K")
                    if count > totalCount:
                        count = totalCount
                    percent = (float(count) / float(totalCount)) * 100.
                    print "%(count)s / %(totalCount)s (%(percent)1.1f%%) rows added to memmory" % locals()
                    rowDict = {}
                    for t, r in zip(theseKeys, row):
                        rowDict[t] = r
                        if t == "ref_date":
                            try:
                                rowDict[t] = int(r) + 1980
                            except:
                                rowDict[t] = None

                    if rowDict["dist_index_id"] != "999999":
                        dictList.append(rowDict)

        csvFile.close()

        self.log.info('completed the ``import_nedcsv_to_database`` method')
        return dictList

    def clean_up_columns(
            self):
        """clean up columns of the NED table
        """
        self.log.info('starting the ``clean_up_columns`` method')

        tableName = self.dbTableName

        print "cleaning up %(tableName)s columns" % locals()

        sqlQuery = u"""
            update %(tableName)s set dist_mod_err = null where dist_mod_err = 0;
            update %(tableName)s set dist_in_ned_flag = null where dist_in_ned_flag = "";
            update %(tableName)s set notes = null where notes = "";
            update %(tableName)s set redshift = null where redshift = 0;
            update %(tableName)s set dist_derived_from_sn = null where dist_derived_from_sn = "";
            update %(tableName)s set hubble_const = null where hubble_const = 0;
            update %(tableName)s set lmc_mod = null where lmc_mod = 0;
            update %(tableName)s set master_row = 0;
            update %(tableName)s set master_row = 1 where primaryId in (select * from (select distinct primaryId from %(tableName)s group by galaxy_index_id) as alias);
        """ % locals()
        dms.execute_mysql_write_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )

        self.log.info('completed the ``clean_up_columns`` method')
        return None

    def create_the_master_view(
            self):
        """create the master view
        """
        self.log.info('starting the ``create_the_master_view`` method')

        tableName = self.dbTableName

        print "creating view for %(tableName)s table" % locals()

        sqlQuery = u"""
            DROP VIEW IF EXISTS `view_%(tableName)s_master_recorders`;
        """ % locals()
        dms.execute_mysql_write_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )
        sqlQuery = u"""
            CREATE
                VIEW `view_%(tableName)s_master_recorders` AS
                    (SELECT 
                        `%(tableName)s`.`primary_ned_id` AS `primary_ned_id`,
                        `%(tableName)s`.`object_type` AS `object_type`,
                        `%(tableName)s`.`raDeg` AS `raDeg`,
                        `%(tableName)s`.`decDeg` AS `decDeg`,
                        `%(tableName)s`.`dist_mpc` AS `dist_mpc`,
                        `%(tableName)s`.`dist_mod` AS `dist_mod`,
                        `%(tableName)s`.`dist_mod_err` AS `dist_mod_err`,
                        `%(tableName)s`.`Method` AS `dist_measurement_method`,
                        `%(tableName)s`.`redshift` AS `redshift`,
                        `%(tableName)s`.`redshift_err` AS `redshift_err`,
                        `%(tableName)s`.`redshift_quality` AS `redshift_quality`,
                        `%(tableName)s`.`major_diameter_arcmin` AS `major_diameter_arcmin`,
                        `%(tableName)s`.`minor_diameter_arcmin` AS `minor_diameter_arcmin`,
                        `%(tableName)s`.`magnitude_filter` AS `magnitude_filter`,
                        `%(tableName)s`.`eb_v` AS `gal_eb_v`,
                        `%(tableName)s`.`hierarchy` AS `hierarchy`,
                        `%(tableName)s`.`morphology` AS `morphology`,
                        `%(tableName)s`.`radio_morphology` AS `radio_morphology`,
                        `%(tableName)s`.`activity_type` AS `activity_type`,
                        `%(tableName)s`.`ned_notes` AS `ned_notes`,
                        `%(tableName)s`.`in_ned` AS `in_ned`,
                        `%(tableName)s`.`primaryId` AS `primaryId`
                    FROM
                        `%(tableName)s`
                    WHERE
                        (`%(tableName)s`.`master_row` = 1))
        """ % locals()
        dms.execute_mysql_write_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )

        self.log.info('completed the ``create_the_master_view`` method')
        return None

    def get_metadata_for_galaxies(
            self):
        """get metadata for galaxies
        """
        self.log.info('starting the ``get_metadata_for_galaxies`` method')

        total, batches = self._count_galaxies_requiring_metadata()
        print "%(total)s galaxies require metadata. Need to send %(batches)s batch requests to NED." % locals()

        totalBatches = self.batches
        thisCount = 0

        # FOR EACH BATCH, GET THE GALAXY IDs, QUERY NED AND UPDATE THE DATABASE
        while self.total:
            thisCount += 1
            self._get_3000_galaxies_needing_metadata()
            self._query_ned_and_add_results_to_database(thisCount)
            self._count_galaxies_requiring_metadata()

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
            select count(*) as count from %(tableName)s where master_row = 1 and in_ned is null
        """ % locals()
        rows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )
        self.total = rows[0]["count"]
        self.batches = int(self.total / 3000.) + 1

        if self.total == 0:
            self.batches = 0

        self.log.info(
            'completed the ``_count_galaxies_requiring_metadata`` method')
        return self.total, self.batches

    def _get_3000_galaxies_needing_metadata(
            self):
        """ get 3000 galaxies needing metadata

        **Return:**
            - ``len(self.theseIds)`` -- the number of NED IDs returned
        """
        self.log.info(
            'starting the ``_get_3000_galaxies_needing_metadata`` method')

        tableName = self.dbTableName

        # SELECT THE DATA FROM NED TABLE
        self.theseIds = []
        sqlQuery = u"""
            select primary_ned_id from %(tableName)s where master_row = 1 and in_ned is null limit 3000;
        """ % locals()
        rows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )
        for row in rows:
            self.theseIds.append(row["primary_ned_id"])

        self.log.info(
            'completed the ``_get_3000_galaxies_needing_metadata`` method')

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
        print "results returned from ned -- starting to add to database" % locals()

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
                        redshift_err = %(redshift_err)s,
                        in_ned = 1
                    where primary_ned_id = "%(input_name)s" and master_row = 1;\n
                """ % thisDict
            else:
                sqlQuery += u"""
                    update %(tableName)s
                        set in_ned = 0
                    where primary_ned_id = "%(input_name)s" and master_row = 1;\n
                """ % thisDict
                sqlQuery = sqlQuery.replace('"null"', 'null')
        sqlQuery = sqlQuery.replace('"null"', 'null')

        rows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )
        print "%(count)s/%(totalCount)s galaxy metadata batch entries added to database" % locals()
        if count < totalCount:
            # Cursor up one line and clear line
            sys.stdout.write("\x1b[1A\x1b[2K")

        self.log.info(
            'completed the ``_query_ned_and_add_results_to_database`` method')
        return None

    def _update_sdss_coverage(
            self):
        """ update sdss coverage
        """
        self.log.info('starting the ``_update_sdss_coverage`` method')

        tableName = self.dbTableName

        # SELECT THE LOCATIONS NEEDING TO BE CHECKED
        sqlQuery = u"""
            select primary_ned_id, primaryID, raDeg, decDeg, sdss_coverage from %(tableName)s where sdss_coverage is null and master_row = 1 and in_ned = 1 order by dist_mpc;
        """ % locals()
        rows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )

        totalCount = len(rows)
        count = 0
        for row in rows:
            count += 1
            if count > 1:
                # Cursor up three lines and clear
                sys.stdout.write("\x1b[1A\x1b[2K")
                sys.stdout.write("\x1b[1A\x1b[2K")
                sys.stdout.write("\x1b[1A\x1b[2K")

            if count > totalCount:
                count = totalCount
            percent = (float(count) / float(totalCount)) * 100.

            primaryID = row["primaryID"]
            raDeg = float(row["raDeg"])
            decDeg = float(row["decDeg"])
            primary_ned_id = row["primary_ned_id"]

            # SDSS CAN ONLY ACCEPT 60 QUERIES/MIN
            time.sleep(1.1)
            print "%(count)s / %(totalCount)s (%(percent)1.1f%%) NED galaxies checked for SDSS coverage" % locals()
            print "NED NAME: ", primary_ned_id
            sdss_coverage = dat.check_for_sdss_coverage.check_for_sdss_coverage(
                log=self.log,
                raDeg=raDeg,
                decDeg=decDeg
            )
            if sdss_coverage == 999:
                sdss_coverage_flag = "null"
            elif sdss_coverage == True:
                sdss_coverage_flag = 1
            elif sdss_coverage == False:
                sdss_coverage_flag = 0
            else:
                self.log.error('cound not get sdss coverage' % locals())
                sys.exit(0)

            # UPDATE THE DATABASE FLAG
            sqlQuery = u"""
                update %(tableName)s set sdss_coverage = %(sdss_coverage_flag)s where primaryID = %(primaryID)s
            """ % locals()
            dms.execute_mysql_write_query(
                sqlQuery=sqlQuery,
                dbConn=self.cataloguesDbConn,
                log=self.log
            )

        self.log.info('completed the ``_update_sdss_coverage`` method')
        return None

    # use the tab-trigger below for new method
    # xt-class-method
