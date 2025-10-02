#!/usr/local/bin/python
# encoding: utf-8
"""
*Import ned_d catalogue into sherlock-catalogues database*

:Author:
    David Young
"""
from ._base_importer import _base_importer
import os
os.environ['TERM'] = 'vt100'


class ned_d(_base_importer):
    """
    *Import the * `NED-D <https://ned.ipac.caltech.edu/Library/Distances/>`_ *galaxy catalogue in to the sherlock-catalogues database*

    **Key Arguments**

    - ``log`` -- logger
    - ``settings`` -- the settings dictionary
    - ``pathToDataFile`` -- path to the ned_d data file
    - ``version`` -- version of the ned_d catalogue
    - ``catalogueName`` -- the name of the catalogue


    **Usage**

    To import the ned_d catalogue catalogue, run the following:

    ```python
    from sherlock.imports import ned_d
    catalogue = ned_d(
        log=log,
        settings=settings,
        pathToDataFile="/path/to/ned_d.txt",
        version="1.0",
        catalogueName="ned_d"
    )
    catalogue.ingest()
    ```


    .. todo ::

        - abstract this module out into its own stand alone script
    """
    # INITIALIZATION

    def ingest(self):
        """Import the ned_d catalogue into the catalogues database

        The method first generates a list of python dictionaries from the ned_d datafile, imports this list of dictionaries into a database table and then generates the HTMIDs for that table. 

        **Usage**

        See class docstring for usage


        .. todo ::

            - update docstring text
            - check sublime snippet exists
            - clip any useful text to docs mindmap
            - regenerate the docs and check redendering of this docstring
        """
        self.log.debug('starting the ``get`` method')

        dictList = self._create_dictionary_of_ned_d()
        self.primaryIdColumnName = "primaryId"
        self.raColName = "raDeg"
        self.declColName = "decDeg"

        tableName = self.dbTableName
        viewName = tableName.replace("tcs_cat_", "tcs_view_galaxy_")
        createStatement = u"""
            CREATE TABLE `%(tableName)s` (
              `primaryId` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'An internal counter',
              `Method` varchar(150) DEFAULT NULL,
              `dateCreated` datetime DEFAULT CURRENT_TIMESTAMP,
              `dateLastModified` datetime DEFAULT CURRENT_TIMESTAMP,
              `updated` varchar(45) DEFAULT '0',
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
              `sdss_coverage` tinyint(4) DEFAULT NULL,
              `htm16ID` bigint(20) DEFAULT NULL,
              `htm13ID` int(11) DEFAULT NULL,
              `htm10ID` int(11) DEFAULT NULL,
              `magnitude` double DEFAULT NULL,
              PRIMARY KEY (`primaryId`),
              UNIQUE KEY `galaxy_index_id_dist_index_id` (`galaxy_index_id`,`dist_index_id`),
              KEY `idx_htm10ID` (`htm10ID`),
              KEY `idx_htm13ID` (`htm13ID`),
              KEY `idx_htm16ID` (`htm16ID`),
              KEY `idx_dist` (`dist_mpc`),
              KEY `idx_redshift` (`redshift`),
              KEY `idx_major_diameter` (`major_diameter_arcmin`),
              KEY `idx_master_name` (`master_row`),
              KEY `idx_magnitude` (`magnitude`),
              KEY `idx_in_ned` (`in_ned`),
              KEY `idx_type` (`object_type`),
              KEY `idx_dist_from_sn` (`dist_derived_from_sn`)
            ) ENGINE=MyISAM AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
        DROP VIEW IF EXISTS `%(viewName)s`;
            CREATE
                VIEW `%(viewName)s` AS
                    (SELECT 
        `%(tableName)s`.`primaryId` AS `primaryId`,
        `%(tableName)s`.`Method` AS `Method`,
        `%(tableName)s`.`dateCreated` AS `dateCreated`,
        `%(tableName)s`.`dist_derived_from_sn` AS `dist_derived_from_sn`,
        `%(tableName)s`.`dist_in_ned_flag` AS `dist_in_ned_flag`,
        `%(tableName)s`.`dist_index_id` AS `dist_index_id`,
        `%(tableName)s`.`dist_mod_median` AS `dist_mod`,
        `%(tableName)s`.`dist_mod_err` AS `dist_mod_err`,
        `%(tableName)s`.`dist_mpc_median` AS `dist_mpc`,
        `%(tableName)s`.`galaxy_index_id` AS `galaxy_index_id`,
        `%(tableName)s`.`hubble_const` AS `hubble_const`,
        `%(tableName)s`.`lmc_mod` AS `lmc_mod`,
        `%(tableName)s`.`notes` AS `notes`,
        `%(tableName)s`.`primary_ned_id` AS `primary_ned_id`,
        `%(tableName)s`.`redshift` AS `redshift`,
        `%(tableName)s`.`ref` AS `ref`,
        `%(tableName)s`.`ref_date` AS `ref_date`,
        `%(tableName)s`.`master_row` AS `master_row`,
        `%(tableName)s`.`major_diameter_arcmin` AS `major_diameter_arcmin`,
        `%(tableName)s`.`ned_notes` AS `ned_notes`,
        `%(tableName)s`.`object_type` AS `object_type`,
        `%(tableName)s`.`redshift_err` AS `redshift_err`,
        `%(tableName)s`.`redshift_quality` AS `redshift_quality`,
        `%(tableName)s`.`magnitude` AS `magnitude`,
        `%(tableName)s`.`minor_diameter_arcmin` AS `minor_diameter_arcmin`,
        `%(tableName)s`.`morphology` AS `morphology`,
        `%(tableName)s`.`hierarchy` AS `hierarchy`,
        `%(tableName)s`.`galaxy_morphology` AS `galaxy_morphology`,
        `%(tableName)s`.`radio_morphology` AS `radio_morphology`,
        `%(tableName)s`.`activity_type` AS `activity_type`,
        `%(tableName)s`.`in_ned` AS `in_ned`,
        `%(tableName)s`.`raDeg` AS `raDeg`,
        `%(tableName)s`.`decDeg` AS `decDeg`,
        `%(tableName)s`.`eb_v` AS `eb_v`,
        `%(tableName)s`.`htm16ID` AS `htm16ID`,
        `%(tableName)s`.`htm10ID` AS `htm10ID`,
        `%(tableName)s`.`htm13ID` AS `htm13ID`,
        `%(tableName)s`.`sdss_coverage` AS `sdss_coverage`
    FROM
        `%(tableName)s`
    WHERE
        `%(tableName)s`.`master_row` = 1);
        """ % locals()

        self.add_data_to_database_table(
            dictList=dictList,
            createStatement=createStatement
        )

        self._clean_up_columns()
        self._get_metadata_for_galaxies()
        # self._update_sdss_coverage()

        self.log.debug('completed the ``get`` method')
        return None

    def _create_dictionary_of_ned_d(
            self):
        """create a list of dictionaries containing all the rows in the ned_d catalogue

        **Return**

        - ``dictList`` - a list of dictionaries containing all the rows in the ned_d catalogue


        .. todo ::

            - update key arguments values and definitions with defaults
            - update return values and definitions
            - update usage examples and text
            - update docstring text
            - check sublime snippet exists
            - clip any useful text to docs mindmap
            - regenerate the docs and check redendering of this docstring
        """
        self.log.debug(
            'starting the ``_create_dictionary_of_ned_d`` method')

        import pandas as pd
        import csv
        import sys

        count = 0
        with open(self.pathToDataFile, 'r') as csvFile:
            csvReader = csv.reader(
                csvFile, dialect='excel', delimiter=',', quotechar='"')
            totalRows = sum(1 for row in csvReader)
        csvFile.close()
        totalCount = totalRows

        with open(self.pathToDataFile, 'r') as csvFile:
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
                    print(
                        "%(count)s / %(totalCount)s (%(percent)1.1f%%) rows added to memory" % locals())
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

        df = pd.DataFrame(dictList)
        df['galaxy_index_id'] = df['galaxy_index_id'].values.astype(int)
        # GROUP RESULTS
        dfGroups = df.groupby(['galaxy_index_id'])

        medianDistDf = dfGroups[["dist_mpc",
                                 "dist_mod"]].median().reset_index()
        # RENAME COLUMNS
        medianDistDf.rename(
            columns={"dist_mpc": "dist_mpc_median"}, inplace=True)
        medianDistDf.rename(
            columns={"dist_mod": "dist_mod_median"}, inplace=True)

        # MERGE DATAFRAMES
        df = df.merge(medianDistDf, on=['galaxy_index_id'], how='outer')

        # SORT BY COLUMN NAME
        df.sort_values(['galaxy_index_id'],
                       ascending=[True], inplace=True)

        dictList = df.to_dict('records')
        self.log.debug(
            'completed the ``_create_dictionary_of_ned_d`` method')
        return dictList

    def _clean_up_columns(
            self):
        """clean up columns of the NED table

        .. todo ::

            - update key arguments values and definitions with defaults
            - update return values and definitions
            - update usage examples and text
            - update docstring text
            - check sublime snippet exists
            - clip any useful text to docs mindmap
            - regenerate the docs and check redendering of this docstring
        """
        self.log.debug('starting the ``_clean_up_columns`` method')

        from fundamentals.mysql import writequery

        tableName = self.dbTableName

        print("cleaning up %(tableName)s columns" % locals())

        sqlQuery = u"""
                set sql_mode="STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION";
            """ % locals()
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
        )

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
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
        )

        self.log.debug('completed the ``_clean_up_columns`` method')
        return None

    def _get_metadata_for_galaxies(
            self):
        """get metadata for galaxies

        .. todo ::

            - update key arguments values and definitions with defaults
            - update return values and definitions
            - update usage examples and text
            - update docstring text
            - check sublime snippet exists
            - clip any useful text to docs mindmap
            - regenerate the docs and check redendering of this docstring
        """
        self.log.debug('starting the ``_get_metadata_for_galaxies`` method')

        total, batches = self._count_galaxies_requiring_metadata()
        print("%(total)s galaxies require metadata. Need to send %(batches)s batch requests to NED." % locals())

        totalBatches = self.batches
        thisCount = 0

        # FOR EACH BATCH, GET THE GALAXY IDs, QUERY NED AND UPDATE THE DATABASE
        while self.total:
            thisCount += 1
            self._get_3000_galaxies_needing_metadata()
            dictList = self._query_ned_and_add_results_to_database(thisCount)

            self.add_data_to_database_table(
                dictList=dictList,
                createStatement=False
            )

            self._count_galaxies_requiring_metadata()

        self.log.debug('completed the ``_get_metadata_for_galaxies`` method')
        return None

    def _count_galaxies_requiring_metadata(
            self):
        """ count galaxies requiring metadata

        **Return**

        - ``self.total``, ``self.batches`` -- total number of galaxies needing metadata & the number of batches required to be sent to NED


        .. todo ::

            - update key arguments values and definitions with defaults
            - update return values and definitions
            - update usage examples and text
            - update docstring text
            - check sublime snippet exists
            - clip any useful text to docs mindmap
            - regenerate the docs and check redendering of this docstring
        """
        self.log.debug(
            'starting the ``_count_galaxies_requiring_metadata`` method')

        from fundamentals.mysql import readquery

        tableName = self.dbTableName

        sqlQuery = u"""
            select count(*) as count from %(tableName)s where master_row = 1 and in_ned is null
        """ % locals()
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            quiet=False
        )
        self.total = rows[0]["count"]
        self.batches = int(self.total // 3000) + 1

        if self.total == 0:
            self.batches = 0

        self.log.debug(
            'completed the ``_count_galaxies_requiring_metadata`` method')
        return self.total, self.batches

    def _get_3000_galaxies_needing_metadata(
            self):
        """ get 3000 galaxies needing metadata

        **Return**

        - ``len(self.theseIds)`` -- the number of NED IDs returned


        .. todo ::

            - update key arguments values and definitions with defaults
            - update return values and definitions
            - update usage examples and text
            - update docstring text
            - check sublime snippet exists
            - clip any useful text to docs mindmap
            - regenerate the docs and check redendering of this docstring
        """
        self.log.debug(
            'starting the ``_get_3000_galaxies_needing_metadata`` method')

        from fundamentals.mysql import readquery

        tableName = self.dbTableName

        # SELECT THE DATA FROM NED TABLE
        self.theseIds = {}
        sqlQuery = u"""
            select primaryId, primary_ned_id from %(tableName)s where master_row = 1 and in_ned is null limit 3000;
        """ % locals()
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            quiet=False
        )
        for row in rows:
            self.theseIds[row["primary_ned_id"]] = row["primaryId"]

        self.log.debug(
            'completed the ``_get_3000_galaxies_needing_metadata`` method')

        return len(self.theseIds)

    def _query_ned_and_add_results_to_database(
            self,
            batchCount):
        """ query ned and add results to database

        **Key Arguments**

        - ``batchCount`` - the index number of the batch sent to NED


        .. todo ::

            - update key arguments values and definitions with defaults
            - update return values and definitions
            - update usage examples and text
            - update docstring text
            - check sublime snippet exists
            - clip any useful text to docs mindmap
            - regenerate the docs and check redendering of this docstring
        """
        self.log.debug(
            'starting the ``_query_ned_and_add_results_to_database`` method')

        import re
        from neddy import namesearch
        from astrocalc.coords import unit_conversion
        regex = re.compile(r'[^\d\.]')

        tableName = self.dbTableName
        # ASTROCALC UNIT CONVERTER OBJECT
        converter = unit_conversion(
            log=self.log
        )

        # QUERY NED WITH BATCH
        totalCount = len(self.theseIds)
        print("requesting metadata from NED for %(totalCount)s galaxies (batch %(batchCount)s)" % locals())
        search = namesearch(
            log=self.log,
            names=list(self.theseIds.keys()),
            quiet=True
        )
        results = search.get()
        print("results returned from ned -- starting to add to database" % locals())

        # CLEAN THE RETURNED DATA AND UPDATE DATABASE
        totalCount = len(results)
        count = 0
        sqlQuery = ""
        dictList = []

        colList = ["redshift_quality", "redshift", "hierarchy", "object_type", "major_diameter_arcmin", "morphology", "magnitude_filter",
                   "ned_notes", "eb_v", "raDeg", "radio_morphology", "activity_type", "minor_diameter_arcmin", "decDeg", "redshift_err", "in_ned", "magnitude"]

        if not len(results):
            for k, v in list(self.theseIds.items()):
                dictList.append({
                    "in_ned": 0,
                    "primaryID": v
                })
        for thisDict in results:
            thisDict["magnitude"] = thisDict["magnitude_filter"]
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
                if k in ["magnitude"] and thisDict[k] != "null":
                    thisDict[k] = regex.sub("", thisDict[k])

            if "Input name not" not in thisDict["input_note"] and "Same object as" not in thisDict["input_note"]:
                if thisDict["ra"] != "null" and thisDict["dec"] != "null":
                    thisDict["raDeg"] = converter.ra_sexegesimal_to_decimal(
                        ra=thisDict["ra"]
                    )
                    thisDict["decDeg"] = converter.dec_sexegesimal_to_decimal(
                        dec=thisDict["dec"]
                    )
                else:
                    thisDict["raDeg"] = None
                    thisDict["decDeg"] = None
                thisDict["in_ned"] = 1
                thisDict["eb_v"] = thisDict["eb-v"]

                row = {}
                row["primary_ned_id"] = thisDict["input_name"]

                try:
                    row["primaryID"] = self.theseIds[thisDict["input_name"]]
                    for c in colList:
                        if thisDict[c] == "null":
                            row[c] = None
                        else:
                            row[c] = thisDict[c]
                    dictList.append(row)
                except:
                    g = thisDict["input_name"]
                    self.log.error(
                        "Cannot find database table %(tableName)s primaryID for '%(g)s'\n\n" % locals())
                    dictList.append({
                        "in_ned": 0,
                        "primary_ned_id": thisDict["input_name"]
                    })

            else:
                dictList.append({
                    "primary_ned_id": thisDict["input_name"],
                    "in_ned": 0,
                    "primaryID": self.theseIds[thisDict["input_name"]]
                })

        self.log.debug(
            'completed the ``_query_ned_and_add_results_to_database`` method')
        return dictList

    def _update_sdss_coverage(
            self):
        """ update sdss coverage

        .. todo ::

            - update key arguments values and definitions with defaults
            - update return values and definitions
            - update usage examples and text
            - update docstring text
            - check sublime snippet exists
            - clip any useful text to docs mindmap
            - regenerate the docs and check redendering of this docstring
        """
        self.log.debug('starting the ``_update_sdss_coverage`` method')

        from sloancone import check_coverage
        from fundamentals.mysql import writequery, readquery
        import time
        import sys

        tableName = self.dbTableName

        # SELECT THE LOCATIONS NEEDING TO BE CHECKED
        sqlQuery = u"""
            select primary_ned_id, primaryID, raDeg, decDeg, sdss_coverage from %(tableName)s where sdss_coverage is null and master_row = 1 and in_ned = 1 order by dist_mpc;
        """ % locals()
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            quiet=False
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
            print("%(count)s / %(totalCount)s (%(percent)1.1f%%) NED galaxies checked for SDSS coverage" % locals())
            print("NED NAME: ", primary_ned_id)

            # covered = True | False | 999 (i.e. not sure)
            sdss_coverage = check_coverage(
                log=self.log,
                ra=raDeg,
                dec=decDeg
            ).get()

            if sdss_coverage == 999:
                sdss_coverage_flag = "null"
            elif sdss_coverage == True:
                sdss_coverage_flag = 1
            elif sdss_coverage == False:
                sdss_coverage_flag = 0
            else:
                self.log.error('could not get sdss coverage' % locals())
                sys.exit(0)

            # UPDATE THE DATABASE FLAG
            sqlQuery = u"""
                update %(tableName)s set sdss_coverage = %(sdss_coverage_flag)s where primaryID = %(primaryID)s
            """ % locals()
            writequery(
                log=self.log,
                sqlQuery=sqlQuery,
                dbConn=self.cataloguesDbConn,
            )

        self.log.debug('completed the ``_update_sdss_coverage`` method')
        return None

    # use the tab-trigger below for new method
    # xt-class-method
