#!/usr/local/bin/python
# encoding: utf-8
"""
*classify a set of transients defined by a database query in the sherlock settings file*

:Author:
    David Young

:Date Created:
    January  5, 2017
"""
################# GLOBAL IMPORTS ####################
import sys
import os
import collections
import codecs
import re
import math
import time
from random import randint
os.environ['TERM'] = 'vt100'
from datetime import datetime, date, time, timedelta
from operator import itemgetter
import numpy as np
from fundamentals import tools
from fundamentals.mysql import readquery, directory_script_runner, writequery
from fundamentals.renderer import list_of_dictionaries
from HMpTy.mysql import conesearch
from HMpTy.htm import sets
from sherlock.imports import ned
from sherlock.commonutils import get_crossmatch_catalogues_column_map


class transient_classifier():
    """
    *The Sherlock Transient Classifier*

    **Key Arguments:**
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary
        - ``update`` -- update the transient database with crossmatch results (boolean)
        - ``ra`` -- right ascension of a single transient source. Default *False*
        - ``dec`` -- declination of a single transient source. Default *False*
        - ``name`` -- the ID of a single transient source. Default *False*
        - ``verbose`` -- amount of details to print about crossmatches to stdout. 0|1|2 Default *0*
        - ``fast`` -- run in fast mode. This mode may not catch errors in the ingest of data to the crossmatches table but runs twice as fast. Default *False*.
        - ``updateNed`` -- update the local NED database before running the classifier. Classification will not be as accuracte the NED database is not up-to-date. Default *True*.
        - ``daemonMode`` -- run sherlock in daemon mode. In daemon mode sherlock remains live and classifies sources as they come into the database. Default *True*


    **Usage:**

        To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_).

        To initiate a transient_classifier object, use the following:

        .. todo::

            - update the package tutorial if needed

        The sherlock classifier can be run in one of two ways. The first is to pass into the coordinates of an object you wish to classify:

        .. code-block:: python

            from sherlock import transient_classifier
            classifier = transient_classifier(
                log=log,
                settings=settings,
                ra="08:57:57.19",
                dec="+43:25:44.1",
                name="PS17gx",
                verbose=0
            )
            classifications, crossmatches = classifier.classify()

        The crossmatches returned are a list of dictionaries giving details of the crossmatched sources. The classifications returned are a list of classifications resulting from these crossmatches. The lists are ordered from most to least likely classification and the indicies for the crossmatch and the classification lists are synced.

        The second way to run the classifier is to not pass in a coordinate set and therefore cause sherlock to run the classifier on the transient database referenced in the sherlock settings file:

        .. code-block:: python

            from sherlock import transient_classifier
            classifier = transient_classifier(
                log=log,
                settings=settings,
                update=True
            )
            classifier.classify()

        Here the transient list is selected out of the database using the ``transient query`` value in the settings file:

        .. code-block:: yaml

            database settings:
                transients:
                    user: myusername
                    password: mypassword
                    db: nice_transients
                    host: 127.0.0.1
                    transient table: transientBucket
                    transient query: "select primaryKeyId as 'id', transientBucketId as 'alt_id', raDeg 'ra', decDeg 'dec', name 'name', sherlockClassification as 'object_classification'
                        from transientBucket where object_classification is null
                    transient primary id column: primaryKeyId
                    transient classification column: sherlockClassification
                    tunnel: False

        By setting ``update=True`` the classifier will update the ``sherlockClassification`` column of the ``transient table`` with new classification and populate the ``sherlock_crossmatches`` table with key details of the crossmatched sources from the catalogues database. By setting ``update=False`` results are printed to stdout but the database is not updated (useful for dry runs and testing new algorithms),

    """
    # INITIALISATION

    def __init__(
            self,
            log,
            settings=False,
            update=False,
            ra=False,
            dec=False,
            name=False,
            verbose=0,
            fast=False,
            updateNed=True,
            daemonMode=False
    ):
        self.log = log
        log.debug("instansiating a new 'classifier' object")
        self.settings = settings
        self.update = update
        self.ra = ra
        self.dec = dec
        self.name = name
        self.cl = False
        self.verbose = verbose
        self.fast = fast
        self.updateNed = updateNed
        self.daemonMode = daemonMode
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
        self.dbVersions = dbVersions
        self.transientsDbConn = dbConns["transients"]
        self.cataloguesDbConn = dbConns["catalogues"]
        self.pmDbConn = dbConns["marshall"]

        # IS SHERLOCK CLASSIFIER BEING QUERIED FROM THE COMMAND-LINE?
        if self.ra and self.dec:
            self.cl = True
            if not self.name:
                self.name = "Transient"

        # DATETIME REGEX - EXPENSIVE OPERATION, LET"S JUST DO IT ONCE
        self.reDatetime = re.compile('^[0-9]{4}-[0-9]{2}-[0-9]{2}T')

        return None

    def classify(self):
        """
        *classify the transients selected from the transient selection query in the settings file or passed in via the CL or other code*

        **Return:**
            - ``crossmatches`` -- list of dictionaries of crossmatched associated sources
            - ``classifications`` -- the classifications assigned to the transients post-crossmatches (dictionary of rank ordered list of classifications)

        See class docstring for usage.
        """
        self.log.info('starting the ``classify`` method')

        remaining = 1
        while remaining:

            # IF A TRANSIENT HAS NOT BEEN PASSED IN VIA THE COMMAND-LINE, THEN
            # QUERY THE TRANSIENT DATABASE
            if not self.ra and not self.dec:

                # COUNT REMAINING TRANSIENTS
                from fundamentals.mysql import readquery
                sqlQuery = self.settings["database settings"][
                    "transients"]["transient count"]
                thisInt = randint(0, 100)
                if "where" in sqlQuery:
                    sqlQuery = sqlQuery.replace(
                        "where", "where %(thisInt)s=%(thisInt)s and " % locals())

                rows = readquery(
                    log=self.log,
                    sqlQuery=sqlQuery,
                    dbConn=self.transientsDbConn,
                )
                remaining = rows[0]["count(*)"]
                print "%(remaining)s transient sources requiring a classification remain" % locals()

                # A LIST OF DICTIONARIES OF TRANSIENT METADATA
                transientsMetadataList = self._get_transient_metadata_from_database_list()
                count = len(transientsMetadataList)
                print "  now classifying the next %(count)s transient sources" % locals()

                # EXAMPLE OF TRANSIENT METADATA
                # { 'name': 'PS17gx',
                # 'alt_id': 'PS17gx',
                # 'object_classification': 'SN',
                # 'dec': '+43:25:44.1',
                # 'id': 1,
                # 'ra': '08:57:57.19'}
            # TRANSIENT PASSED VIA COMMAND-LINE
            else:
                if not self.name:
                    name = "transient"
                else:
                    name = self.name
                transient = {
                    'name': name,
                    'object_classification': None,
                    'dec': self.dec,
                    'id': name,
                    'ra': self.ra
                }
                transientsMetadataList = [transient]
                remaining = 0

            if len(transientsMetadataList) == 0:
                if self.daemonMode == False:
                    remaining = 0
                    print "No transients need classified"
                    return None, None
                else:
                    print "No remaining transients need classified, will try again in 5 mins"
                    time.sleep("10")

            # THE COLUMN MAPS - WHICH COLUMNS IN THE CATALOGUE TABLES = RA, DEC,
            # REDSHIFT, MAG ETC
            colMaps = get_crossmatch_catalogues_column_map(
                log=self.log,
                dbConn=self.cataloguesDbConn
            )

            # FROM THE LOCATIONS OF THE TRANSIENTS, CHECK IF OUR LOCAL NED DATABASE
            # NEEDS UPDATED
            if self.updateNed:
                self._update_ned_stream(
                    transientsMetadataList=transientsMetadataList
                )

            batchSize = 100
            total = len(transientsMetadataList[1:])
            batches = int(total / batchSize)

            start = 0
            end = 0
            theseBatches = []
            for i in range(batches + 1):
                end = end + batchSize
                start = i * batchSize
                thisBatch = transientsMetadataList[start:end]
                theseBatches.append(thisBatch)

            count = 1
            for batch in theseBatches:
                count += 1
                # RUN THE CROSSMATCH CLASSIFIER FOR ALL TRANSIENTS
                crossmatches = self._crossmatch_transients_against_catalogues(
                    colMaps=colMaps,
                    transientsMetadataList=batch
                )

                classifications, crossmatches = self._rank_classifications(
                    colMaps=colMaps,
                    crossmatches=crossmatches
                )

                if self.cl:
                    self._print_results_to_stdout(
                        classifications=classifications,
                        crossmatches=crossmatches
                    )

                for t in batch:
                    if t["id"] not in classifications:
                        classifications[t["id"]] = ["ORPHAN"]

                # UPDATE THE TRANSIENT DATABASE IF UPDATE REQUESTED (ADD DATA TO
                # tcs_crossmatch_table AND A CLASSIFICATION TO THE ORIGINAL TRANSIENT
                # TABLE)
                if self.update and not self.ra:
                    self._update_transient_database(
                        crossmatches=crossmatches,
                        classifications=classifications,
                        transientsMetadataList=batch,
                        colMaps=colMaps
                    )

                if self.ra:
                    return classifications, crossmatches

                self.update_peak_magnitudes()
                self.update_classification_annotations_and_summaries()

        self.log.info('completed the ``classify`` method')
        return None, None

    def _get_transient_metadata_from_database_list(
            self):
        """use the transient query in the settings file to generate a list of transients to corssmatch and classify

         **Return:**
            - ``transientsMetadataList``
        """
        self.log.debug(
            'starting the ``_get_transient_metadata_from_database_list`` method')

        sqlQuery = self.settings["database settings"][
            "transients"]["transient query"] + " limit " + str(self.settings["database settings"][
                "transients"]["classificationBatchSize"])

        thisInt = randint(0, 100)
        if "where" in sqlQuery:
            sqlQuery = sqlQuery.replace(
                "where", "where %(thisInt)s=%(thisInt)s and " % locals())

        transientsMetadataList = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.transientsDbConn,
            quiet=False
        )

        self.log.debug(
            'completed the ``_get_transient_metadata_from_database_list`` method')
        return transientsMetadataList

    def _update_ned_stream(
        self,
        transientsMetadataList
    ):
        """ update the NED stream within the catalogues database at the locations of the transients

        **Key Arguments:**
            - ``transientsMetadataList`` -- the list of transient metadata lifted from the database.
        """
        self.log.info('starting the ``_update_ned_stream`` method')

        coordinateList = []
        for i in transientsMetadataList:
            # thisList = str(i["ra"]) + " " + str(i["dec"])
            thisList = (i["ra"], i["dec"])
            coordinateList.append(thisList)

        coordinateList = self._remove_previous_ned_queries(
            coordinateList=coordinateList
        )

        # MINIMISE COORDINATES IN LIST TO REDUCE NUMBER OF REQUIRE NED QUERIES
        coordinateList = self._consolidate_coordinateList(
            coordinateList=coordinateList
        )

        stream = ned(
            log=self.log,
            settings=self.settings,
            coordinateList=coordinateList,
            radiusArcsec=self.settings["ned stream search radius arcec"]
        )
        stream.ingest()

        sqlQuery = """SET session sql_mode = "";""" % locals(
        )
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn
        )

        sqlQuery = """update tcs_cat_ned_stream set magnitude = CAST(`magnitude_filter` AS DECIMAL(5,2)) where magnitude is null;""" % locals(
        )
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn
        )

        self.log.info('completed the ``_update_ned_stream`` method')
        return None

    def _remove_previous_ned_queries(
            self,
            coordinateList):
        """iterate through the transient locations to see if we have recent local NED coverage of that area already

        **Key Arguments:**
            - ``coordinateList`` -- set of coordinate to check for previous queries

        **Return:**
            - ``updatedCoordinateList`` -- coordinate list with previous queries removed
        """
        self.log.info('starting the ``_remove_previous_ned_queries`` method')

        # 1 DEGREE QUERY RADIUS
        radius = 60. * 60.
        updatedCoordinateList = []
        keepers = []

        # CALCULATE THE OLDEST RESULTS LIMIT
        now = datetime.now()
        td = timedelta(
            days=self.settings["ned stream refresh rate in days"])
        refreshLimit = now - td
        refreshLimit = refreshLimit.strftime("%Y-%m-%d %H:%M:%S")

        raList = []
        raList[:] = [c[0] for c in coordinateList]
        decList = []
        decList[:] = [c[1] for c in coordinateList]

        # MATCH COORDINATES AGAINST PREVIOUS NED SEARCHES
        cs = conesearch(
            log=self.log,
            dbConn=self.cataloguesDbConn,
            tableName="tcs_helper_ned_query_history",
            columns="*",
            ra=raList,
            dec=decList,
            radiusArcsec=radius,
            separations=True,
            distinct=True,
            sqlWhere="dateQueried > '%(refreshLimit)s'" % locals(),
            closest=False
        )
        matchIndies, matches = cs.search()

        # DETERMINE WHICH COORDINATES REQUIRE A NED QUERY
        curatedMatchIndices = []
        curatedMatches = []
        for i, m in zip(matchIndies, matches.list):
            match = False
            row = m
            row["separationArcsec"] = row["cmSepArcsec"]
            raStream = row["raDeg"]
            decStream = row["decDeg"]
            radiusStream = row["arcsecRadius"]
            dateStream = row["dateQueried"]
            angularSeparation = row["separationArcsec"]

            if angularSeparation + self.settings["first pass ned search radius arcec"] < radiusStream:
                curatedMatchIndices.append(i)
                curatedMatches.append(m)

        # NON MATCHES
        for i, v in enumerate(coordinateList):
            if i not in curatedMatchIndices:
                updatedCoordinateList.append(v)

        self.log.info('completed the ``_remove_previous_ned_queries`` method')
        return updatedCoordinateList

    def _crossmatch_transients_against_catalogues(
            self,
            colMaps,
            transientsMetadataList):
        """run the transients through the crossmatch algorithm in the settings file

         **Key Arguments:**
            - ``colMaps`` -- maps of the important column names for each table/view in the crossmatch-catalogues database
            - ``transientsMetadataList`` -- the list of transient metadata lifted from the database.

        **Return:**
            - ``crossmatches`` -- a list of dictionaries of the associated sources crossmatched from the catalogues database
        """
        self.log.debug(
            'starting the ``_crossmatch_transients_against_catalogues`` method')

        from sherlock import transient_catalogue_crossmatch
        self.allClassifications = []

        cm = transient_catalogue_crossmatch(
            log=self.log,
            dbConn=self.cataloguesDbConn,
            transients=transientsMetadataList,
            settings=self.settings,
            colMaps=colMaps
        )
        crossmatches = cm.match()

        self.log.debug(
            'completed the ``_crossmatch_transients_against_catalogues`` method')
        return crossmatches

    def _update_transient_database(
            self,
            crossmatches,
            classifications,
            transientsMetadataList,
            colMaps):
        """ update transient database with classifications and crossmatch results

        **Key Arguments:**
            - ``crossmatches`` -- the crossmatches and associations resulting from the catlaogue crossmatches
            - ``classifications`` -- the classifications assigned to the transients post-crossmatches (dictionary of rank ordered list of classifications)
            - ``transientsMetadataList`` -- the list of transient metadata lifted from the database.
            - ``colMaps`` -- maps of the important column names for each table/view in the crossmatch-catalogues database
        """

        self.log.debug('starting the ``_update_transient_database`` method')

        myPid = self.myPid
        now = datetime.now()
        now = now.strftime("%Y-%m-%d_%H-%M-%S-%f")

        transientTable = self.settings["database settings"][
            "transients"]["transient table"]
        transientTableClassCol = self.settings["database settings"][
            "transients"]["transient classification column"]
        transientTableIdCol = self.settings["database settings"][
            "transients"]["transient primary id column"]
        crossmatchTable = "sherlock_crossmatches"

        # RECURSIVELY CREATE MISSING DIRECTORIES
        if not os.path.exists("/tmp/sherlock"):
            os.makedirs("/tmp/sherlock")

        # COMBINE ALL CROSSMATCHES INTO A LIST OF DICTIONARIES TO DUMP INTO
        # DATABASE TABLE
        transientIDs = []
        transientIDs[:] = [str(c["transient_object_id"])
                           for c in crossmatches]
        transientIDs = ",".join(transientIDs)

        createStatement = """
CREATE TABLE IF NOT EXISTS `%(crossmatchTable)s` (
  `transient_object_id` bigint(20) unsigned DEFAULT NULL,
  `catalogue_object_id` varchar(30) DEFAULT NULL,
  `catalogue_table_id` smallint(5) unsigned DEFAULT NULL,
  `separationArcsec` double DEFAULT NULL,
  `northSeparationArcsec` DOUBLE DEFAULT NULL,
  `eastSeparationArcsec` DOUBLE DEFAULT NULL,
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `z` double DEFAULT NULL,
  `scale` double DEFAULT NULL,
  `distance` double DEFAULT NULL,
  `distance_modulus` double DEFAULT NULL,
  `photoZ` double DEFAULT NULL,
  `photoZErr` double DEFAULT NULL,
  `association_type` varchar(45) DEFAULT NULL,
  `dateCreated` datetime DEFAULT CURRENT_TIMESTAMP,
  `physical_separation_kpc` double DEFAULT NULL,
  `catalogue_object_type` varchar(45) DEFAULT NULL,
  `catalogue_object_subtype` varchar(45) DEFAULT NULL,
  `association_rank` int(11) DEFAULT NULL,
  `catalogue_table_name` varchar(100) DEFAULT NULL,
  `catalogue_view_name` varchar(100) DEFAULT NULL,
  `rank` int(11) DEFAULT NULL,
  `rankScore` double DEFAULT NULL,
  `search_name` varchar(100) DEFAULT NULL,
  `major_axis_arcsec` double DEFAULT NULL,
  `direct_distance` double DEFAULT NULL,
  `direct_distance_scale` double DEFAULT NULL,
  `direct_distance_modulus` double DEFAULT NULL,
  `raDeg` double DEFAULT NULL,
  `decDeg` double DEFAULT NULL,
  `original_search_radius_arcsec` double DEFAULT NULL,
  `catalogue_view_id` int(11) DEFAULT NULL,
  `U` double DEFAULT NULL,
  `UErr` double DEFAULT NULL,
  `B` double DEFAULT NULL,
  `BErr` double DEFAULT NULL,
  `V` double DEFAULT NULL,
  `VErr` double DEFAULT NULL,
  `R` double DEFAULT NULL,
  `RErr` double DEFAULT NULL,
  `I` double DEFAULT NULL,
  `IErr` double DEFAULT NULL,
  `J` double DEFAULT NULL,
  `JErr` double DEFAULT NULL,
  `H` double DEFAULT NULL,
  `HErr` double DEFAULT NULL,
  `K` double DEFAULT NULL,
  `KErr` double DEFAULT NULL,
  `_u` double DEFAULT NULL,
  `_uErr` double DEFAULT NULL,
  `_g` double DEFAULT NULL,
  `_gErr` double DEFAULT NULL,
  `_r` double DEFAULT NULL,
  `_rErr` double DEFAULT NULL,
  `_i` double DEFAULT NULL,
  `_iErr` double DEFAULT NULL,
  `_z` double DEFAULT NULL,
  `_zErr` double DEFAULT NULL,
  `_y` double DEFAULT NULL,
  `_yErr` double DEFAULT NULL,
  `G` double DEFAULT NULL,
  `GErr` double DEFAULT NULL,
  `unkMag` double DEFAULT NULL,
  `unkMagErr` double DEFAULT NULL,
  `dateLastModified` datetime DEFAULT CURRENT_TIMESTAMP,
  `updated` TINYINT NULL DEFAULT 0,
  `classificationReliability` TINYINT NULL DEFAULT NULL,
  `transientAbsMag` DOUBLE NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `key_transient_object_id` (`transient_object_id`),
  KEY `key_catalogue_object_id` (`catalogue_object_id`),
  KEY `idx_separationArcsec` (`separationArcsec`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;

delete from %(crossmatchTable)s where transient_object_id in (%(transientIDs)s);
""" % locals()

        # A FIX FOR MYSQL VERSIONS < 5.6
        if float(self.dbVersions["transients"][:3]) < 5.6:
            createStatement = createStatement.replace(
                "`dateLastModified` datetime DEFAULT CURRENT_TIMESTAMP,", "`dateLastModified` datetime DEFAULT NULL,")
            createStatement = createStatement.replace(
                "`dateCreated` datetime DEFAULT CURRENT_TIMESTAMP,", "`dateCreated` datetime DEFAULT NULL,")

            exists = os.path.exists(
                "/tmp/sherlock_trigger/%(myPid)s/%(crossmatchTable)s_trigger.sql" % locals())
            if not exists:
                trigger = """
                    DELIMITER $$
                    CREATE TRIGGER dateCreated
                    BEFORE INSERT ON `%(crossmatchTable)s`
                    FOR EACH ROW
                    BEGIN
                       IF NEW.dateCreated IS NULL THEN
                          SET NEW.dateCreated = NOW();
                          SET NEW.dateLastModified = NOW();
                       END IF;
                    END$$
                    DELIMITER ;""" % locals()

                # Recursively create missing directories
                if not os.path.exists("/tmp/sherlock_trigger/%(myPid)s" % locals()):
                    os.makedirs("/tmp/sherlock_trigger/%(myPid)s" % locals())

                pathToWriteFile = "/tmp/sherlock_trigger/%(myPid)s/%(crossmatchTable)s_trigger.sql" % locals(
                )
                try:
                    self.log.debug("attempting to open the file %s" %
                                   (pathToWriteFile,))
                    writeFile = codecs.open(
                        pathToWriteFile, encoding='utf-8', mode='w')
                except IOError, e:
                    message = 'could not open the file %s' % (pathToWriteFile,)
                    self.log.critical(message)
                    raise IOError(message)
                writeFile.write(trigger)
                writeFile.close()

                directory_script_runner(
                    log=self.log,
                    pathToScriptDirectory="/tmp/sherlock_trigger/%(myPid)s" % locals(
                    ),
                    databaseName=self.settings[
                        "database settings"]["transients"]["db"],
                    loginPath=self.settings["database settings"][
                        "transients"]["loginPath"],
                    waitForResult=False
                )

        # Recursively create missing directories
        if not os.path.exists("/tmp/sherlock/%(myPid)s" % locals()):
            os.makedirs("/tmp/sherlock/%(myPid)s" % locals())

        dataSet = list_of_dictionaries(
            log=self.log,
            listOfDictionaries=crossmatches,
            reDatetime=self.reDatetime
        )

        mysqlData = dataSet.mysql(
            tableName=crossmatchTable, filepath="/tmp/sherlock/%(myPid)s/%(now)s_cm_results.sql" % locals(), createStatement=createStatement)

        if self.fast:
            waitForResult = "delete"
        else:
            waitForResult = True

        directory_script_runner(
            log=self.log,
            pathToScriptDirectory="/tmp/sherlock/%(myPid)s" % locals(),
            databaseName=self.settings[
                "database settings"]["transients"]["db"],
            loginPath=self.settings["database settings"][
                "transients"]["loginPath"],
            waitForResult=waitForResult,
            successRule="delete",
            failureRule="failed"
        )

        sqlQuery = ""
        inserts = []
        for k, v in classifications.iteritems():
            thisInsert = {
                "transient_object_id": k,
                "classification": v[0]
            }
            inserts.append(thisInsert)

            classification = v[0]
            sqlQuery += u"""
                    update %(transientTable)s set %(transientTableClassCol)s = "%(classification)s"
                        where %(transientTableIdCol)s  = "%(k)s";
                """ % locals()

        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.transientsDbConn,
        )

        createStatement = """CREATE TABLE IF NOT EXISTS `sherlock_classifications` (
  `transient_object_id` bigint(20) NOT NULL,
  `classification` varchar(45) DEFAULT NULL,
  `annotation` TEXT COLLATE utf8_unicode_ci DEFAULT NULL,
  `summary` VARCHAR(50) COLLATE utf8_unicode_ci DEFAULT NULL,
  `matchVerified` TINYINT NULL DEFAULT NULL,
  `developmentComment` VARCHAR(100) NULL,
  `dateLastModified` datetime DEFAULT NULL,
  `dateCreated` datetime DEFAULT NULL,
  `updated` varchar(45) DEFAULT '0',
  PRIMARY KEY (`transient_object_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
"""
        dataSet = list_of_dictionaries(
            log=self.log,
            listOfDictionaries=inserts,
            reDatetime=re.compile('^[0-9]{4}-[0-9]{2}-[0-9]{2}T')
        )

        # Recursively create missing directories
        if not os.path.exists("/tmp/sherlock/%(myPid)s/classifications" % locals()):
            os.makedirs("/tmp/sherlock/%(myPid)s/classifications" % locals())

        now = datetime.now()
        now = now.strftime("%Y%m%dt%H%M%S")
        mysqlData = dataSet.mysql(tableName="sherlock_classifications",
                                  filepath="/tmp/sherlock/%(myPid)s/classifications/%(now)s.sql" % locals(), createStatement=createStatement)

        if self.fast:
            waitForResult = "delete"
        else:
            waitForResult = True

        directory_script_runner(
            log=self.log,
            pathToScriptDirectory="/tmp/sherlock/%(myPid)s/classifications" % locals(
            ),
            databaseName=self.settings[
                "database settings"]["transients"]["db"],
            loginPath=self.settings["database settings"][
                "transients"]["loginPath"],
            waitForResult=waitForResult,
            successRule="delete",
            failureRule="failed"
        )

        self.log.debug('completed the ``_update_transient_database`` method')
        return None

    def _rank_classifications(
            self,
            crossmatches,
            colMaps):
        """*rank the classifications returned from the catalogue crossmatcher, annotate the results with a classification rank-number (most likely = 1) and a rank-score (weight of classification)*

        **Key Arguments:**
            - ``crossmatches`` -- the unranked crossmatch classifications
            - ``colMaps`` -- maps of the important column names for each table/view in the crossmatch-catalogues database

        **Return:**
            - ``classifications`` -- the classifications assigned to the transients post-crossmatches
            - ``crossmatches`` -- the crossmatches annotated with rankings and rank-scores
        """
        self.log.info('starting the ``_rank_classifications`` method')

        for xm in crossmatches:
            if (xm["physical_separation_kpc"] is not None and xm["physical_separation_kpc"] != "null" and xm["physical_separation_kpc"] < 20. and xm["association_type"] == "SN"):
                rankScore = xm["classificationReliability"] * 1000 + 2. - \
                    colMaps[xm["catalogue_view_name"]][
                        "object_type_accuracy"] * 0.1
            else:
                rankScore = xm["classificationReliability"] * 1000 + xm["separationArcsec"] + 1. - \
                    colMaps[xm["catalogue_view_name"]][
                        "object_type_accuracy"] * 0.1
            xm["rankScore"] = rankScore

        crossmatches = sorted(
            crossmatches, key=itemgetter('rankScore'), reverse=False)
        crossmatches = sorted(
            crossmatches, key=itemgetter('transient_object_id'))

        transient_object_id = None
        classifications = {}
        for xm in crossmatches:
            if transient_object_id != xm["transient_object_id"]:
                if transient_object_id != None:
                    classifications[transient_object_id] = transClass
                transClass = []
                rank = 0
                transient_object_id = xm["transient_object_id"]
            rank += 1
            transClass.append(xm["association_type"])
            xm["rank"] = rank
        if transient_object_id != None:
            classifications[transient_object_id] = transClass

        self.log.info('completed the ``_rank_classifications`` method')
        return classifications, crossmatches

    def _print_results_to_stdout(
            self,
            classifications,
            crossmatches):
        """*print the classification and crossmatch results for a single transient object to stdout*

        **Key Arguments:**
            - ``crossmatches`` -- the unranked crossmatch classifications
            - ``classifications`` -- the classifications assigned to the transients post-crossmatches (dictionary of rank ordered list of classifications)
        """
        self.log.info('starting the ``_print_results_to_stdout`` method')

        if self.verbose == 0:
            return

        if self.name in classifications:
            headline = self.name + "'s Predicted Classification: " + \
                classifications[self.name][0]
        else:
            headline = self.name + "'s Predicted Classification: ORPHAN"
        print headline
        print
        print "Suggested Associations:"

        # REPORT ONLY THE MOST PREFERED MAGNITUDE VALUE
        filterPreference = [
            "R", "_r", "G", "V", "_g", "B", "I", "_i", "_z", "J", "H", "K", "U", "_u", "_y", "unkMag"
        ]
        basic = ["association_type", "rank", "rankScore", "catalogue_table_name", "catalogue_object_id", "catalogue_object_type", "catalogue_object_subtype",
                 "raDeg", "decDeg", "separationArcsec", "physical_separation_kpc", "direct_distance", "distance", "z", "photoZ", "photoZErr", "Mag", "MagFilter", "MagErr", "classificationReliability"]
        verbose = ["search_name", "catalogue_view_name", "original_search_radius_arcsec", "direct_distance_modulus", "distance_modulus", "direct_distance_scale", "major_axis_arcsec", "scale", "U", "UErr",
                   "B", "BErr", "V", "VErr", "R", "RErr", "I", "IErr", "J", "JErr", "H", "HErr", "K", "KErr", "_u", "_uErr", "_g", "_gErr", "_r", "_rErr", "_i", "_iErr", "_z", "_zErr", "_y", "G", "GErr", "_yErr", "unkMag"]
        dontFormat = ["decDeg", "raDeg", "rank",
                      "catalogue_object_id", "catalogue_object_subtype"]

        if self.verbose == 2:
            basic = basic + verbose

        for c in crossmatches:
            for f in filterPreference:
                if f in c and c[f]:
                    c["Mag"] = c[f]
                    c["MagFilter"] = f.replace("_", "").replace("Mag", "")
                    if f + "Err" in c:
                        c["MagErr"] = c[f + "Err"]
                    else:
                        c["MagErr"] = None
                    break

        allKeys = []
        for c in crossmatches:
            for k, v in c.iteritems():
                if k not in allKeys:
                    allKeys.append(k)

        for c in crossmatches:
            for k in allKeys:
                if k not in c:
                    c[k] = None

        printCrossmatches = []
        for c in crossmatches:
            ordDict = collections.OrderedDict(sorted({}.items()))
            for k in basic:
                if k in c:
                    if k == "catalogue_table_name":
                        c[k] = c[k].replace("tcs_cat_", "").replace("_", " ")
                    if k == "classificationReliability":
                        if c[k] == 1:
                            c["classification reliability"] = "synonym"
                        elif c[k] == 2:
                            c["classification reliability"] = "association"
                        elif c[k] == 3:
                            c["classification reliability"] = "annotation"
                        k = "classification reliability"
                    if k == "catalogue_object_subtype" and "sdss" in c["catalogue_table_name"]:
                        if c[k] == 6:
                            c[k] = "galaxy"
                        elif c[k] == 3:
                            c[k] = "star"
                    columnName = k.replace("tcs_cat_", "").replace("_", " ")
                    value = c[k]
                    if k not in dontFormat:

                        try:
                            ordDict[columnName] = "%(value)0.2f" % locals()
                        except:
                            ordDict[columnName] = value
                    else:
                        ordDict[columnName] = value

            printCrossmatches.append(ordDict)

        from fundamentals.renderer import list_of_dictionaries
        dataSet = list_of_dictionaries(
            log=self.log,
            listOfDictionaries=printCrossmatches
        )
        tableData = dataSet.table(filepath=None)

        print tableData

        self.log.info('completed the ``_print_results_to_stdout`` method')
        return None

    def _consolidate_coordinateList(
            self,
            coordinateList):
        """*match the coordinate list against itself with the parameters of the NED search queries to minimise duplicated NED queries*

        **Key Arguments:**
            - ``coordinateList`` -- the original coordinateList.

        **Return:**
            - ``updatedCoordinateList`` -- the coordinate list with duplicated search areas removed

        **Usage:**
            ..  todo::

                - add usage info
                - create a sublime snippet for usage
                - update package tutorial if needed

            .. code-block:: python

                usage code

        """
        self.log.info('starting the ``_consolidate_coordinateList`` method')

        raList = []
        raList[:] = np.array([c[0] for c in coordinateList])
        decList = []
        decList[:] = np.array([c[1] for c in coordinateList])

        nedStreamRadius = self.settings[
            "ned stream search radius arcec"] / (60. * 60.)
        firstPassNedSearchRadius = self.settings[
            "first pass ned search radius arcec"] / (60. * 60.)
        radius = nedStreamRadius - firstPassNedSearchRadius

        # LET'S BE CONSERVATIVE
        # radius = radius * 0.9

        xmatcher = sets(
            log=self.log,
            ra=raList,
            dec=decList,
            radius=radius,  # in degrees
            sourceList=coordinateList,
            convertToArray=False
        )
        allMatches = xmatcher.match

        updatedCoordianteList = []
        for aSet in allMatches:
            updatedCoordianteList.append(aSet[0])

        self.log.info('completed the ``_consolidate_coordinateList`` method')
        return updatedCoordianteList

    def classification_annotations(
            self):
        """*add a detialed classification annotation to each classification in the sherlock_classifications table*

        **Key Arguments:**
            # -

        **Return:**
            - None

        **Usage:**
            ..  todo::

                - add usage info
                - create a sublime snippet for usage
                - write a command-line tool for this method
                - update package tutorial with command-line tool info if needed

            .. code-block:: python

                usage code

        """
        self.log.info('starting the ``classification_annotations`` method')

        from fundamentals.mysql import readquery
        sqlQuery = u"""
            select * from sherlock_classifications cl, sherlock_crossmatches xm where cl.transient_object_id=xm.transient_object_id and cl.annotation is null
        """ % locals()
        topXMs = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.transientsDbConn
        )

        for xm in topXMs:
            annotation = []
            classType = xm["classificationReliability"]
            if classType == 1:
                annotation.append("is synonymous with")
            elif classType in [2, 3]:
                annotation.append("is possibly associated with")

            print xm["catalogue_object_id"]

        self.log.info('completed the ``classification_annotations`` method')
        return None

    # use the tab-trigger below for new method
    def update_classification_annotations_and_summaries(
            self):
        """*update classification annotations and summaries*

        **Key Arguments:**
            # -

        **Return:**
            - None

        **Usage:**
            ..  todo::

                - add usage info
                - create a sublime snippet for usage
                - write a command-line tool for this method
                - update package tutorial with command-line tool info if needed

            .. code-block:: python

                usage code

        """
        self.log.info(
            'starting the ``update_classification_annotations_and_summaries`` method')

        myPid = self.myPid

        sqlQuery = u"""
            SELECT * from sherlock_crossmatches cm, sherlock_classifications cl where rank =1 and cl.transient_object_id=cm.transient_object_id
        """ % locals()
        rows = readquery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.transientsDbConn,
            quiet=False
        )

        from astrocalc.coords import unit_conversion
        # ASTROCALC UNIT CONVERTER OBJECT
        converter = unit_conversion(
            log=self.log
        )

        updates = []

        for row in rows:

            catalogue = row["catalogue_table_name"]
            objectId = row["catalogue_object_id"]
            objectType = row["catalogue_object_type"]
            objectSubtype = row["catalogue_object_subtype"]
            catalogueString = catalogue
            if "catalogue" not in catalogueString.lower():
                catalogueString = catalogue + " catalogue"

            if "ned" in catalogue.lower():
                objectId = '''<a href="https://ned.ipac.caltech.edu/cgi-bin/objsearch?objname=%(objectId)s&extend=no&hconst=73&omegam=0.27&omegav=0.73&corr_z=1&out_csys=Equatorial&out_equinox=J2000.0&obj_sort=RA+or+Longitude&of=pre_text&zv_breaker=30000.0&list_limit=5&img_stamp=YES">%(objectId)s</a>''' % locals()
            elif "sdss" in catalogue.lower():
                objectId = "http://skyserver.sdss.org/dr12/en/tools/explore/Summary.aspx?id=%(objectId)s" % locals(
                )

                ra = converter.ra_decimal_to_sexegesimal(
                    ra=row["raDeg"],
                    delimiter=""
                )
                dec = converter.dec_decimal_to_sexegesimal(
                    dec=row["decDeg"],
                    delimiter=""
                )
                betterName = "SDSS J" + ra[0:9] + dec[0:9]
                objectId = '''<a href="%(objectId)s">%(betterName)s</a>''' % locals()
            elif "milliquas" in catalogue.lower():
                thisName = objectId
                objectId = objectId.replace(" ", "+")
                objectId = '''<a href="https://heasarc.gsfc.nasa.gov/db-perl/W3Browse/w3table.pl?popupFrom=Query+Results&tablehead=name%%3Dheasarc_milliquas%%26description%%3DMillion+Quasars+Catalog+%%28MILLIQUAS%%29%%2C+Version+4.8+%%2822+June+2016%%29%%26url%%3Dhttp%%3A%%2F%%2Fheasarc.gsfc.nasa.gov%%2FW3Browse%%2Fgalaxy-catalog%%2Fmilliquas.html%%26archive%%3DN%%26radius%%3D1%%26mission%%3DGALAXY+CATALOG%%26priority%%3D5%%26tabletype%%3DObject&dummy=Examples+of+query+constraints%%3A&varon=name&bparam_name=%%3D%%22%(objectId)s%%22&bparam_name%%3A%%3Aunit=+&bparam_name%%3A%%3Aformat=char25&varon=ra&bparam_ra=&bparam_ra%%3A%%3Aunit=degree&bparam_ra%%3A%%3Aformat=float8%%3A.5f&varon=dec&bparam_dec=&bparam_dec%%3A%%3Aunit=degree&bparam_dec%%3A%%3Aformat=float8%%3A.5f&varon=bmag&bparam_bmag=&bparam_bmag%%3A%%3Aunit=mag&bparam_bmag%%3A%%3Aformat=float8%%3A4.1f&varon=rmag&bparam_rmag=&bparam_rmag%%3A%%3Aunit=mag&bparam_rmag%%3A%%3Aformat=float8%%3A4.1f&varon=redshift&bparam_redshift=&bparam_redshift%%3A%%3Aunit=+&bparam_redshift%%3A%%3Aformat=float8%%3A6.3f&varon=radio_name&bparam_radio_name=&bparam_radio_name%%3A%%3Aunit=+&bparam_radio_name%%3A%%3Aformat=char22&varon=xray_name&bparam_xray_name=&bparam_xray_name%%3A%%3Aunit=+&bparam_xray_name%%3A%%3Aformat=char22&bparam_lii=&bparam_lii%%3A%%3Aunit=degree&bparam_lii%%3A%%3Aformat=float8%%3A.5f&bparam_bii=&bparam_bii%%3A%%3Aunit=degree&bparam_bii%%3A%%3Aformat=float8%%3A.5f&bparam_broad_type=&bparam_broad_type%%3A%%3Aunit=+&bparam_broad_type%%3A%%3Aformat=char4&bparam_optical_flag=&bparam_optical_flag%%3A%%3Aunit=+&bparam_optical_flag%%3A%%3Aformat=char3&bparam_red_psf_flag=&bparam_red_psf_flag%%3A%%3Aunit=+&bparam_red_psf_flag%%3A%%3Aformat=char1&bparam_blue_psf_flag=&bparam_blue_psf_flag%%3A%%3Aunit=+&bparam_blue_psf_flag%%3A%%3Aformat=char1&bparam_ref_name=&bparam_ref_name%%3A%%3Aunit=+&bparam_ref_name%%3A%%3Aformat=char6&bparam_ref_redshift=&bparam_ref_redshift%%3A%%3Aunit=+&bparam_ref_redshift%%3A%%3Aformat=char6&bparam_qso_prob=&bparam_qso_prob%%3A%%3Aunit=percent&bparam_qso_prob%%3A%%3Aformat=int2%%3A3d&bparam_alt_name_1=&bparam_alt_name_1%%3A%%3Aunit=+&bparam_alt_name_1%%3A%%3Aformat=char22&bparam_alt_name_2=&bparam_alt_name_2%%3A%%3Aunit=+&bparam_alt_name_2%%3A%%3Aformat=char22&Entry=&Coordinates=J2000&Radius=Default&Radius_unit=arcsec&NR=CheckCaches%%2FGRB%%2FSIMBAD%%2BSesame%%2FNED&Time=&ResultMax=1000&displaymode=Display&Action=Start+Search&table=heasarc_milliquas">%(thisName)s</a>''' % locals()

            if objectSubtype and objectSubtype.lower() in ["uvs", "radios", "xray", "qso", "irs", 'uves', 'viss', 'hii', 'gclstr', 'ggroup', 'gpair', 'gtrpl']:
                objectType = objectSubtype

            if objectType == "star":
                objectType = "stellar source"
            elif objectType == "agn":
                objectType = "AGN"
            elif objectType == "cb":
                objectType = "CV"
            elif objectType == "unknown":
                objectType = "unclassified source"

            sep = row["separationArcsec"]
            if row["classificationReliability"] == 1:
                classificationReliability = "synonymous"
                psep = row["physical_separation_kpc"]
                if psep:
                    location = '%(sep)0.1f" (%(psep)0.1f Kpc) from the %(objectType)s core' % locals(
                    )
                else:
                    location = '%(sep)0.1f" from the %(objectType)s core' % locals(
                    )
            elif row["classificationReliability"] in (2, 3):
                classificationReliability = "possibly associated"
                n = row["northSeparationArcsec"]
                if n < 0:
                    nd = "S"
                else:
                    nd = "N"
                e = row["eastSeparationArcsec"]
                if e < 0:
                    ed = "W"
                else:
                    ed = "E"
                n = math.fabs(n)
                e = math.fabs(e)
                psep = row["physical_separation_kpc"]
                if psep:
                    location = '%(n)0.2f" %(nd)s, %(e)0.2f" %(ed)s (%(psep)0.1f Kpc) from the %(objectType)s centre' % locals(
                    )
                else:
                    location = '%(n)0.2f" %(nd)s, %(e)0.2f" %(ed)s from the %(objectType)s centre' % locals(
                    )
                location = location.replace("unclassified", "object's")

            best_mag = None
            best_mag_error = None
            best_mag_filter = None
            filters = ["R", "V", "B", "I", "J", "G", "H", "K", "U",
                       "_r", "_g", "_i", "_g", "_z", "_y", "_u", "unkMag"]
            for f in filters:
                if row[f] and not best_mag:
                    best_mag = row[f]
                    best_mag_error = row[f + "Err"]
                    subfilter = f.replace(
                        "_", "").replace("Mag", "")
                    best_mag_filter = f.replace(
                        "_", "").replace("Mag", "") + "="
                    if "unk" in best_mag_filter:
                        best_mag_filter = ""
                        subfilter = ''

            if not best_mag_filter:
                if str(best_mag).lower() in ("8", "11", "18"):
                    best_mag_filter = "an "
                else:
                    best_mag_filter = "a "
            else:
                if str(best_mag_filter)[0].lower() in ("r", "i", "h"):
                    best_mag_filter = "an " + best_mag_filter
                else:
                    best_mag_filter = "a " + best_mag_filter
            if not best_mag:
                best_mag = "an unknown-"
                best_mag_filter = ""
            else:
                best_mag = "%(best_mag)0.2f " % locals()

            distance = None
            if row["direct_distance"]:
                d = row["direct_distance"]
                distance = "distance of %(d)0.1f Mpc" % locals()

                if row["z"]:
                    z = row["z"]
                    distance += "(z=%(z)0.3f)" % locals()
            elif row["z"]:
                z = row["z"]
                distance = "z=%(z)0.3f" % locals()
            elif row["photoZ"]:
                z = row["photoZ"]
                zErr = row["photoZErr"]
                distance = "photoZ=%(z)0.3f (&plusmn%(zErr)0.3f)" % locals()

            if distance:
                distance = "%(distance)s" % locals()

            if distance:
                absMag = row["transientAbsMag"]
                absMag = """ A host %(distance)s implies a transient <em>M =</em> %(absMag)s.""" % locals(
                )
            else:
                absMag = ""

            annotation = "The transient is %(classificationReliability)s with <em>%(objectId)s</em>; %(best_mag_filter)s%(best_mag)smag %(objectType)s found in the %(catalogueString)s. It's located %(location)s.%(absMag)s" % locals()
            summary = '%(sep)0.1f" from %(objectType)s in %(catalogue)s' % locals(
            )

            update = {
                "transient_object_id": row["transient_object_id"],
                "annotation": annotation,
                "summary": summary
            }
            updates.append(update)

        # Recursively create missing directories
        if not os.path.exists("/tmp/sherlock/%(myPid)s" % locals()):
            os.makedirs("/tmp/sherlock/%(myPid)s" % locals())

        dataSet = list_of_dictionaries(
            log=self.log,
            listOfDictionaries=updates,
            reDatetime=re.compile('^[0-9]{4}-[0-9]{2}-[0-9]{2}T')
        )
        mysqlData = dataSet.mysql(
            tableName="sherlock_classifications", filepath="/tmp/sherlock/%(myPid)s/annotation_updates.sql" % locals())

        directory_script_runner(
            log=self.log,
            pathToScriptDirectory="/tmp/sherlock/" + self.myPid,
            databaseName=self.settings[
                "database settings"]["transients"]["db"],
            loginPath=self.settings["database settings"][
                "transients"]["loginPath"],
            waitForResult=True,
            successRule="delete",
            failureRule="failed"
        )

        from fundamentals.mysql import writequery
        sqlQuery = """update sherlock_classifications  set annotation = "The transient location is not matched against any known catalogued source", summary = "No catalogued match" where classification = 'ORPHAN' """ % locals()
        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.transientsDbConn,
        )

        self.log.info(
            'completed the ``update_classification_annotations_and_summaries`` method')
        return None

    # use the tab-trigger below for new method
    def update_peak_magnitudes(
            self):
        """*update peak magnitudes*

        **Key Arguments:**
            # -

        **Return:**
            - None

        **Usage:**
            ..  todo::

                - add usage info
                - create a sublime snippet for usage
                - write a command-line tool for this method
                - update package tutorial with command-line tool info if needed

            .. code-block:: python 

                usage code 

        """
        self.log.info('starting the ``update_peak_magnitudes`` method')

        sqlQuery = self.settings["database settings"][
            "transients"]["transient peak magnitude query"]

        sqlQuery = """UPDATE sherlock_crossmatches s,
            (%(sqlQuery)s) t 
        SET 
            s.transientAbsMag = ROUND(t.mag - IFNULL(direct_distance_modulus,
                            distance_modulus),
                    2)
        WHERE
            IFNULL(direct_distance_modulus,
                    distance_modulus) IS NOT NULL
                AND t.id = s.transient_object_id;""" % locals()

        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.transientsDbConn,
        )

        self.log.info('completed the ``update_peak_magnitudes`` method')
        return None

    # use the tab-trigger below for new method
    # xt-class-method
