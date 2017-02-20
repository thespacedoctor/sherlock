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
                    crossmatchTable: tcs_cross_matches
                    tunnel: False

        By setting ``update=True`` the classifier will update the ``sherlockClassification`` column of the ``transient table`` with new classification and populate the ``crossmatchTable`` with key details of the crossmatched sources from the catalogues database. By setting ``update=False`` results are printed to stdout but the database is not updated (useful for dry runs and testing new algorithms),

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
            updateNed=True
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
                print "No transients need classified"
                return None, None

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

        now = datetime.now()
        now = now.strftime("%Y-%m-%d_%H-%M-%S-%f")

        transientTable = self.settings["database settings"][
            "transients"]["transient table"]
        transientTableClassCol = self.settings["database settings"][
            "transients"]["transient classification column"]
        transientTableIdCol = self.settings["database settings"][
            "transients"]["transient primary id column"]
        crossmatchTable = self.settings["database settings"][
            "transients"]["crossmatchTable"]

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
                "/tmp/sherlock_trigger/%(crossmatchTable)s_trigger.sql" % locals())
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
                if not os.path.exists("/tmp/sherlock_trigger"):
                    os.makedirs("/tmp/sherlock_trigger")

                pathToWriteFile = "/tmp/sherlock_trigger/%(crossmatchTable)s_trigger.sql" % locals(
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
                    pathToScriptDirectory="/tmp/sherlock_trigger",
                    databaseName=self.settings[
                        "database settings"]["transients"]["db"],
                    loginPath=self.settings["database settings"][
                        "transients"]["loginPath"],
                    waitForResult=False
                )

        dataSet = list_of_dictionaries(
            log=self.log,
            listOfDictionaries=crossmatches,
            reDatetime=self.reDatetime
        )

        mysqlData = dataSet.mysql(
            tableName=crossmatchTable, filepath="/tmp/sherlock/%(now)s_cm_results.sql" % locals(), createStatement=createStatement)

        if self.fast:
            waitForResult = "delete"
        else:
            waitForResult = True

        directory_script_runner(
            log=self.log,
            pathToScriptDirectory="/tmp/sherlock",
            databaseName=self.settings[
                "database settings"]["transients"]["db"],
            loginPath=self.settings["database settings"][
                "transients"]["loginPath"],
            waitForResult=waitForResult,
            successRule="delete",
            failureRule="failed"
        )

        sqlQuery = ""
        for k, v in classifications.iteritems():
            classification = v[0]
            sqlQuery += u"""
                    update %(transientTable)s  set %(transientTableClassCol)s = "%(classification)s"
                        where %(transientTableIdCol)s  = "%(k)s";
                """ % locals()

        writequery(
            log=self.log,
            sqlQuery=sqlQuery,
            dbConn=self.transientsDbConn,
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
            if xm["separationArcsec"] < 2. or (xm["physical_separation_kpc"] != "null" and xm["physical_separation_kpc"] < 20. and xm["association_type"] == "SN") or (xm["major_axis_arcsec"] != "null" and xm["association_type"] == "SN"):
                rankScore = 2. - \
                    colMaps[xm["catalogue_view_name"]][
                        "object_type_accuracy"] * 0.1
                # print rankScore
            else:
                rankScore = xm["separationArcsec"] + 1. - \
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

        headline = self.name + "'s Predicted Classification: " + \
            classifications[self.name][0]
        print headline
        print
        print "Suggested Associations:"

        # REPORT ONLY THE MOST PREFERED MAGNITUDE VALUE
        filterPreference = [
            "R", "_r", "G", "V", "_g", "B", "I", "_i", "_z", "J", "H", "K", "U", "_u", "_y", "unkMag"
        ]
        basic = ["association_type", "rank", "rankScore", "catalogue_table_name", "catalogue_object_id", "catalogue_object_type", "catalogue_object_subtype",
                 "raDeg", "decDeg", "separationArcsec", "physical_separation_kpc", "direct_distance", "distance", "z", "photoZ", "photoZErr", "Mag", "MagFilter", "MagErr"]
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

    # use the tab-trigger below for new method
    # xt-class-method
