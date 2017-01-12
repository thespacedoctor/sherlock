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
os.environ['TERM'] = 'vt100'
from datetime import datetime, date, time, timedelta
from fundamentals import tools
from fundamentals.mysql import readquery, directory_script_runner, writequery
from fundamentals.renderer import list_of_dictionaries
from HMpTy.mysql import conesearch
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


    **Usage:**

        To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_).

        To initiate a transient_classifier object, use the following:

        .. todo::

            - add usage info
            - create a sublime snippet for usage
            - update the package tutorial if needed

        .. code-block:: python

            usage code
    """
    # INITIALISATION

    def __init__(
            self,
            log,
            settings=False,
            update=False,
            ra=False,
            dec=False,
            name=False
    ):
        self.log = log
        log.debug("instansiating a new 'classifier' object")
        self.settings = settings
        self.update = update
        self.ra = ra
        self.dec = dec
        self.name = name

        # xt-self-arg-tmpx

        # INITIAL ACTIONS
        # SETUP DATABASE CONNECTIONS
        # SETUP ALL DATABASE CONNECTIONS
        from sherlock import database
        db = database(
            log=self.log,
            settings=self.settings
        )
        dbConns = db.connect()
        self.transientsDbConn = dbConns["transients"]
        self.cataloguesDbConn = dbConns["catalogues"]
        self.pmDbConn = dbConns["marshall"]

        return None

    def classify(self):
        """
        *classify the transients selected from the transient selection query in the settings file or passed in via the CL*

        **Return:**
            - None

        **Usage:**
        .. todo::

            - add usage info
            - create a sublime snippet for usage
            - update the package tutorial if needed

        .. code-block:: python

            usage code
        """
        self.log.info('starting the ``classify`` method')

        # IF A TRANSIENT HAS NOT BEEN PASSED IN VIA THE COMMAND-LINE, THEN
        # QUERY THE TRANSIENT DATABASE
        if not self.ra and not self.dec:
            # A LIST OF DICTIONARIES OF TRANSIENT METADATA
            transientsMetadataList = self._get_transient_metadata_from_database_list()
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

        # THE COLUMN MAPS - WHICH COLUMNS IN THE CATALOGUE TABLES = RA, DEC,
        # REDSHIFT, MAG ETC
        colMaps = get_crossmatch_catalogues_column_map(
            log=self.log,
            dbConn=self.cataloguesDbConn
        )

        # FROM THE LOCATIONS OF THE TRANSIENTS, CHECK IF OUR LOCAL NED DATABASE
        # NEEDS UPDATED
        self._update_ned_stream(
            transientsMetadataList=transientsMetadataList
        )

        batchSize = 10
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
            print "BATCH %(count)s " % locals()
            count += 1
            # RUN THE CROSSMATCH CLASSIFIER FOR ALL TRANSIENTS
            classifications = self._crossmatch_transients_against_catalogues(
                colMaps=colMaps,
                transientsMetadataList=batch
            )

            # for c in classifications[0]["crossmatches"]:
            #     for k, v in c.iteritems():
            #         print k, v
            #     print

            # UPDATE THE TRANSIENT DATABASE IF UPDATE REQUESTED (ADD DATA TO
            # tcs_crossmatch_table AND A CLASSIFICATION TO THE ORIGINAL TRANSIENT
            # TABLE)
            if self.update:
                self._update_transient_database(
                    classifications=classifications,
                    transientsMetadataList=batch,
                    colMaps=colMaps
                )

        self.log.info('completed the ``classify`` method')
        return None

    def _get_transient_metadata_from_database_list(
            self):
        """use the transient query in the settings file to generate a list of transients to corssmatch and classify

         **Return:**
            - ``transientsMetadataList``
        """
        self.log.debug(
            'starting the ``_get_transient_metadata_from_database_list`` method')

        sqlQuery = self.settings["database settings"][
            "transients"]["transient query"]
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
            thisList = str(i["ra"]) + " " + str(i["dec"])
            coordinateList.append(thisList)

        coordinateList = self._remove_previous_ned_queries(
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

        # CALCULATE THE OLDEST RESULTS LIMIT
        now = datetime.now()
        td = timedelta(
            days=self.settings["ned stream refresh rate in days"])
        refreshLimit = now - td
        refreshLimit = refreshLimit.strftime("%Y-%m-%d %H:%M:%S")

        from astrocalc.coords import unit_conversion
        # ASTROCALC UNIT CONVERTER OBJECT
        converter = unit_conversion(
            log=self.log
        )

        # FOR EACH TRANSIENT IN COORDINATE LIST
        for c in coordinateList:
            this = c.split(" ")

            raDeg = converter.ra_sexegesimal_to_decimal(
                ra=this[0]
            )
            decDeg = converter.dec_sexegesimal_to_decimal(
                dec=this[1]
            )

            cs = conesearch(
                log=self.log,
                dbConn=self.cataloguesDbConn,
                tableName="tcs_helper_ned_query_history",
                columns="*",
                ra=raDeg,
                dec=decDeg,
                radiusArcsec=radius,
                separations=True,
                distinct=True,
                sqlWhere="dateQueried > '%(refreshLimit)s'" % locals(),
                closest=True
            )
            matchIndies, matches = cs.search()

            # DETERMINE WHICH COORDINATES REQUIRE A NED QUERY
            match = False
            for row in matches.list:
                row["separationArcsec"] = row["cmSepArcsec"]
                raStream = row["raDeg"]
                decStream = row["decDeg"]
                radiusStream = row["arcsecRadius"]
                dateStream = row["dateQueried"]
                angularSeparation = row["separationArcsec"]

                if angularSeparation + self.settings["first pass ned search radius arcec"] < radiusStream:
                    match = True

            if match == False:
                updatedCoordinateList.append(c)

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
            - None

        **Usage:**
            ..  todo::

                - add usage info
                - create a sublime snippet for usage
                - update package tutorial if needed

            .. code-block:: python 

                usage code 

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
        classifications = cm.match()

        self.log.debug(
            'completed the ``_crossmatch_transients_against_catalogues`` method')
        return classifications

    def _update_transient_database(
            self,
            classifications,
            transientsMetadataList,
            colMaps):
        """ update transient database with classifications and crossmatch results

        **Key Arguments:**
            - ``classifications`` -- the classifications and associations resulting from the catlaogue crossmatches
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

        # RECURSIVELY CREATE MISSING DIRECTORIES
        if not os.path.exists("/tmp/sherlock"):
            os.makedirs("/tmp/sherlock")

        # COMBINE ALL CROSSMATCHES INTO A LIST OF DICTIONARIES TO DUMP INTO
        # DATABASE TABLE
        transientIDs = []
        transientIDs[:] = [str(c["transient_object_id"])
                           for c in classifications]
        transientIDs = ",".join(transientIDs)

        createStatement = """
CREATE TABLE IF NOT EXISTS `tcs_cross_matches` (
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
  `dateCreated` DATETIME DEFAULT NOW(),
  `physical_separation_kpc` double DEFAULT NULL,
  `catalogue_object_type` varchar(45) DEFAULT NULL,
  `catalogue_object_subtype` varchar(45) DEFAULT NULL,
  `association_rank` int(11) DEFAULT NULL,
  `catalogue_table_name` varchar(100) DEFAULT NULL,
  `catalogue_view_name` varchar(100) DEFAULT NULL,
  `rank` int(11) DEFAULT NULL,
  `search_name` varchar(100) DEFAULT NULL,
  `major_axis_arcsec` double DEFAULT NULL,
  `direct_distance` double DEFAULT NULL,
  `direct_distance_scale` double DEFAULT NULL,
  `direct_distance_modulus` double DEFAULT NULL,
  `raDeg` double DEFAULT NULL,
  `decDeg` double DEFAULT NULL,
  `original_search_radius_arcsec` double DEFAULT NULL,
  `catalogue_view_id` int(11) DEFAULT NULL,
  `catalogue_object_mag` float DEFAULT NULL,
  `catalogue_object_filter` varchar(10) DEFAULT NULL,
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
  `unkMag` double DEFAULT NULL,
  `unkMagErr` double DEFAULT NULL,
  `dateLastModified` DATETIME NULL DEFAULT NOW(),
  `updated` TINYINT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `key_transient_object_id` (`transient_object_id`),
  KEY `key_catalogue_object_id` (`catalogue_object_id`),
  KEY `idx_separationArcsec` (`separationArcsec`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;

delete from tcs_cross_matches where transient_object_id in (%(transientIDs)s);
""" % locals()

        dataSet = list_of_dictionaries(
            log=self.log,
            listOfDictionaries=classifications
        )

        mysqlData = dataSet.mysql(
            tableName="tcs_cross_matches", filepath="/tmp/sherlock/%(now)s_cm_results.sql" % locals(), createStatement=createStatement)

        directory_script_runner(
            log=self.log,
            pathToScriptDirectory="/tmp/sherlock",
            databaseName=self.settings[
                "database settings"]["transients"]["db"],
            loginPath=self.settings["database settings"][
                "transients"]["loginPath"],
            successRule="delete",
            failureRule="failed"
        )

        for ob in transientsMetadataList:
            transId = ob["id"]
            name = ob["name"]

            sqlQuery = u"""
                select id, separationArcsec, catalogue_view_name, association_type, physical_separation_kpc, major_axis_arcsec from tcs_cross_matches where transient_object_id = %(transId)s order by separationArcsec
            """ % locals()
            rows = readquery(
                log=self.log,
                sqlQuery=sqlQuery,
                dbConn=self.transientsDbConn,
                quiet=False
            )

            rankScores = []
            for row in rows:
                if row["separationArcsec"] < 2. or (row["physical_separation_kpc"] != "null" and row["physical_separation_kpc"] < 20. and row["association_type"] == "SN") or (row["major_axis_arcsec"] != "null" and row["association_type"] == "SN"):
                    # print row["separationArcsec"]
                    # print row["physical_separation_kpc"]
                    # print row["major_axis_arcsec"]
                    rankScore = 2. - \
                        colMaps[row["catalogue_view_name"]][
                            "object_type_accuracy"] * 0.1
                    # print rankScore
                else:
                    # print row["separationArcsec"]
                    # print row["physical_separation_kpc"]
                    # print row["major_axis_arcsec"]
                    rankScore = row["separationArcsec"] + 1. - \
                        colMaps[row["catalogue_view_name"]][
                            "object_type_accuracy"] * 0.1
                rankScores.append(rankScore)

            rank = 0
            for rs, row in sorted(zip(rankScores, rows)):
                rank += 1
                primaryId = row["id"]
                sqlQuery = u"""
                    update tcs_cross_matches set rank = %(rank)s where id = %(primaryId)s
                """ % locals()
                rows = readquery(
                    log=self.log,
                    sqlQuery=sqlQuery,
                    dbConn=self.transientsDbConn,
                    quiet=False
                )

            sqlQuery = u"""
               select distinct association_type from (select association_type from tcs_cross_matches where transient_object_id = %(transId)s  order by rank) as alias;
            """ % locals()
            rows = readquery(
                log=self.log,
                sqlQuery=sqlQuery,
                dbConn=self.transientsDbConn,
                quiet=False
            )

            classification = ""
            for row in rows:
                classification += row["association_type"] + "/"
            classification = classification[:-1]
            if len(rows):
                classification = rows[0]["association_type"]

            if len(classification) == 0:
                classification = "ORPHAN"

            sqlQuery = u"""
                    update %(transientTable)s  set %(transientTableClassCol)s = "%(classification)s"
                        where %(transientTableIdCol)s  = %(transId)s
                """ % locals()

            print """%(name)s: %(classification)s """ % locals()

            writequery(
                log=self.log,
                sqlQuery=sqlQuery,
                dbConn=self.transientsDbConn,
            )

        self.log.debug('completed the ``_update_transient_database`` method')
        return None

    # use the tab-trigger below for new method
    # xt-class-method
