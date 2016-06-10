#!/usr/local/bin/python
# encoding: utf-8
"""
*The classifier object for Sherlock*

:Author:
    David Young

:Date Created:
    June 29, 2015

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
import time
import MySQLdb as ms
from docopt import docopt
from dryxPython import logs as dl
from dryxPython import commonutils as dcu
from dryxPython import mysql as dms
from fundamentals import tools, times
from sherlock.imports.ned_conesearch import ned_conesearch


class classifier():

    """
    *The classifier object for Sherlock*

    **Key Arguments:**
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary
        - ``update`` -- update the transient database with crossmatch results (boolean)
        - ``transientIdList`` -- a list of individual transient ids (overrides `workflowListId` - used if user want to run the classifier on specific transients)
    """
    # INITIALISATION

    def __init__(
            self,
            log,
            settings=False,
            update=False,
            transientIdList=[]
    ):
        self.log = log
        log.debug("instansiating a new 'classifier' object")
        self.settings = settings
        self.update = update
        self.transientIdList = transientIdList
        # xt-self-arg-tmpx

        # VARIABLE DATA ATRRIBUTES
        self.transientsMetadataList = []

        # INITIAL ACTIONS
        # SETUP DATABASE CONNECTIONS
        from sherlock import database
        db = database(
            log=self.log,
            settings=self.settings
        )
        self.transientsDbConn, self.cataloguesDbConn, self.pmDbConn = db.get()

        return None

    # METHOD ATTRIBUTES

    def get(self):
        """
        *perform the classifications*
        """
        self.log.debug('starting the ``get`` method')

        self._create_crossmatch_table_if_not_existing()
        self._grab_column_name_map_from_database()

        # CHOOSE METHOD TO GRAB TRANSIENT METADATA BEFORE CLASSIFICATION
        # EITHER LIST OF IDS, OR A STREAM FROM THE DATABASE
        if len(self.transientIdList) > 0:
            self._get_individual_transient_metadata()
        else:
            self._get_transient_metadata_from_database_list()

        self._update_ned_stream()
        self._crossmatch_transients_against_catalogues()

        if self.update:
            self._update_transient_database()

        self.log.debug('completed the ``get`` method')
        return None

    def _get_individual_transient_metadata(
            self):
        """
        *get individual transient metadata from the transient database*
        """
        self.log.debug(
            'starting the ``_get_individual_transient_metadata`` method')

        for tran in self.transientIdList:
            sqlQuery = u"""
                select id, followup_id, ra_psf 'ra', dec_psf 'dec', local_designation 'name', ps1_designation, object_classification, local_comments, detection_list_id
                from tcs_transient_objects
                where id = %(tran)s
            """ % locals()
            rows = dms.execute_mysql_read_query(
                sqlQuery=sqlQuery,
                dbConn=self.transientsDbConn,
                log=self.log
            )
            if len(rows):
                self.transientsMetadataList.append(rows[0])
            else:
                log.warning(
                    'could not find transient in database with id %(tran)s' % locals())

        self.log.debug(
            'completed the ``_get_individual_transient_metadata`` method')
        return None

    def _get_transient_metadata_from_database_list(
            self):
        """
        *get transient metadata from a given workflow list in the transient database*
        """
        self.log.debug(
            'starting the ``_get_transient_metadata_from_database_list`` method')

        sqlQuery = self.settings["database settings"][
            "transients"]["transient query"]
        self.transientsMetadataList = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.transientsDbConn,
            log=self.log
        )

        self.log.debug(
            'completed the ``_get_transient_metadata_from_database_list`` method')
        return None

    def _crossmatch_transients_against_catalogues(
            self):
        """
        *crossmatch transients against catalogue*

        **Key Arguments:**
            # -

        **Return:**
            - None

        .. todo::

            - @review: when complete, clean _crossmatch_transients_against_catalogues method
            - @review: when complete add logging
        """
        self.log.debug(
            'starting the ``_crossmatch_transients_against_catalogues`` method')

        from sherlock import crossmatcher
        self.allClassifications = []

        cm = crossmatcher(
            log=self.log,
            dbConn=self.cataloguesDbConn,
            transients=self.transientsMetadataList,
            settings=self.settings,
            colMaps=self.colMaps
        )
        self.classifications = cm.get()
        # tid = trans["id"]
        # name = tran["name"]
        # classification = self.getFlagDefs(
        #     objectType, CLASSIFICATION_FLAGS, delimiter=' ').upper()
        # oldClassification = self.getFlagDefs(
        #     trans['object_classification'], CLASSIFICATION_FLAGS, delimiter=' ').upper()
        # print "*** Object ID = %(tid)s (%(name)s): CLASSIFICATION =
        # %(classification)s (PREVIOUSLY = %(oldClassification)s)" % locals()

        self.log.debug(
            'completed the ``_crossmatch_transients_against_catalogues`` method')
        return None

    # use the tab-trigger below for new method
    def _update_transient_database(
            self):
        """
        *update transient database*

        **Key Arguments:**
            # -

        **Return:**
            - None

        .. todo::

            - @review: when complete, clean _update_transient_database method
            - @review: when complete add logging
        """
        self.log.debug('starting the ``_update_transient_database`` method')

        from datetime import datetime, date, time
        now = datetime.now()
        now = now.strftime("%Y-%m-%d %H:%M:%S")

        transientTable = self.settings["database settings"][
            "transients"]["transient table"]
        transientTableClassCol = self.settings["database settings"][
            "transients"]["transient classification column"]
        transientTableIdCol = self.settings["database settings"][
            "transients"]["transient primary id column"]

        for c in self.classifications:

            objectType = c["object_classification_new"]
            transientObjectId = c["id"]

            # DELETE PREVIOUS CROSSMATCHES
            sqlQuery = u"""
                    delete from tcs_cross_matches where transient_object_id = %(transientObjectId)s
                """ % locals()
            dms.execute_mysql_write_query(
                sqlQuery=sqlQuery,
                dbConn=self.transientsDbConn,
                log=self.log
            )

            # INSERT NEW CROSSMATCHES
            for crossmatch in c["crossmatches"]:
                for k, v in crossmatch.iteritems():
                    if v == None:
                        crossmatch[k] = "null"
                if "physical_separation_kpc" not in crossmatch.keys():
                    crossmatch["physical_separation_kpc"] = "null"
                if "sourceFilter" not in crossmatch.keys():
                    crossmatch["sourceFilter"] = "null"
                if "sourceMagnitude" not in crossmatch.keys():
                    crossmatch["sourceMagnitude"] = "null"
                else:
                    try:
                        crossmatch["sourceMagnitude"] = float(
                            crossmatch["sourceMagnitude"])
                    except:
                        this = crossmatch["sourceMagnitude"]
                        crossmatch["sourceMagnitude"] = '"%(this)s"' % locals()

                if crossmatch["sourceSubType"] and "null" not in str(crossmatch["sourceSubType"]):
                    crossmatch["sourceSubType"] = '"%s"' % (crossmatch[
                        "sourceSubType"],)
                else:
                    crossmatch["sourceSubType"] = "null"

                sqlQuery = u"""
                        insert into tcs_cross_matches (
                           transient_object_id,
                           catalogue_object_id,
                           catalogue_table_id,
                           catalogue_view_id,
                           catalogue_object_ra,
                           catalogue_object_dec,
                           catalogue_object_mag,
                           catalogue_object_filter,
                           original_search_radius_arcsec,
                           separation,
                           z,
                           scale,
                           distance,
                           distance_modulus,
                           date_added,
                           association_type,
                           physical_separation_kpc,
                           catalogue_object_type,
                           catalogue_object_subtype,
                           catalogue_table_name,
                           catalogue_view_name,
                           search_name,
                           major_axis_arcsec,
                           direct_distance,
                           direct_distance_scale,
                           direct_distance_modulus
                           )
                        values (
                           %s,
                           "%s",
                           %s,
                           %s,
                           %s,
                           %s,
                           %s,
                           %s,
                           %s,
                           %s,
                           %s,
                           %s,
                           %s,
                           %s,
                           "%s",
                           "%s",
                           %s,
                           "%s",
                           %s,
                           "%s",
                           "%s",
                           "%s",
                           %s,
                           %s,
                           %s,
                           %s)
                        """ % (crossmatch["transientObjectId"], crossmatch["catalogueObjectId"], crossmatch["catalogueTableId"], crossmatch["catalogueViewId"], crossmatch["sourceRa"], crossmatch["sourceDec"], crossmatch["sourceMagnitude"],  crossmatch["sourceFilter"], crossmatch["originalSearchRadius"], crossmatch["separation"], crossmatch["z"], crossmatch["scale"], crossmatch["distance"], crossmatch["distanceModulus"], now, crossmatch["association_type"], crossmatch["physical_separation_kpc"], crossmatch["sourceType"], crossmatch["sourceSubType"], crossmatch["catalogueTableName"], crossmatch["catalogueViewName"], crossmatch["searchName"], crossmatch["xmmajoraxis"], crossmatch["xmdirectdistance"], crossmatch["xmdirectdistancescale"], crossmatch["xmdirectdistanceModulus"])
                dms.execute_mysql_write_query(
                    sqlQuery=sqlQuery,
                    dbConn=self.transientsDbConn,
                    log=self.log
                )

        for ob in self.transientsMetadataList:
            transId = ob["id"]
            name = ob["name"]

            sqlQuery = u"""
                select id, separation, catalogue_view_name, association_type, physical_separation_kpc, major_axis_arcsec from tcs_cross_matches where transient_object_id = %(transId)s order by separation
            """ % locals()
            rows = dms.execute_mysql_read_query(
                sqlQuery=sqlQuery,
                dbConn=self.transientsDbConn,
                log=self.log
            )

            rankScores = []
            for row in rows:
                if row["separation"] < 2. or (row["physical_separation_kpc"] != "null" and row["physical_separation_kpc"] < 20. and row["association_type"] == "SN") or (row["major_axis_arcsec"] != "null" and row["association_type"] == "SN"):
                    # print row["separation"]
                    # print row["physical_separation_kpc"]
                    # print row["major_axis_arcsec"]
                    rankScore = 2. - \
                        self.colMaps[row["catalogue_view_name"]][
                            "object_type_accuracy"] * 0.1
                    # print rankScore
                else:
                    # print row["separation"]
                    # print row["physical_separation_kpc"]
                    # print row["major_axis_arcsec"]
                    rankScore = row["separation"] + 1. - \
                        self.colMaps[row["catalogue_view_name"]][
                            "object_type_accuracy"] * 0.1
                rankScores.append(rankScore)

            rank = 0
            for rs, row in sorted(zip(rankScores, rows)):
                rank += 1
                primaryId = row["id"]
                sqlQuery = u"""
                    update tcs_cross_matches set rank = %(rank)s where id = %(primaryId)s
                """ % locals()
                rows = dms.execute_mysql_read_query(
                    sqlQuery=sqlQuery,
                    dbConn=self.transientsDbConn,
                    log=self.log
                )

            sqlQuery = u"""
               select distinct association_type from (select association_type from tcs_cross_matches where transient_object_id = %(transId)s  order by rank) as alias;
            """ % locals()
            rows = dms.execute_mysql_read_query(
                sqlQuery=sqlQuery,
                dbConn=self.transientsDbConn,
                log=self.log
            )

            classification = ""
            if len(rows):
                classification = rows[0]["association_type"]
            else:
                classification = "ORPHAN"

            sqlQuery = u"""
                    update %(transientTable)s  set %(transientTableClassCol)s = "%(classification)s"
                        where %(transientTableIdCol)s  = %(transId)s
                """ % locals()

            print """%(name)s: %(classification)s """ % locals()

            dms.execute_mysql_write_query(
                sqlQuery=sqlQuery,
                dbConn=self.transientsDbConn,
                log=self.log
            )

        self.log.debug('completed the ``_update_transient_database`` method')
        return None

    # use the tab-trigger below for new method
    def _update_ned_stream(
            self):
        """
        *update ned stream*

        **Key Arguments:**
            # -

        **Return:**
            - None

        .. todo::

            - @review: when complete, clean _update_ned_stream method
            - @review: when complete add logging
        """
        self.log.info('starting the ``_update_ned_stream`` method')

        coordinateList = []
        for i in self.transientsMetadataList:
            thisList = str(i["ra"]) + " " + str(i["dec"])
            coordinateList.append(thisList)

        coordinateList = self._remove_previous_ned_queries(
            coordinateList=coordinateList
        )

        this = ned_conesearch(
            log=self.log,
            settings=self.settings,
            coordinateList=coordinateList
        )
        this.get()

        self.log.info('completed the ``_update_ned_stream`` method')
        return None

    def _grab_column_name_map_from_database(
            self):
        """
        *grab column name map from database*

        **Return:**
            - None
        """
        self.log.info(
            'starting the ``_grab_column_name_map_from_database`` method')

        # GRAB THE NAMES OF THE IMPORTANT COLUMNS FROM DATABASE
        sqlQuery = u"""
            select v.id as view_id, view_name, raColName, decColName, object_type, subTypeColName, objectNameColName, redshiftColName, distanceColName, semiMajorColName, semiMajorToArcsec, table_id, table_name, object_type_accuracy from tcs_helper_catalogue_views_info v, tcs_helper_catalogue_tables_info t where v.table_id = t.id
        """ % locals()
        rows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )
        self.colMaps = {}
        for row in rows:
            self.colMaps[row["view_name"]] = row

        self.log.info(
            'completed the ``_grab_column_name_map_from_database`` method')
        return None

    def _create_crossmatch_table_if_not_existing(
            self):
        """
        *create crossmatch table if it does not yet exist in the transients database*

        **Return:**
            - None
        """
        self.log.info(
            'starting the ``_create_crossmatch_table_if_not_existing`` method')

        sqlQuery = u"""
            CREATE TABLE `tcs_cross_matches` (
              `transient_object_id` bigint(20) unsigned NOT NULL,
              `catalogue_object_id` varchar(30) COLLATE utf8_unicode_ci NOT NULL,
              `catalogue_table_id` smallint(5) unsigned NOT NULL,
              `separation` double DEFAULT NULL,
              `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
              `z` double DEFAULT NULL,
              `scale` double DEFAULT NULL,
              `distance` double DEFAULT NULL,
              `distance_modulus` double DEFAULT NULL,
              `association_type` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,
              `date_added` datetime DEFAULT NULL,
              `physical_separation_kpc` double DEFAULT NULL,
              `catalogue_object_type` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,
              `catalogue_object_subtype` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,
              `association_rank` int(11) DEFAULT NULL,
              `catalogue_table_name` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
              `catalogue_view_name` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
              `rank` int(11) DEFAULT NULL,
              `search_name` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
              `major_axis_arcsec` double DEFAULT NULL,
              `direct_distance` double DEFAULT NULL,
              `direct_distance_scale` double DEFAULT NULL,
              `direct_distance_modulus` double DEFAULT NULL,
              PRIMARY KEY (`id`),
              KEY `key_transient_object_id` (`transient_object_id`),
              KEY `key_catalogue_object_id` (`catalogue_object_id`),
              KEY `idx_separation` (`separation`)
            ) ENGINE=MyISAM AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
        """ % locals()
        try:
            dms.execute_mysql_write_query(
                sqlQuery=sqlQuery,
                dbConn=self.transientsDbConn,
                log=self.log
            )
        except:
            pass

        self.log.info(
            'completed the ``_create_crossmatch_table_if_not_existing`` method')
        return None

    # use the tab-trigger below for new method
    def _remove_previous_ned_queries(
            self,
            coordinateList):
        """
        *remove previous ned queries*

        **Key Arguments:**
            # - ``coordinateList`` -- set of coordinate to check for previous queries

        **Return:**
            - ``updatedCoordinateList`` -- coordinate list with previous queries removed
        """
        self.log.info('starting the ``_remove_previous_ned_queries`` method')

        # IMPORTS
        import htmCircle
        import math
        from dryxPython import astrotools as dat
        from datetime import datetime, date, time, timedelta

        # 1 DEGREE QUERY RADIUS
        radius = 60. * 60.
        updatedCoordinateList = []

        # FOR EACH TRANSIENT IN COORDINATE LIST
        for c in coordinateList:
            this = c.split(" ")
            raDeg = float(this[0])
            decDeg = float(this[1])

            # BUILD WHERE SECTION OF CLAUSE
            htmWhereClause = htmCircle.htmCircleRegion(
                16, raDeg, decDeg, float(radius))

            # CONVERT RA AND DEC TO CARTESIAN COORDINATES
            ra = math.radians(raDeg)
            dec = math.radians(decDeg)
            cos_dec = math.cos(dec)
            cx = math.cos(ra) * cos_dec
            cy = math.sin(ra) * cos_dec
            cz = math.sin(dec)
            cartesians = (cx, cy, cz)

            # CREATE CARTESIAN SECTION OF QUERY
            cartesianClause = 'and (cx * %.17f + cy * %.17f + cz * %.17f >= cos(%.17f))' % (
                cartesians[0], cartesians[1], cartesians[2], math.radians(radius / 3600.0))

            # CALCULATE THE OLDEST RESULTS LIMIT
            now = datetime.now()
            td = timedelta(
                days=self.settings["ned stream refresh rate in days"])
            refreshLimit = now - td
            refreshLimit = refreshLimit.strftime("%Y-%m-%d %H:%M:%S")

            # FINALLY BUILD THE FULL QUERY AND HIT DATABASE
            sqlQuery = "select * from tcs_helper_ned_query_history %(htmWhereClause)s %(cartesianClause)s and dateQueried > '%(refreshLimit)s'" % locals(
            )
            rows = dms.execute_mysql_read_query(
                sqlQuery=sqlQuery,
                dbConn=self.cataloguesDbConn,
                log=self.log
            )

            # DETERMINE WHICH COORDINATES REQUIRE A NED QUERY
            match = False
            for row in rows:
                raStream = row["raDeg"]
                decStream = row["decDeg"]
                radiusStream = row["arcsecRadius"]
                dateStream = row["dateQueried"]
                angularSeparation, northSep, eastSep = dat.get_angular_separation(
                    log=self.log,
                    ra1=raDeg,
                    dec1=decDeg,
                    ra2=raStream,
                    dec2=decStream
                )
                if angularSeparation + self.settings["first pass ned search radius arcec"] < radiusStream:
                    match = True

            if match == False:
                updatedCoordinateList.append(c)

        self.log.info('completed the ``_remove_previous_ned_queries`` method')
        return updatedCoordinateList

    # use the tab-trigger below for new method
    # xt-class-method


if __name__ == '__main__':
    main()
