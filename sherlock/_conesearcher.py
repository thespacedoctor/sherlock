#!/usr/local/bin/python
# encoding: utf-8
"""
*The conesearch object for sherlock*

:Author:
    David Young

:Date Created:
    July 1, 2015
"""
################# GLOBAL IMPORTS ####################
import sys
import os
os.environ['TERM'] = 'vt100'
import readline
import glob
import pickle
import math
from docopt import docopt
import htmCircle
from dryxPython import logs as dl
from dryxPython import astrotools as dat
from dryxPython import mysql as dms
from dryxPython import commonutils as dcu
from fundamentals import tools, times


class conesearcher():

    """
    *The worker class for the conesearcher module*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        - ``ra`` -- ra of location to search
        - ``dec`` -- dec of location to search
        - ``tableName`` -- the name of the database table to perform conesearch on
        - ``radius`` -- radius of the conesearch to perform (arcsec)
        - ``colMaps`` -- maps of the important column names for each table/view in the crossmatch-catalogues database
        - ``settings`` -- the settings dictionary
        - ``queryType`` -- queryType ["quick" | "full" | "count"]
        - ``htmLevel`` -- htmLevel [16 | 21]
        - ``nearestOnly`` -- return only the nearest object if true
        - ``physicalSearch`` -- is this a physical search (False for angular search only)
        - ``transType`` -- type of transient if match is found
    """
    # Initialisation

    def __init__(
            self,
            log,
            ra,
            dec,
            tableName,
            radius,
            colMaps,
            dbConn=False,
            settings=False,
            htmLevel=16,
            queryType="quick",
            nearestOnly=False,
            physicalSearch=False,
            transType=False
    ):
        self.log = log
        log.debug("instansiating a new '_conesearcher' object")
        self.dbConn = dbConn
        self.settings = settings
        self.ra = ra
        self.dec = dec
        self.htmLevel = htmLevel
        self.queryType = queryType
        self.radius = radius
        self.tableName = tableName
        self.nearestOnly = nearestOnly
        self.colMaps = colMaps
        self.physicalSearch = physicalSearch
        self.transType = transType

        # xt-self-arg-tmpx

        # VARIABLE DATA ATRRIBUTES
        self.message = ""

        # INITIAL ACTIONS
        # FLIP QUERYTYPE TO INT
        for l, n in zip(["quick", "full", "count"], [1, 2, 3]):
            if self.queryType == l:
                self.queryType = n

        # CHECK HTM LEVELS
        if htmLevel not in (16, 20):
            self.log.error('Must be HTM level 16 or 20' % locals())
            return "Must be HTM level 16 or 20", []

        # CONVERT RA AND DEC TO DEGREES
        try:
            self.ra = dat.ra_sexegesimal_to_decimal.ra_sexegesimal_to_decimal(
                self.ra)
        except:
            pass
        try:
            self.dec = dat.declination_sexegesimal_to_decimal.declination_sexegesimal_to_decimal(
                self.dec)
        except:
            pass

        return None

    # METHOD ATTRIBUTES
    def get(self):
        """
        *get the conesearcher object*

        **Return:**
            - ``message`` -- message of success/failure
            - ``results`` -- the results for the conesearch
        """
        self.log.debug('starting the ``get`` method')

        self._build_sql_query_from_htm()
        self._grab_conesearch_results_from_db()

        self.log.debug('completed the ``get`` method')
        return self.message, self.results

    def _build_sql_query_from_htm(
            self):
        """
        *build sql query from htm*
        """
        self.log.debug('starting the ``_build_sql_query_from_htm`` method')

        # BUILD WHERE SECTION OF CLAUSE
        self.radius = float(self.radius)
        htmWhereClause = htmCircle.htmCircleRegion(
            self.htmLevel, self.ra, self.dec, self.radius)

        # CONVERT RA AND DEC TO CARTESIAN COORDINATES
        ra = math.radians(self.ra)
        dec = math.radians(self.dec)
        cos_dec = math.cos(dec)
        cx = math.cos(ra) * cos_dec
        cy = math.sin(ra) * cos_dec
        cz = math.sin(dec)
        cartesians = (cx, cy, cz)

        # CREATE CARTESIAN SECTION OF QUERY
        cartesianClause = 'and (cx * %.17f + cy * %.17f + cz * %.17f >= cos(%.17f))' % (
            cartesians[0], cartesians[1], cartesians[2], math.radians(self.radius / 3600.0))

        # DECIDE WHAT COLUMNS TO REQUEST
        if self.queryType == 1:
            columns = self.quickColumns
        elif self.queryType == 3:
            columns = ['count(*) number']
        else:
            columns = ['*']

        columns = ','.join(columns)
        tableName = self.tableName

        # FINALLY BUILD THE FULL QUERY
        self.sqlQuery = "select %(columns)s from %(tableName)s %(htmWhereClause)s %(cartesianClause)s" % locals(
        )

        self.log.debug('completed the ``_build_sql_query_from_htm`` method')
        return None

    def _grab_conesearch_results_from_db(
            self):
        """
        *grab conesearch results from db*
        """
        self.log.debug(
            'starting the ``_grab_conesearch_results_from_db`` method')

        # ACCOUNT FOR TYPE OF SEARCH
        if self.physicalSearch == False and self.transType == "SN":
            where = ""
            if self.colMaps[self.tableName]["redshiftColName"]:
                where += " and %s is null" % (
                    self.colMaps[self.tableName]["redshiftColName"],)
            if self.colMaps[self.tableName]["distanceColName"]:
                where += " and %s is null" % (
                    self.colMaps[self.tableName]["distanceColName"],)
            if self.colMaps[self.tableName]["semiMajorColName"]:
                where += " and %s is null" % (
                    self.colMaps[self.tableName]["semiMajorColName"],)
            self.sqlQuery += where
        elif self.physicalSearch == True:
            where = ""
            if self.colMaps[self.tableName]["redshiftColName"]:
                where += " or %s is not null" % (
                    self.colMaps[self.tableName]["redshiftColName"],)
            if self.colMaps[self.tableName]["distanceColName"]:
                where += " or %s is not null" % (
                    self.colMaps[self.tableName]["distanceColName"],)
            if self.colMaps[self.tableName]["semiMajorColName"]:
                where += " or %s is not null" % (
                    self.colMaps[self.tableName]["semiMajorColName"],)
            if len(where):
                where = " and (" + where[4:] + ")"
                self.sqlQuery += where

        self.results = []
        rows = dms.execute_mysql_read_query(
            sqlQuery=self.sqlQuery,
            dbConn=self.dbConn,
            log=self.log
        )

        if len(rows):
            # IF ONLY A COUNT(*)
            if self.queryType == 3:
                self.results = [[0.0, rows[0]['number']]]
                return "Count", self.results

            # CALCULATE THE ANGULAR SEPARATION FOR EACH ROW
            for row in rows:
                if "guide_star" in self.tableName:
                    # Guide star cat RA and DEC are in RADIANS
                    ra2 = math.degrees(
                        row[self.colMaps[self.tableName]["raColName"]])
                    dec2 = math.degrees(
                        row[self.colMaps[self.tableName]["decColName"]])
                else:
                    ra2 = row[self.colMaps[self.tableName]["raColName"]]
                    dec2 = row[self.colMaps[self.tableName]["decColName"]]

                separation, northSep, eastSep = dat.get_angular_separation(
                    log=self.log,
                    ra1=self.ra,
                    dec1=self.dec,
                    ra2=ra2,
                    dec2=dec2
                )
                self.results.append([separation, row])

            # SORT BY SEPARATION
            from operator import itemgetter
            self.results = sorted(self.results, key=itemgetter(0))

            # IF NEAREST ONLY REQUESTED
            if self.nearestOnly == True:
                self.results = [self.results[0]]
        else:
            tableName = self.tableName
            self.message = "No matches from %(tableName)s." % locals()

        self.log.debug(
            'completed the ``_grab_conesearch_results_from_db`` method')
        return None

    # use the tab-trigger below for new method
    # xt-class-method
