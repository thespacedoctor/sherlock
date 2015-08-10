#!/usr/local/bin/python
# encoding: utf-8
"""
_conesearcher.py
================
:Summary:
    The conesearch object for sherlock

:Author:
    David Young

:Date Created:
    July 1, 2015

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
import math
from docopt import docopt
import htmCircle
from dryxPython import logs as dl
from dryxPython import astrotools as dat
from dryxPython import mysql as dms
from dryxPython import commonutils as dcu
from dryxPython.projectsetup import setup_main_clutil


class conesearcher():

    """
    The worker class for the conesearcher module

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        - ``ra`` -- ra of location to search
        - ``dec`` -- dec of location to search
        - ``tableName`` -- the name of the database table to perform conesearch on
        - ``radius`` -- radius of the conesearch to perform (arcsec)
        - ``settings`` -- the settings dictionary
        - ``queryType`` -- queryType ["quick" | "full" | "count"]
        - ``htmLevel`` -- htmLevel [16 | 21]
    """
    # Initialisation

    def __init__(
            self,
            log,
            ra,
            dec,
            tableName,
            radius,
            dbConn=False,
            settings=False,
            htmLevel=16,
            queryType="quick"
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

        # GRAB THE NAMES OF THE IMPORTANT COLUMNS FOR SETTINGS FILE
        if self.tableName in settings["CAT_ID_RA_DEC_COLS"]:
            self.quickColumns = settings[
                "CAT_ID_RA_DEC_COLS"][self.tableName][0]
        else:
            log.error(
                'the table `%(tableName)s` is not recognised listed in the settings file' % locals())

        return None

    # METHOD ATTRIBUTES
    def close(self):
        del self
        return None

    def get(self):
        """get the conesearcher object

        **Return:**
            - ``message`` -- message of success/failure
            - ``results`` -- the results for the conesearch
        """
        self.log.info('starting the ``get`` method')

        self._build_sql_query_from_htm()
        self._grab_conesearch_results_from_db()

        self.log.info('completed the ``get`` method')
        return self.message, self.results

    def _build_sql_query_from_htm(
            self):
        """ build sql query from htm
        """
        self.log.info('starting the ``_build_sql_query_from_htm`` method')

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
            cartesians[0], cartesians[1], cartesians[2], math.radians(self.radius/3600.0))

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

        self.log.info('completed the ``_build_sql_query_from_htm`` method')
        return None

    def _grab_conesearch_results_from_db(
            self):
        """ grab conesearch results from db
        """
        self.log.info(
            'starting the ``_grab_conesearch_results_from_db`` method')

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

            theseCols = self.settings["CAT_ID_RA_DEC_COLS"][self.tableName]
            # CALCULATE THE ANGULAR SEPARATION FOR EACH ROW
            for row in rows:
                if self.tableName == 'tcs_guide_star_cat' or self.tableName == 'tcs_cat_v_guide_star_ps':
                    # Guide star cat RA and DEC are in RADIANS
                    ra2 = math.degrees(row[theseCols[0][1]])
                    dec2 = math.degrees(row[theseCols[0][2]])
                else:
                    ra2 = row[theseCols[0][1]]
                    dec2 = row[theseCols[0][2]]
                separation, northSep, eastSep = dat.get_angular_separation(
                    log=self.log,
                    ra1=self.ra,
                    dec1=self.dec,
                    ra2=ra2,
                    dec2=dec2
                )
                self.results.append([separation, row])

            # SORT BY SEPARATION
            self.results.sort
        else:
            tableName = self.tableName
            self.message = "No matches from %(tableName)s." % locals()

        self.log.info(
            'completed the ``_grab_conesearch_results_from_db`` method')
        return None

    # use the tab-trigger below for new method
    # xt-class-method
