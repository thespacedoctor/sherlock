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
from HMpTy import htm
from cStringIO import StringIO
import numpy as np
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
        self.mesh16 = htm.HTM(16)

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
        """get the conesearcher object

        **Return:**
            - ``message`` -- message of success/failure
            - ``results`` -- the results for the conesearch
        """
        self.log.debug('starting the ``get`` method')

        self._build_sql_query_from_htm()

        # RETURN RESULTS IN BATCHES TO AVOID MEMORY ISSUES
        resultLen = 5000
        offset = 0
        returnLimit = 5000
        results = []
        while resultLen == 5000:
            resultSet, resultLen = self._grab_conesearch_results_from_db(
                returnLimit=returnLimit,
                offset=offset
            )
            results += resultSet
            offset += returnLimit

        # SORT BY SEPARATION
        from operator import itemgetter
        self.results = sorted(results, key=itemgetter(0))

        self.log.debug('completed the ``get`` method')
        return self.message, self.results

    def _build_sql_query_from_htm(
            self):
        """ build sql query from htm
        """
        self.log.debug('starting the ``_build_sql_query_from_htm`` method')

        # BUILD WHERE SECTION OF CLAUSE
        self.radius = float(self.radius)

        # CREATE AN ARRAY OF RELEVANT HTMIDS AND FIND MAX AND MIN
        thisArray = self.mesh16.intersect(
            self.ra, self.dec, self.radius / 3600., inclusive=True)
        hmax = thisArray.max()
        hmin = thisArray.min()
        ratio = float(hmax - hmin + 1) / float(thisArray.size)
        if ratio < 100 or thisArray.size > 2000:
            htmWhereClause = "where htm16ID between %(hmin)s and %(hmax)s" % locals(
            )
        else:
            s = StringIO()
            np.savetxt(s, thisArray, fmt='%d', newline=",")
            thesHtmIds = s.getvalue()[:-1]
            htmWhereClause = "where htm16ID in (%(thesHtmIds)s)" % locals()

        # hmax = thisArray.max()
        # hmin = thisArray.min()
        #

        # DECIDE WHAT COLUMNS TO REQUEST
        if self.queryType == 1:
            columns = self.quickColumns
        elif self.queryType == 3:
            columns = ['count(*) number']
        else:
            columns = []
            for k, v in self.colMaps[self.tableName].iteritems():
                if "colname" in k.lower() and v:
                    columns.append(v)

        columns = ','.join(columns)
        tableName = self.tableName

        # FINALLY BUILD THE FULL QUERY
        self.sqlQuery = "select %(columns)s from %(tableName)s %(htmWhereClause)s" % locals(
        )

        self.log.debug('completed the ``_build_sql_query_from_htm`` method')
        return None

    def _grab_conesearch_results_from_db(
            self,
            returnLimit=None,
            offset=None):
        """ grab conesearch results from db
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

        # SETUP OFFSETS
        if returnLimit != None and offset != None:
            sqlQueryExtra = " limit %(returnLimit)s offset %(offset)s" % locals(
            )
        else:
            sqlQueryExtra = ""

        results = []
        # print "START DB"
        print self.sqlQuery + sqlQueryExtra
        rows = dms.execute_mysql_read_query(
            sqlQuery=self.sqlQuery + sqlQueryExtra,
            dbConn=self.dbConn,
            log=self.log
        )
        # print "END DB"

        resultLen = len(rows)

        if len(rows):
            # IF ONLY A COUNT(*)
            if self.queryType == 3:
                results = [[0.0, rows[0]['number']]]
                return "Count", results

            # CALCULATE THE ANGULAR SEPARATION FOR EACH ROW
            raList = []
            decList = []
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
                raList.append(ra2)
                decList.append(dec2)

            # CREATE TWO ARRAYS OF RA,DEC (1. TRANSIENT & 2. DATABASE RETURNS)
            tRa = np.array([self.ra])
            tDec = np.array([self.dec])
            raList = np.array(raList)
            decList = np.array(decList)
            indexList1, indexList2, separation = self.mesh16.match(
                tRa, tDec, raList, decList, self.radius / 3600., maxmatch=0)
            for i in xrange(indexList1.size):
                results.append([separation[i] * 3600., rows[i]])

            # IF NEAREST ONLY REQUESTED
            if self.nearestOnly == True:
                results = [results[0]]
        else:
            tableName = self.tableName
            self.message = "No matches from %(tableName)s." % locals()

        self.log.debug(
            'completed the ``_grab_conesearch_results_from_db`` method')
        return results, resultLen

    # use the tab-trigger below for new method
    # xt-class-method
