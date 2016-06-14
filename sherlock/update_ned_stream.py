#!/usr/local/bin/python
# encoding: utf-8
"""
update_ned_stream.py
==================
:Summary:
    Update the NED stream by querying NED with an area larger than required for the transients listed

:Author:
    David Young

:Date Created:
    November  2, 2015

:dryx syntax:
    - ``_someObject`` = a 'private' object that should only be changed for debugging

:Notes:
    - If you have any questions requiring this script/module please email me: davidrobertyoung@gmail.com

:Tasks:
    @review: when complete pull all general functions and classes into dryxPython
"""
################# GLOBAL IMPORTS ####################
import sys
import os
os.environ['TERM'] = 'vt100'
from dryxPython import logs as dl
from dryxPython import commonutils as dcu
from dryxPython import mysql as dms
from sherlock.imports.ned_conesearch import ned_conesearch
from HMpTy import htm
import numpy as np
from cStringIO import StringIO


class update_ned_stream():
    """
    The worker class for the update_ned_stream module

    **Key Arguments:**
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary
        - ``transientsMetadataList`` -- the metadata for the transients (list of dictionaries, must contain keys 'ra' and 'dec')
        - ``cataloguesDbConn`` -- database connection for the catalogue database

    **Todo**
        - @review: when complete, clean update_ned_stream class
        - @review: when complete add logging
        - @review: when complete, decide whether to abstract class to another module
    """
    # Initialisation
    # 1. @flagged: what are the unique attrributes for each object? Add them
    # to __init__

    def __init__(
        self,
        log,
        cataloguesDbConn,
        settings=False,
        transientsMetadataList=[]
    ):
        self.log = log
        log.debug("instansiating a new 'update_ned_stream' object")
        self.settings = settings
        self.transientsMetadataList = transientsMetadataList
        self.cataloguesDbConn = cataloguesDbConn

        # xt-self-arg-tmpx

        # 2. @flagged: what are the default attrributes each object could have? Add them to variable attribute set here
        # Variable Data Atrributes

        # 3. @flagged: what variable attrributes need overriden in any baseclass(es) used
        # Override Variable Data Atrributes

        # Initial Actions

        return None

    # 4. @flagged: what actions does each object have to be able to perform? Add them here
    # Method Attributes
    def get(self):
        """get the update_ned_stream object

        **Return:**
            - ``update_ned_stream``

        **Todo**
            - @review: when complete, clean get method
            - @review: when complete add logging
        """
        self.log.info('starting the ``get`` method')

        coordinateList = []
        for i in self.transientsMetadataList:
            thisList = str(i["ra"]) + " " + str(i["dec"])
            coordinateList.append(thisList)

        initialListLen = len(coordinateList)
        coordinateList = self._remove_previous_ned_queries(
            coordinateList=coordinateList
        )
        newListLen = len(coordinateList)
        previousSearches = initialListLen - newListLen

        print "%(newListLen)s/%(initialListLen)s coordinate sets need an inital NED conesearch to find matched source names (%(previousSearches)s coordinate sets found in search history)" % locals()

        this = ned_conesearch(
            log=self.log,
            settings=self.settings,
            coordinateList=coordinateList,
            cataloguesDbConn=self.cataloguesDbConn
        )
        this.get()

        self.log.info('completed the ``get`` method')
        return update_ned_stream

    def _remove_previous_ned_queries(
            self,
            coordinateList):
        """ remove previous ned queries

        **Key Arguments:**
            # - ``coordinateList`` -- set of coordinate to check for previous queries

        **Return:**
            - ``updatedCoordinateList`` -- coordinate list with previous queries removed
        """
        self.log.info('starting the ``_remove_previous_ned_queries`` method')

        # IMPORTS
        import math
        from dryxPython import astrotools as dat
        from datetime import datetime, date, time, timedelta

        print "Removing transient/subdisk areas that have had previous searches performed"

        # VARAIBLES
        self.mesh16 = htm.HTM(16)
        updatedCoordinateList = []

        #  FIND THE LARGE NED QUERY DONE SO FAR FROM DATABASE
        sqlQuery = u"""
            select arcsecRadius from tcs_helper_ned_query_history order by arcsecRadius desc limit 1
        """ % locals()
        rows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )
        largestNedQueryDeg = rows[0]["arcsecRadius"] / 3600.

        # CALCULATE THE OLDEST RESULTS LIMIT
        now = datetime.now()
        td = timedelta(
            days=self.settings["ned stream refresh rate in days"])
        refreshLimit = now - td
        refreshLimit = refreshLimit.strftime("%Y-%m-%d %H:%M:%S")

        # FOR EACH TRANSIENT IN COORDINATE LIST ...
        totalCount = len(coordinateList)
        count = 0
        htmWhereClauses = []
        transRas = []
        transDecs = []
        for c in coordinateList:
            count += 1
            if count > 1:
                # Cursor up one line and clear line
                sys.stdout.write("\x1b[1A\x1b[2K")
            if count > totalCount:
                count = totalCount
            percent = (float(count) / float(totalCount)) * 100.
            print "  %(count)s / %(totalCount)s (%(percent)1.1f%%) transients/subdisks checked" % locals()
            this = c.split(" ")
            raDeg = float(this[0])
            decDeg = float(this[1])
            transRas.append(raDeg)
            transDecs.append(decDeg)

            # BUILD WHERE SECTION OF CLAUSE
            # CREATE AN ARRAY OF RELEVANT HTMIDS AND FIND MAX AND MIN
            thisArray = self.mesh16.intersect(
                raDeg, decDeg, largestNedQueryDeg, inclusive=True)
            hmax = thisArray.max()
            hmin = thisArray.min()
            htmWhereClause = "(htm16ID between %(hmin)s and %(hmax)s)" % locals(
            )
            htmWhereClauses.append(htmWhereClause)

        htmWhereClauses = " or ".join(htmWhereClauses)

        # DO A LARGE FIRST SWEEP FOR PREVIOUS SEARCHES - BEFORE PINPOINTING
        if not len(htmWhereClauses):
            return []
        sqlQuery = "select * from tcs_helper_ned_query_history where (%(htmWhereClauses)s) and dateQueried > '%(refreshLimit)s'" % locals(
        )
        rows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )

        nedRas = []
        nedDecs = []
        nedRadii = []
        for row in rows:
            nedRas.append(row["raDeg"])
            nedDecs.append(row["decDeg"])
            nedRadii.append(row["arcsecRadius"])

        # CREATE TWO ARRAYS OF RA,DEC (1. SUBDISKS & 2. NED PREVIOUS QUERY
        # RETURNS)
        tRa = np.array(transRas)
        tDec = np.array(transDecs)
        raList = np.array(nedRas)
        decList = np.array(nedDecs)

        indexList1, indexList2, separation = self.mesh16.match(
            tRa, tDec, raList, decList, largestNedQueryDeg, maxmatch=0)

        matchList = [False] * len(transRas)
        for i in xrange(indexList1.size):
            if separation[i] * 3600. + self.settings["first pass ned search radius arcec"] < nedRadii[indexList2[i]]:
                matchList[indexList1[i]] = True

        for m, c in zip(matchList, coordinateList):
            if m == False:
                updatedCoordinateList.append(c)

        self.log.info('completed the ``_remove_previous_ned_queries`` method')
        return updatedCoordinateList

    # xt-class-method

    # 5. @flagged: what actions of the base class(es) need ammending? ammend them here
    # Override Method Attributes
    # method-override-tmpx
