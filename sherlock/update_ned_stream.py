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

        coordinateList = self._remove_previous_ned_queries(
            coordinateList=coordinateList
        )

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

    # xt-class-method

    # 5. @flagged: what actions of the base class(es) need ammending? ammend them here
    # Override Method Attributes
    # method-override-tmpx
