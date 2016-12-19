#!/usr/local/bin/python
# encoding: utf-8
"""
*perform a conesearch on a **single** sherlock crossmatch catalogue table*

:Author:
    David Young

:Date Created:
    December 16, 2016
"""
################# GLOBAL IMPORTS ####################
import sys
import os
os.environ['TERM'] = 'vt100'
from fundamentals import tools
from astrocalc.coords import unit_conversion
from HMpTy.mysql import conesearch as hmptyConesearch


class conesearch():
    """
    *The worker class for the conesearch module*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        - ``ra`` -- ra of location to search
        - ``dec`` -- dec of location to search
        - ``tableName`` -- the name of the database table to perform conesearch on
        - ``radius`` -- radius of the conesearch to perform (arcsec)
        - ``colMaps`` -- maps of the important column names for each table/view in the crossmatch-catalogues database
        - ``settings`` -- the settings dictionary
        - ``nearestOnly`` -- return only the nearest object if true
        - ``physicalSearch`` -- is this a physical search (False for angular search only)
        - ``transType`` -- type of transient if match is found

    **Usage:**

        To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_). 

        To initiate a conesearch object, use the following:

        .. todo::

            - add usage info
            - create a sublime snippet for usage
            - update the package tutorial if needed

        .. code-block:: python 

            usage code   
    """
    # Initialisation
    # 1. @flagged: what are the unique attrributes for each object? Add them
    # to __init__

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
            nearestOnly=False,
            physicalSearch=False,
            transType=False
    ):
        self.log = log
        log.debug("instansiating a new 'conesearcher' object")
        self.dbConn = dbConn
        self.settings = settings
        self.radius = radius
        self.tableName = tableName
        self.nearestOnly = nearestOnly
        self.colMaps = colMaps
        self.physicalSearch = physicalSearch
        self.transType = transType

        # xt-self-arg-tmpx

        # CONVERT RA AND DEC TO DEGREES
        converter = unit_conversion(
            log=self.log
        )
        self.ra = converter.ra_sexegesimal_to_decimal(
            ra=ra
        )
        self.dec = converter.dec_sexegesimal_to_decimal(
            dec=dec
        )

        return None

    def match(self):
        """
        *match the conesearch object*

        **Return:**
            - ``conesearch``

        **Usage:**
        .. todo::

            - add usage info
            - create a sublime snippet for usage
            - update the package tutorial if needed

        .. code-block:: python 

            usage code 
        """
        self.log.info('starting the ``match`` method')

        # ACCOUNT FOR TYPE OF SEARCH
        sqlWhere = False
        if self.physicalSearch == False and self.transType == "SN":
            sqlWhere = ""
            if self.colMaps[self.tableName]["redshiftColName"]:
                sqlWhere += " and %s is null" % (
                    self.colMaps[self.tableName]["redshiftColName"],)
            if self.colMaps[self.tableName]["distanceColName"]:
                sqlWhere += " and %s is null" % (
                    self.colMaps[self.tableName]["distanceColName"],)
            if self.colMaps[self.tableName]["semiMajorColName"]:
                sqlWhere += " and %s is null" % (
                    self.colMaps[self.tableName]["semiMajorColName"],)

        elif self.physicalSearch == True:
            sqlWhere = ""
            if self.colMaps[self.tableName]["redshiftColName"]:
                sqlWhere += " or %s is not null" % (
                    self.colMaps[self.tableName]["redshiftColName"],)
            if self.colMaps[self.tableName]["distanceColName"]:
                sqlWhere += " or %s is not null" % (
                    self.colMaps[self.tableName]["distanceColName"],)
            if self.colMaps[self.tableName]["semiMajorColName"]:
                sqlWhere += " or %s is not null" % (
                    self.colMaps[self.tableName]["semiMajorColName"],)
            if len(sqlWhere):
                sqlWhere = " and (" + sqlWhere[4:] + ")"

        cs = hmptyConesearch(
            log=self.log,
            dbConn=self.dbConn,
            tableName=self.tableName,
            columns="*",
            ra=self.ra,
            dec=self.dec,
            radiusArcsec=self.radius,
            separations=True,
            distinct=False,
            sqlWhere=sqlWhere,
            closest=self.nearestOnly,
            raCol=self.colMaps[self.tableName]["raColName"],
            decCol=self.colMaps[self.tableName]["decColName"],
        )
        matchIndies, matches = cs.search()

        print matchIndies, matches.list

        self.log.info('completed the ``match`` method')
        return matchIndies, matches

    # xt-class-method

    # 5. @flagged: what actions of the base class(es) need ammending? ammend them here
    # Override Method Attributes
    # method-override-tmpx
