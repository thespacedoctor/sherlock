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
import copy


class catalogue_conesearch():
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
        if not isinstance(ra, list):
            ra = [ra]
            dec = [dec]

        self.ra = []
        self.dec = []

        converter = unit_conversion(
            log=self.log
        )
        for r, d in zip(ra, dec):
            self.ra.append(converter.ra_sexegesimal_to_decimal(
                ra=r
            ))
            self.dec.append(converter.dec_sexegesimal_to_decimal(
                dec=d
            ))

        return None

    def search(self):
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
        self.log.info('starting the ``search`` method')

        # ACCOUNT FOR TYPE OF SEARCH
        sqlWhere = False
        disCols = ["zColName",
                   "distanceColName", "semiMajorColName"]
        sqlWhere = ""
        if self.physicalSearch == False and self.transType == "SN":
            for d in disCols:
                colName = self.colMaps[self.tableName][d]
                if colName:
                    sqlWhere += " and `%(colName)s` is null" % locals()

        elif self.physicalSearch == True:
            for d in disCols:
                colName = self.colMaps[self.tableName][d]
                if colName:
                    sqlWhere += " or `%(colName)s` is not null" % locals()
            if len(sqlWhere):
                sqlWhere = " and (" + sqlWhere[4:] + ")"

        if sqlWhere and " and" == sqlWhere[0:4]:
            sqlWhere = sqlWhere[5:]

        # THE COLUMN MAP LIFTED FROM ``tcs_helper_catalogue_tables_info` TABLE
        # IN CATALOGUE DATABASE (COLUMN NAMES ENDDING WITH 'ColName')
        columns = {}
        for k, v in self.colMaps[self.tableName].iteritems():
            name = k.replace("ColName", "")
            if "colname" in k.lower() and v:
                columns[k] = "`%(v)s` as `%(name)s`" % locals()
        columns = ", ".join(columns.values())

        cs = hmptyConesearch(
            log=self.log,
            dbConn=self.dbConn,
            tableName=self.tableName,
            columns=columns,
            ra=self.ra,
            dec=self.dec,
            radiusArcsec=self.radius,
            separations=True,
            distinct=False,
            sqlWhere=sqlWhere,
            closest=self.nearestOnly,
            raCol="ra",
            decCol="dec",
        )
        matchIndies, matches = cs.search()

        # MATCH ARE NOT NECESSARILY UNIQUE IF MANY TRANSIENT MATCH ONE SOURCE
        uniqueMatchDicts = []
        uniqueMatchDicts[:] = [copy.copy(d) for d in matches.list]

        self.log.info('completed the ``search`` method')
        return matchIndies, uniqueMatchDicts

    # xt-class-method
