#!/usr/local/bin/python
# encoding: utf-8
"""
*perform a conesearch on a **single** sherlock crossmatch catalogue table*

:Author:
    David Young

:noindex:
"""
import copy
from HMpTy.mysql import conesearch as hmptyConesearch
from astrocalc.coords import unit_conversion
from fundamentals import tools
from builtins import zip
from builtins import object
import sys
import os
os.environ['TERM'] = 'vt100'


class catalogue_conesearch(object):
    """
    *The worker class for the conesearch module*

    **Key Arguments**

    - ``dbConn`` -- mysql database connection to the catalogues database
    - ``log`` -- logger
    - ``ra`` -- ra of transient location (sexagesimal or decimal degrees, J2000, single location or list of locations)
    - ``dec`` -- dec of transient location (sexagesimal or decimal degrees, J2000, single location or list of locations)
    - ``tableName`` -- the name of the database table to perform the conesearch on
    - ``radius`` -- radius of the conesearch to perform (arcsec)
    - ``colMaps`` -- maps of the important column names for each table/view in the crossmatch-catalogues database
    - ``nearestOnly`` -- return only the nearest object. Default *False*
    - ``physicalSearch`` -- is this a physical search, so only return matches with distance information. Default *False*
    - ``upperMagnitudeLimit`` -- the upper magnitude limit if a magnitude cut is required with the conesearch. Default *False*
    - ``lowerMagnitudeLimit`` -- the lower magnitude limit if a magnitude cut is required with the conesearch. Default *False*
    - ``magnitudeLimitFilter`` -- the filter to use for the magnitude limit if required. Default *False*, ("_u"|"_g"|"_r"|"_i"|"_z"|"_y"|"U"|"B"|"V"|"R"|"I"|"Z"|"J"|"H"|"K"|"G")
    - ``semiMajorAxisOperator`` -- the semi-major axis operator in use.

    **Usage**

    To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_). 

    .. todo::

        - update the package tutorial if needed

    The following examples assume you've connected to the various databases and generated the catalogue column maps in the following manner:

    ```python
    # SETUP ALL DATABASE CONNECTIONS
    from sherlock import database
    db = database(
        log=log,
        settings=settings
    )
    dbConns, dbVersions = db.connect()
    transientsDbConn = dbConns["transients"]
    cataloguesDbConn = dbConns["catalogues"]

    # GET THE COLUMN MAPS FROM THE CATALOGUE DATABASE
    from sherlock.commonutils import get_crossmatch_catalogues_column_map
    colMaps = get_crossmatch_catalogues_column_map(
        log=log,
        dbConn=cataloguesDbConn
    )
    ```

    To perform a single location conesearch on a catalogue in the database, for example the milliquas AGN catalogue:

    ```python
    from sherlock import catalogue_conesearch
    cs = catalogue_conesearch(
        log=log,
        ra="23:01:07.99",
        dec="-01:58:04.5",
        radiusArcsec=60.,
        colMaps=colMaps,
        tableName="tcs_view_agn_milliquas_v4_5",
        dbConn=cataloguesDbConn,
        nearestOnly=False,
        physicalSearch=False
    )
    # catalogueMatches ARE ORDERED BY ANGULAR SEPARATION
    indices, catalogueMatches = cs.search()

    print(catalogueMatches)
    ```

    The output of this search is:

    ```text
    [{'R': 20.1, 'cmSepArcsec': 0.28015184686564643, 'ra': 345.2832267, 'catalogue_object_subtype': u'QR', 'z': 0.777, 'dec': -1.9679629, 'catalogue_object_id': u'PKS 2258-022'}]        
    ```

    Note ``catalogue_conesearch`` can accept coordinates in sexagesimal or decimal degrees (J200). It can also accept lists of coordinates:

    ```python
    from sherlock import catalogue_conesearch
    cs = catalogue_conesearch(
        log=log,
        ra=["23:01:07.99", 45.36722, 13.875250],
        dec=["-01:58:04.5", 30.45671, -25.26721],
        radiusArcsec=60.,


            colMaps=colMaps,
            tableName="tcs_view_agn_milliquas_v4_5",
            dbConn=cataloguesDbConn,
            nearestOnly=False,
            physicalSearch=False
        )
        ```

        When passing a list of transient coordinates the returned ``indices`` value becomes important as this list identifies which transient is matched with which crossmatch results (and possibly multiple crossmatch results)

        ```python
        indices, catalogueMatches = cs.search()
        for i, c in zip(indices, catalogueMatches):
            print(i, c)
        ```

        The output of this search is:

        ```text
        0 {'R': 20.1, 'cmSepArcsec': 0.28015184686564643, 'ra': 345.2832267, 'catalogue_object_subtype': u'QR', 'z': 0.777, 'dec': -1.9679629, 'catalogue_object_id': u'PKS 2258-022'}
        2 {'R': 19.2, 'cmSepArcsec': 0.81509715903447644, 'ra': 13.875, 'catalogue_object_subtype': u'Q', 'z': 2.7, 'dec': -25.2672223, 'catalogue_object_id': u'Q 0053-2532'}
        ```

    .. todo ::

        - update key arguments values and definitions with defaults
        - update return values and definitions
        - update usage examples and text
        - update docstring text
        - check sublime snippet exists
        - clip any useful text to docs mindmap
        - regenerate the docs and check redendering of this docstring
    """
    # Initialisation

    def __init__(
            self,
            log,
            ra,
            dec,
            tableName,
            radiusArcsec,
            colMaps,
            dbConn=False,
            nearestOnly=False,
            physicalSearch=False,
            upperMagnitudeLimit=False,
            lowerMagnitudeLimit=False,
            magnitudeLimitFilter=False,
            semiMajorAxisOperator=False
    ):
        self.log = log
        log.debug("instansiating a new 'conesearcher' object")
        self.dbConn = dbConn
        self.radius = radiusArcsec
        self.tableName = tableName
        self.nearestOnly = nearestOnly
        self.colMaps = colMaps
        self.physicalSearch = physicalSearch
        self.upperMagnitudeLimit = upperMagnitudeLimit
        self.lowerMagnitudeLimit = lowerMagnitudeLimit
        self.magnitudeLimitFilter = magnitudeLimitFilter
        self.semiMajorAxisOperator = semiMajorAxisOperator
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

        try:
            float(ra[0])
            convert = False
        except:
            convert = True

        if convert == True:

            for r, d in zip(ra, dec):
                self.ra.append(converter.ra_sexegesimal_to_decimal(
                    ra=r
                ))
                self.dec.append(converter.dec_sexegesimal_to_decimal(
                    dec=d
                ))
        else:
            self.ra = ra
            self.dec = dec

        return None

    def search(self):
        """
        *trigger the conesearch*

        **Return**

        - ``matchIndices`` -- the indices of the input transient sources (syncs with ``uniqueMatchDicts``)
        - ``uniqueMatchDicts`` -- the crossmatch results


        **Usage**

        See class docstring for usage examples


        .. todo ::

            - update key arguments values and definitions with defaults
            - update return values and definitions
            - update usage examples and text
            - update docstring text
            - check sublime snippet exists
            - clip any useful text to docs mindmap
            - regenerate the docs and check rerendering of this docstring
        """
        self.log.debug('starting the ``search`` method')

        # ACCOUNT FOR TYPE OF SEARCH
        sqlWhere = False
        magnitudeLimitFilter = self.magnitudeLimitFilter
        disCols = ["zColName",
                   "distanceColName"]
        if "_big_" not in self.tableName.lower() and self.semiMajorAxisOperator:
            disCols.append("semiMajorColName")
        sqlWhere = ""

        if self.physicalSearch == True:
            for d in disCols:
                colName = self.colMaps[self.tableName][d]
                if colName:
                    sqlWhere += " or `%(colName)s` is not null" % locals()
                if len(sqlWhere):
                    sqlWhere = " and (" + sqlWhere[4:] + ")"

        if self.upperMagnitudeLimit != False and self.upperMagnitudeLimit and not self.lowerMagnitudeLimit:
            mag = self.upperMagnitudeLimit
            sqlWhere += " and `%(magnitudeLimitFilter)s` > %(mag)s" % locals()
        if self.lowerMagnitudeLimit != False and self.lowerMagnitudeLimit and not self.upperMagnitudeLimit:
            mag = self.lowerMagnitudeLimit
            sqlWhere += " and `%(magnitudeLimitFilter)s` < %(mag)s" % locals()
        # THE GENERAL (INBETWEEN) CASE
        if self.lowerMagnitudeLimit != False and self.lowerMagnitudeLimit and self.upperMagnitudeLimit != False and self.upperMagnitudeLimit:
            upperMagnitudeLimit = self.upperMagnitudeLimit
            lowerMagnitudeLimit = self.lowerMagnitudeLimit
            sqlWhere += " and ((`%(magnitudeLimitFilter)s` > %(upperMagnitudeLimit)s and `%(magnitudeLimitFilter)s` < %(lowerMagnitudeLimit)s))" % locals()

        if sqlWhere and " and" == sqlWhere[0:4]:
            sqlWhere = sqlWhere[5:]

        # THE COLUMN MAP LIFTED FROM ``tcs_helper_catalogue_tables_info` TABLE
        # IN CATALOGUE DATABASE (COLUMN NAMES ENDDING WITH 'ColName')
        columns = {}
        for k, v in list(self.colMaps[self.tableName].items()):
            name = k.replace("ColName", "")
            if "colname" in k.lower() and v:
                columns[k] = "`%(v)s` as `%(name)s`" % locals()
        columns = ", ".join(list(columns.values()))

        htmColumns = ["htm10ID", "htm13ID", "htm16ID"]
        if "ned_d_" in self.tableName.lower():
            htmColumns = ["htm07ID", "htm10ID", "htm13ID", "htm16ID"]

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
            htmColumns=htmColumns
        )
        matchIndices, matches = cs.search()

        # MATCH ARE NOT NECESSARILY UNIQUE IF MANY TRANSIENT MATCH ONE SOURCE
        uniqueMatchDicts = []
        uniqueMatchDicts[:] = [copy.copy(d) for d in matches.list]

        self.log.debug('completed the ``search`` method')
        return matchIndices, uniqueMatchDicts

    # xt-class-method
