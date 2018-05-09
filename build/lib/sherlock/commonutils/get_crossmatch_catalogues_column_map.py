#!/usr/local/bin/python
# encoding: utf-8
"""
*query the crossmatch catalogues helper tables to generate a map of the important columns of each catalogue*

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
from fundamentals.mysql import readquery


def get_crossmatch_catalogues_column_map(
        dbConn,
        log):
    """*query the crossmatch catalogues helper tables to generate a map of the important columns of each catalogue*

    Note the work must be done to manually map the inhomogeneous column-names from the crossmatch catalogues to an internal homogeneous name-set which includes ra, dec, redshift, object name etc.
    The maps are set in the two helper tables called `tcs_helper_catalogue_views_info` and `tcs_helper_catalogue_views_info`. 

    **Key Arguments:**
        - ``dbConn`` -- the crossatch catalogues database connection
        - ``log`` -- logger

    **Return:**
        - ``colMaps`` -- dictionary of dictionaries {view_name: {columnMap}}

    **Usage:**

        To collect the column map dictionary of dictionaries from the catalogues database, use the ``get_crossmatch_catalogues_column_map`` function:

        .. code-block:: python 

            from sherlock.commonutils import get_crossmatch_catalogues_column_map
            colMaps = get_crossmatch_catalogues_column_map(
                log=log,
                dbConn=cataloguesDbConn
            )
    """
    log.info('starting the ``get_crossmatch_catalogues_column_map`` function')

    # GRAB THE NAMES OF THE IMPORTANT COLUMNS FROM DATABASE
    sqlQuery = u"""
        SELECT 
            *
        FROM
            tcs_helper_catalogue_views_info v,
            tcs_helper_catalogue_tables_info t
        WHERE
            v.table_id = t.id
    """ % locals()
    rows = readquery(
        log=log,
        sqlQuery=sqlQuery,
        dbConn=dbConn,
        quiet=False
    )
    colMaps = {}
    for row in rows:
        colMaps[row["view_name"]] = row

    log.info('completed the ``get_crossmatch_catalogues_column_map`` function')
    return colMaps
