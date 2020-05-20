#!/usr/local/bin/python
# encoding: utf-8
"""
*Query the sherlock-catalogues helper tables to generate a map of the important columns of each catalogue*

:Author:
    David Young
"""
import sys
import os
os.environ['TERM'] = 'vt100'
from fundamentals import tools
from fundamentals.mysql import readquery

def get_crossmatch_catalogues_column_map(
        dbConn,
        log):
    """*Query the sherlock-catalogues helper tables to generate a map of the important columns of each catalogue*

    Within your sherlock-catalogues database you need to manually map the inhomogeneous column-names from the sherlock-catalogues to an internal homogeneous name-set which includes *ra*, *dec*, *redshift*, *object name*, *magnitude*, *filter* etc.
    The column-name map is set within the two database helper tables called `tcs_helper_catalogue_views_info` and `tcs_helper_catalogue_views_info`. See the *'Checklist for Adding A New Reference Catalogue to the Sherlock Catalogues Database'* for more information.

    .. todo::

        - write a checklist for adding a new catalogue to the sherlock database and reference it from here (use the image below of the tcs_helper_catalogue_views_info table)

    .. image:: https://farm5.staticflickr.com/4604/38429536400_eafa991580_o.png
        :width: 200 px

    **Key Arguments**

    - ``dbConn`` -- the sherlock-catalogues database connection
    - ``log`` -- logger
    

    **Return**

    - ``colMaps`` -- dictionary of dictionaries with the name of the database-view (e.g. `tcs_view_agn_milliquas_v4_5`) as the key and the column-name dictary map as value (`{view_name: {columnMap}}`).
    

    **Usage**

    To collect the column map dictionary of dictionaries from the catalogues database, use the ``get_crossmatch_catalogues_column_map`` function:

    ```python
    from sherlock.commonutils import get_crossmatch_catalogues_column_map
    colMaps = get_crossmatch_catalogues_column_map(
        log=log,
        dbConn=cataloguesDbConn
    )
    ```
    
    """
    log.debug('starting the ``get_crossmatch_catalogues_column_map`` function')

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

    log.debug('completed the ``get_crossmatch_catalogues_column_map`` function')
    return colMaps
