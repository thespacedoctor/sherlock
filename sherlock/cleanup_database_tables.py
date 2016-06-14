#!/usr/local/bin/python
# encoding: utf-8
"""
*Clean up the database tables used by sherlock - maintainance tools*

:Author:
    David Young

:Date Created:
    August 20, 2015

.. todo::
    
    @review: when complete pull all general functions and classes into dryxPython

# xdocopt-usage-tempx
"""
################# GLOBAL IMPORTS ####################
import sys
import os
os.environ['TERM'] = 'vt100'
import readline
import glob
import pickle
from dryxPython import mysql as dms
from docopt import docopt
from dryxPython import logs as dl
from dryxPython import commonutils as dcu
from fundamentals import tools, times
# from ..__init__ import *


def tab_complete(text, state):
    return (glob.glob(text + '*') + [None])[state]


def main(arguments=None):
    """
    *The main function used when ``cleanup_database_tables.py`` is run as a single script from the cl, or when installed as a cl command*
    """
    # setup the command-line util settings
    su = tools(
        arguments=arguments,
        docString=__doc__,
        logLevel="DEBUG",
        options_first=False
    )
    arguments, settings, log, dbConn = su.setup()

    # tab completion for raw_input
    readline.set_completer_delims(' \t\n;')
    readline.parse_and_bind("tab: complete")
    readline.set_completer(tab_complete)

    # unpack remaining cl arguments using `exec` to setup the variable names
    # automatically
    for arg, val in arguments.iteritems():
        if arg[0] == "-":
            varname = arg.replace("-", "") + "Flag"
        else:
            varname = arg.replace("<", "").replace(">", "")
        if isinstance(val, str) or isinstance(val, unicode):
            exec(varname + " = '%s'" % (val,))
        else:
            exec(varname + " = %s" % (val,))
        if arg == "--dbConn":
            dbConn = val
        log.debug('%s = %s' % (varname, val,))

    ## START LOGGING ##
    startTime = times.get_now_sql_datetime()
    log.info(
        '--- STARTING TO RUN THE cleanup_database_tables.py AT %s' %
        (startTime,))

    # set options interactively if user requests
    if "interactiveFlag" in locals() and interactiveFlag:

        # load previous settings
        moduleDirectory = os.path.dirname(__file__) + "/resources"
        pathToPickleFile = "%(moduleDirectory)s/previousSettings.p" % locals()
        try:
            with open(pathToPickleFile):
                pass
            previousSettingsExist = True
        except:
            previousSettingsExist = False
        previousSettings = {}
        if previousSettingsExist:
            previousSettings = pickle.load(open(pathToPickleFile, "rb"))

        # x-raw-input
        # x-boolean-raw-input
        # x-raw-input-with-default-value-from-previous-settings

        # save the most recently used requests
        pickleMeObjects = []
        pickleMe = {}
        theseLocals = locals()
        for k in pickleMeObjects:
            pickleMe[k] = theseLocals[k]
        pickle.dump(pickleMe, open(pathToPickleFile, "wb"))

    # call the worker function
    # x-if-settings-or-database-credientials
    cleanup_database_tables(
        log=log,
        settings=settings
    )

    if "dbConn" in locals() and dbConn:
        dbConn.commit()
        dbConn.close()
    ## FINISH LOGGING ##
    endTime = times.get_now_sql_datetime()
    runningTime = times.calculate_time_difference(startTime, endTime)
    log.info('-- FINISHED ATTEMPT TO RUN THE cleanup_database_tables.py AT %s (RUNTIME: %s) --' %
             (endTime, runningTime, ))

    return


###################################################################
# CLASSES                                                         #
###################################################################
class cleanup_database_tables():

    """
    *The worker class for the cleanup_database_tables module*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary


    .. todo::

        - @review: when complete, clean cleanup_database_tables class
        - @review: when complete add logging
        - @review: when complete, decide whether to abstract class to another module
    """
    # Initialisation
    # 1. @flagged: what are the unique attrributes for each object? Add them
    # to __init__

    def __init__(
            self,
            log,
            settings=False,

    ):
        self.log = log
        log.debug("instansiating a new 'cleanup_database_tables' object")
        self.settings = settings
        # xt-self-arg-tmpx

        # 2. @flagged: what are the default attrributes each object could have? Add them to variable attribute set here
        # Variable Data Atrributes

        # 3. @flagged: what variable attrributes need overriden in any baseclass(es) used
        # Override Variable Data Atrributes

        # Initial Actions
        from sherlock import database
        db = database(
            log=self.log,
            settings=self.settings
        )
        self.transientsDbConn, self.cataloguesDbConn, self.pmDbConn = db.get()

        return None

    def close(self):
        del self
        return None

    # 4. @flagged: what actions does each object have to be able to perform? Add them here
    # Method Attributes
    def get(self):
        """
        *get the cleanup_database_tables object*

        **Return:**
            - ``cleanup_database_tables``

        .. todo::

            - @review: when complete, clean get method
            - @review: when complete add logging
        """
        self.log.info('starting the ``get`` method')

        self._update_tcs_helper_catalogue_tables_info_with_new_tables()
        self._updated_row_counts_in_tcs_helper_catalogue_tables_info()
        self._clean_up_columns()
        self._update_tcs_helper_catalogue_views_info_with_new_views()
        self._copy_helper_tables_to_transient_database()

        self.log.info('completed the ``get`` method')
        return cleanup_database_tables

    def _updated_row_counts_in_tcs_helper_catalogue_tables_info(
            self):
        """
        *updated row counts in tcs catalogue tables*

        **Key Arguments:**
            # -

        **Return:**
            - None

        .. todo::

            - @review: when complete, clean _updated_row_counts_in_tcs_helper_catalogue_tables_info method
            - @review: when complete add logging
        """
        self.log.info(
            'starting the ``_updated_row_counts_in_tcs_helper_catalogue_tables_info`` method')

        sqlQuery = u"""
            select * from tcs_helper_catalogue_tables_info where table_name like "%%stream" or number_of_rows is null and legacy_table = 0
        """ % locals()
        rows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )

        for row in rows:
            tbName = row["table_name"]

            sqlQuery = u"""
                update tcs_helper_catalogue_tables_info set number_of_rows = (select count(*) as count from %(tbName)s) where table_name = "%(tbName)s"
            """ % locals()
            count = dms.execute_mysql_write_query(
                sqlQuery=sqlQuery,
                dbConn=self.cataloguesDbConn,
                log=self.log,
                quiet=True
            )

        print "Row counts updated in `tcs_helper_catalogue_tables_info` database table"

        sqlQuery = u"""
            select * from tcs_helper_catalogue_views_info where view_name like "%%stream" or number_of_rows is null and legacy_view = 0
        """ % locals()
        rows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )

        for row in rows:
            tbName = row["view_name"]

            sqlQuery = u"""
                update tcs_helper_catalogue_views_info set number_of_rows = (select count(*) as count from %(tbName)s) where view_name = "%(tbName)s"
            """ % locals()
            count = dms.execute_mysql_write_query(
                sqlQuery=sqlQuery,
                dbConn=self.cataloguesDbConn,
                log=self.log,
                quiet=True
            )

        self.log.info(
            'completed the ``_updated_row_counts_in_tcs_helper_catalogue_tables_info`` method')
        return None

    # use the tab-trigger below for new method
    def _update_tcs_helper_catalogue_tables_info_with_new_tables(
            self):
        """
        *update tcs helper catalogue tables info with new tables*

        **Key Arguments:**
            # -

        **Return:**
            - None

        .. todo::

            - @review: when complete, clean _update_tcs_helper_catalogue_tables_info_with_new_tables method
            - @review: when complete add logging
        """
        self.log.info(
            'starting the ``_update_tcs_helper_catalogue_tables_info_with_new_tables`` method')

        sqlQuery = u"""
            SELECT max(id) as thisId FROM tcs_helper_catalogue_tables_info;
        """ % locals()
        thisId = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )
        try:
            highestId = thisId[0]["thisId"] + 1
        except:
            highestId = 1

        sqlQuery = u"""
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA='crossmatch_catalogues' and TABLE_NAME like "tcs_cat%%";
        """ % locals()
        tablesInDatabase = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )

        sqlQuery = u"""
            SELECT table_name FROM tcs_helper_catalogue_tables_info;
        """ % locals()
        tableList = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )
        tbList = []
        for tb in tableList:
            tbList.append(tb["table_name"])

        for tb in tablesInDatabase:
            if tb["TABLE_NAME"] not in tbList:
                thisTableName = tb["TABLE_NAME"]
                print "`%(thisTableName)s` added to `tcs_helper_catalogue_tables_info` database table" % locals()
                sqlQuery = u"""
                    INSERT INTO tcs_helper_catalogue_tables_info (
                            id,
                            table_name
                        )
                        VALUES (
                            %(highestId)s,
                            "%(thisTableName)s"
                    )""" % locals()
                dms.execute_mysql_write_query(
                    sqlQuery=sqlQuery,
                    dbConn=self.cataloguesDbConn,
                    log=self.log
                )
                highestId += 1

        self.log.info(
            'completed the ``_update_tcs_helper_catalogue_tables_info_with_new_tables`` method')
        return None

    # use the tab-trigger below for new method
    def _clean_up_columns(
            self):
        """
        *clean up columns*

        **Key Arguments:**
            # -

        **Return:**
            - None

        .. todo::

            - @review: when complete, clean _clean_up_columns method
            - @review: when complete add logging
        """
        self.log.info('starting the ``_clean_up_columns`` method')

        sqlQueries = [
            "update tcs_helper_catalogue_tables_info set old_table_name = table_name where old_table_name is null;",
            "update tcs_helper_catalogue_tables_info set version_number = 'stream' where table_name like '%%stream' and version_number is null;",
            """update tcs_helper_catalogue_tables_info set in_ned = 0 where table_name like '%%stream' and in_ned is null;""",
            """update tcs_helper_catalogue_tables_info set vizier_link = 0 where table_name like '%%stream' and vizier_link is null;""",
            "update tcs_helper_catalogue_views_info set old_view_name = view_name where old_view_name is null;",
        ]

        for sqlQuery in sqlQueries:
            dms.execute_mysql_write_query(
                sqlQuery=sqlQuery,
                dbConn=self.cataloguesDbConn,
                log=self.log
            )

        # VIEW OBJECT TYPES
        sqlQuery = u"""
            SELECT view_name FROM crossmatch_catalogues.tcs_helper_catalogue_views_info where legacy_view = 0 and object_type is null;
        """ % locals()
        rows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )

        for row in rows:
            view_name = row["view_name"]
            object_type = view_name.replace("tcs_view_", "").split("_")[0]

            sqlQuery = u"""
                update tcs_helper_catalogue_views_info set object_type = "%(object_type)s" where view_name = "%(view_name)s"
            """ % locals()
            dms.execute_mysql_write_query(
                sqlQuery=sqlQuery,
                dbConn=self.cataloguesDbConn,
                log=self.log
            )

        # MASTER TABLE ID FOR VIEWS
        sqlQuery = u"""
            SELECT view_name FROM crossmatch_catalogues.tcs_helper_catalogue_views_info where legacy_view = 0 and table_id is null;
        """ % locals()
        rows = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )

        for row in rows:
            view_name = row["view_name"]
            table_name = view_name.replace("tcs_view_", "").split("_")[1:]
            table_name = ("_").join(table_name)
            table_name = "tcs_cat_%(table_name)s" % locals()

            # print table_name

            sqlQuery = u"""
                update tcs_helper_catalogue_views_info set table_id = (select id from tcs_helper_catalogue_tables_info where table_name = "%(table_name)s") where view_name = "%(view_name)s"
            """ % locals()
            dms.execute_mysql_write_query(
                sqlQuery=sqlQuery,
                dbConn=self.cataloguesDbConn,
                log=self.log
            )

        self.log.info('completed the ``_clean_up_columns`` method')
        return None

    # use the tab-trigger below for new method
    def _update_tcs_helper_catalogue_views_info_with_new_views(
            self):
        """
        *update tcs helper catalogue tables info with new tables*

        **Key Arguments:**
            # -

        **Return:**
            - None

        .. todo::

            - @review: when complete, clean _update_tcs_helper_catalogue_views_info_with_new_views method
            - @review: when complete add logging
        """
        self.log.info(
            'starting the ``_update_tcs_helper_catalogue_views_info_with_new_views`` method')

        sqlQuery = u"""
            SELECT max(id) as thisId FROM tcs_helper_catalogue_views_info;
        """ % locals()
        thisId = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )
        try:
            highestId = thisId[0]["thisId"] + 1
        except:
            highestId = 1

        sqlQuery = u"""
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='VIEW' AND TABLE_SCHEMA='crossmatch_catalogues' and TABLE_NAME like "tcs_view%%" and TABLE_NAME not like "%%helper%%";
        """ % locals()
        tablesInDatabase = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )

        sqlQuery = u"""
            SELECT view_name FROM tcs_helper_catalogue_views_info;
        """ % locals()
        tableList = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )
        tbList = []
        for tb in tableList:
            tbList.append(tb["view_name"])

        for tb in tablesInDatabase:
            if tb["TABLE_NAME"] not in tbList:
                thisViewName = tb["TABLE_NAME"]
                print "`%(thisViewName)s` added to `tcs_helper_catalogue_views_info` database table" % locals()
                sqlQuery = u"""
                    INSERT INTO tcs_helper_catalogue_views_info (
                            id,
                            view_name
                        )
                        VALUES (
                            %(highestId)s,
                            "%(thisViewName)s"
                    )""" % locals()
                dms.execute_mysql_write_query(
                    sqlQuery=sqlQuery,
                    dbConn=self.cataloguesDbConn,
                    log=self.log
                )
                highestId += 1

        self.log.info(
            'completed the ``_update_tcs_helper_catalogue_views_info_with_new_views`` method')
        return None

    # use the tab-trigger below for new method
    def _copy_helper_tables_to_transient_database(
            self):
        """ copy helper tables to transient database

        **Key Arguments:**
            # -

        **Return:**
            - None

        **Todo**
            - @review: when complete, clean _copy_helper_tables_to_transient_database method
            - @review: when complete add logging

        .. todo ::

            - need to change pessto marshall database to database table from settings file
        """
        self.log.info(
            'starting the ``_copy_helper_tables_to_transient_database`` method')

        sqlQuery = u"""
            DELETE FROM pessto_marshall.tcs_helper_catalogue_tables_info;
            INSERT INTO pessto_marshall.tcs_helper_catalogue_tables_info SELECT * FROM crossmatch_catalogues.tcs_helper_catalogue_tables_info;
        """ % locals()
        dms.execute_mysql_write_query(
            sqlQuery=sqlQuery,
            dbConn=self.transientsDbConn,
            log=self.log,
            quiet=False,
            Force=False,  # do not exit code if error occurs
            manyValueList=False  # a list of value tuples if executing more than one insert
        )

        sqlQuery = u"""
            DELETE FROM pessto_marshall.tcs_helper_catalogue_views_info;
            INSERT INTO pessto_marshall.tcs_helper_catalogue_views_info SELECT * FROM crossmatch_catalogues.tcs_helper_catalogue_views_info;
        """ % locals()
        dms.execute_mysql_write_query(
            sqlQuery=sqlQuery,
            dbConn=self.transientsDbConn,
            log=self.log,
            quiet=False,
            Force=False,  # do not exit code if error occurs
            manyValueList=False  # a list of value tuples if executing more than one insert
        )

        self.log.info(
            'completed the ``_copy_helper_tables_to_transient_database`` method')
        return None

    # use the tab-trigger below for new method
    # xt-class-method

    # 5. @flagged: what actions of the base class(es) need ammending? ammend them here
    # Override Method Attributes
    # method-override-tmpx

# xt-class-tmpx


###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################
# xt-worker-def

# use the tab-trigger below for new function
# xt-def-with-logger

###################################################################
# PRIVATE (HELPER) FUNCTIONS                                      #
###################################################################

if __name__ == '__main__':
    main()
