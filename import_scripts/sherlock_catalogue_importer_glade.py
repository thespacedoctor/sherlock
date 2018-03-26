#!/usr/local/bin/python
# encoding: utf-8
"""
*Import glade catagloue into the sherlock catalogues database*

http://aquarius.elte.hu/glade/

:Author:
    David Young

:Date Created:
    March  9, 2018

Usage:
    sherlock_catalogue_importer_glade.py <pathToDataFile> <cat_version> [-s <pathToSettingsFile>]

Options:
    pathToDataFile        path to the plain text glade catalogue downloaded from "http://aquarius.elte.hu/glade/"
    cat_version           current version number of the catalogue
    pathToSettingsFile    path to the sherlock settings file containing details of the sherlock catalogues database connection

    -h, --help            show this help message
    -v, --version         show version
    -s, --settings        the settings file
"""
################# GLOBAL IMPORTS ####################
import sys
import os
from fundamentals import tools
import readline
import glob
import pickle
import codecs
import string
import re
from docopt import docopt
from sherlock.imports import _base_importer


def main(arguments=None):
    """
    *The main function used when ``sherlock_catalogue_importer_glade.py`` is run as a single script from the cl*
    """

    # SETUP THE COMMAND-LINE UTIL SETTINGS
    su = tools(
        arguments=arguments,
        docString=__doc__,
        logLevel="WARNING",
        options_first=False,
        projectName=False
    )
    arguments, settings, log, dbConn = su.setup()

    # UNPACK REMAINING CL ARGUMENTS USING `EXEC` TO SETUP THE VARIABLE NAMES
    # AUTOMATICALLY
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

    # NOTE TO USER
    # 1. In the _create_dictionary_of_glade() METHOD GENERATE A LIST OF
    # DICTIONARIES FROM THE INPUT CATALOGUE FILE
    catalogue = importer(
        log=log,
        settings=settings,
        pathToDataFile=pathToDataFile,
        version=cat_version,
        catalogueName="glade"
    )
    catalogue.ingest()
    #dictList = catalogue._create_dictionary_of_glade()
    # print dictList

    return


class importer(_base_importer):
    """
    *Import the* `glade catalogue <http://aquarius.elte.hu/glade/>`_ *catagloue into the sherlock-catalogues database*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary
        - ``pathToDataFIle`` -- path to the glade data file
        - ``version`` -- version of the glade catalogue
        - ``catalogueName`` -- name of the catalogue to be imported (glade)
    """
    # INITIALISATION

    def ingest(self):
        """Ingest the glade catalogue into the sherlock catalogues database

        The method first generates a list of python dictionaries from the glade datafile, imports this list of dictionaries into a database table and then generates the HTMIDs for that table. 

        **Usage:**

            See class docstring for usage
        """
        self.log.debug('starting the ``get`` method')

        dictList = self._create_dictionary_of_glade()

        tableName = self.dbTableName
        createStatement = """CREATE TABLE `%(tableName)s` (
  `primaryId` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'An internal counter',
  `2mass_id` varchar(50) DEFAULT NULL,
  `sdss_id` varchar(50) DEFAULT NULL,
  `b_mag` float DEFAULT NULL,
  `b_mag_err` float DEFAULT NULL,
  `abs_b_mag` float DEFAULT NULL,
  `decDeg` double DEFAULT NULL,
  `dist_lum_flag` tinyint(4) DEFAULT NULL,
  `dist_lum_mpc` float DEFAULT NULL,
  `dist_lum_mpc_err` float DEFAULT NULL,
  `gwgc_id` varchar(50) DEFAULT NULL,
  `h_mag` float DEFAULT NULL,
  `h_mag_err` float DEFAULT NULL,
  `hyperleda_id` varchar(50) DEFAULT NULL,
  `j_mag` float DEFAULT NULL,
  `j_mag_err` float DEFAULT NULL,
  `k_mag` float DEFAULT NULL,
  `k_mag_err` float DEFAULT NULL,
  `object_type` varchar(5) DEFAULT NULL,
  `pgc_id` varchar(50) DEFAULT NULL,
  `raDeg` double DEFAULT NULL,
  `redshift` double DEFAULT NULL,
  `velocity_corrected` tinyint(4) DEFAULT NULL,
  `dateCreated` datetime DEFAULT CURRENT_TIMESTAMP,
  `dateLastModified` datetime DEFAULT CURRENT_TIMESTAMP,
  `updated` varchar(45) DEFAULT '0',
  `htm16ID` bigint(20) DEFAULT NULL,
  `htm10ID` bigint(20) DEFAULT NULL,
  `htm13ID` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`primaryId`),
  KEY `idx_b_mag` (`b_mag`),
  KEY `idx_j_mag` (`j_mag`),
  KEY `idx_h_mag` (`h_mag`),
  KEY `idx_k_mag` (`k_mag`),
  KEY `idx_ra` (`radeg`),
  KEY `idx_dist_lum` (`dist_lum_mpc`),
  KEY `idx_object_type` (`object_type`),
  KEY `idx_decDeg` (`decdeg`),
  KEY `idx_htm16ID` (`htm16ID`),
  KEY `idx_htm10ID` (`htm10ID`),
  KEY `idx_htm13ID` (`htm13ID`)
) ENGINE=Innodb AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
""" % locals()

        self.add_data_to_database_table(
            dictList=dictList,
            createStatement=createStatement
        )

        self.log.debug('completed the ``get`` method')
        return None

    def _create_dictionary_of_glade(
            self):
        """create a list of dictionaries containing all the rows in the glade catalogue

        **Return:**
            - ``dictList`` - a list of dictionaries containing all the rows in the glade catalogue
        """
        self.log.debug(
            'starting the ``_create_dictionary_of_glade`` method')

        dictList = []
        lines = string.split(self.catData, '\n')

        keys = ["pgc_id", "gwgc_id", "hyperleda_id", "2mass_id", "sdss_id", "object_type", "radeg", "decdeg", "dist_lum_mpc", "dist_lum_mpc_err", "redshift",
                "b_mag", "b_mag_err", "abs_b_mag", "j_mag", "j_mag_err", "h_mag", "h_mag_err", "k_mag", "k_mag_err", "dist_lum_flag", "velocity_corrected"]

        for line in lines:
            values = line.split()
            thisDict = {}
            for k, v in zip(keys, values):
                if v == "null":
                    v = None

                thisDict[k] = v
            dictList.append(thisDict)

        # FIRST TIME ROUND TRY AND
        # - CONVERT A SINGLE DICTIONARY IN MYSQL TO GENERATE TABLE
        # - THEN OPEN TABLE IN MYSQLWORKBENCH AND ALTER COLUMN TYPES AND ADD INDEXES TO MAGS, COORDINATE, OBJECT TYPE AND DIST COLUMNS
        # - THEN COPY TABLE CREATE STATEMENT INTO ingest METHOD ABOVE ALONG WITH HTMID COLUMNS AND INDEXES AND
        # - DROP THE TABLE CREATED
        # - REMOVE THIS CODE BELOW
        # - UNCOMMENT catalogue.ingest() AND RUN AGAIN
        # import re
        # from fundamentals.mysql import convert_dictionary_to_mysql_table

        # for l in dictList:

        #     insertCommand, valueTuple = convert_dictionary_to_mysql_table(
        #         dbConn=self.cataloguesDbConn,
        #         log=self.log,
        #         dictionary=l,
        #         dbTableName=self.dbTableName,
        #         uniqueKeyList=[],
        #         dateModified=False,
        #         returnInsertOnly=False,
        #         replace=False,
        #         batchInserts=True,  # will only return inserts,
        #         reDatetime=re.compile(
        #             '^[0-9]{4}-[0-9]{2}-[0-9]{2}T')  # OR FALSE
        #     )

        self.log.debug(
            'completed the ``_create_dictionary_of_glade`` method')
        return dictList


if __name__ == '__main__':
    main()
