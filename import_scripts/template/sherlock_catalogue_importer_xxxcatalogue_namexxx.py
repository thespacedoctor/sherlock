#!/usr/local/bin/python
# encoding: utf-8
"""
*Import xxxcatalogue_namexxx catagloue into the sherlock catalogues database*

xxxcatalogue_urlxxx

:Author:
    David Young

:Date Created:
    xxxnow-datexxx

Usage:
    sherlock_catalogue_importer_xxxcatalogue_namexxx.py <pathToDataFile> <cat_version> [-s <pathToSettingsFile>]

Options:
    pathToDataFile        path to the plain text xxxcatalogue_namexxx catalogue downloaded from "xxxcatalogue_urlxxx"
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
    *The main function used when ``sherlock_catalogue_importer_xxxcatalogue_namexxx.py`` is run as a single script from the cl*
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

    catalogue = importer(
        log=log,
        settings=settings,
        pathToDataFile=pathToDataFile,
        version=cat_version,
        catalogueName="xxxcatalogue_namexxx"
    )
    # catalogue.ingest()
    dictList = catalogue._create_dictionary_of_glade()
    print dictList

    return


class importer(_base_importer):
    """
    *Import the* `xxxcatalogue_namexxx catalogue <xxxcatalogue_urlxxx>`_ *catagloue into the sherlock-catalogues database*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary
        - ``pathToDataFIle`` -- path to the xxxcatalogue_namexxx data file
        - ``version`` -- version of the xxxcatalogue_namexxx catalogue
        - ``catalogueName`` -- name of the catalogue to be imported (xxxcatalogue_namexxx)
    """
    # INITIALISATION

    def ingest(self):
        """Ingest the xxxcatalogue_namexxx catalogue into the sherlock catalogues database

        The method first generates a list of python dictionaries from the xxxcatalogue_namexxx datafile, imports this list of dictionaries into a database table and then generates the HTMIDs for that table. 

        **Usage:**

            See class docstring for usage
        """
        self.log.debug('starting the ``get`` method')

        dictList = self._create_dictionary_of_xxxcatalogue_namexxx()

        tableName = self.dbTableName
        createStatement = """
CREATE TABLE `%(tableName)s` (
  `primaryId` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'An internal counter',
  
  --- ADD THE EXTRA COLUMNS HERE

  `htm16ID` bigint(20) DEFAULT NULL,
  `htm10ID` bigint(20) DEFAULT NULL,
  `htm13ID` bigint(20) DEFAULT NULL,
  `dateCreated` datetime DEFAULT CURRENT_TIMESTAMP,
  `dateLastModified` datetime DEFAULT CURRENT_TIMESTAMP,
  `updated` varchar(45) DEFAULT '0',
  PRIMARY KEY (`primaryId`),
  UNIQUE KEY `radeg_name` (`raDeg`,`name`),
  KEY `idx_htm16ID` (`htm16ID`),
  KEY `idx_htm10ID` (`htm10ID`),
  KEY `idx_htm13ID` (`htm13ID`)
) ENGINE=MyISAM AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
""" % locals()

        self.add_data_to_database_table(
            dictList=dictList,
            createStatement=createStatement
        )

        self.log.debug('completed the ``get`` method')
        return None

    def _create_dictionary_of_xxxcatalogue_namexxx(
            self):
        """create a list of dictionaries containing all the rows in the xxxcatalogue_namexxx catalogue

        **Return:**
            - ``dictList`` - a list of dictionaries containing all the rows in the xxxcatalogue_namexxx catalogue
        """
        self.log.debug(
            'starting the ``_create_dictionary_of_xxxcatalogue_namexxx`` method')

        dictList = []
        lines = string.split(self.catData, '\n')

        # START HERE
        # ADD CODE TO GENERATE LIST OF DICTIONARIES FROM INPUT DATA

        # FIRST TIME ROUND TRY AND
        # - CONVERT A SINGLE DICTIONARY IN MYSQL TO GENERATE TABLE (EXPECT SNIPPET BELOW}
        # - THEN OPEN TABLE IN MYSQLWORKBENCH AND ALTER COLUMN TYPES AND ADD INDEXES TO MAGS, COORDINATE, OBJECT TYPE AND DIST COLUMNS
        # - THEN COPY TABLE CREATE STATEMENT INTO ingest METHOD ABOVE ALONG WITH HTMID COLUMNS AND INDEXES AND
        # - DROP THE TABLE CREATED
        # - REMOVE THIS CODE BELOW
        # - UNCOMMENT catalogue.ingest() AND RUN AGAIN
        import re
        from fundamentals.mysql import convert_dictionary_to_mysql_table
        insertCommand, valueTuple = convert_dictionary_to_mysql_table(
            dbConn=self.cataloguesDbConn,
            log=self.log,
            dictionary=dictList[0],
            dbTableName=self.tableName,
            uniqueKeyList=[],
            dateModified=False,
            returnInsertOnly=False,
            replace=False,
            batchInserts=True,  # will only return inserts,
            reDatetime=re.compile('^[0-9]{4}-[0-9]{2}-[0-9]{2}T')  # OR FALSE
        )

        self.log.debug(
            'completed the ``_create_dictionary_of_xxxcatalogue_namexxx`` method')
        return dictList

if __name__ == '__main__':
    main()
