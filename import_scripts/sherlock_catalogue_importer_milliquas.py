#!/usr/local/bin/python
# encoding: utf-8
"""
*Import Milliquas (Million Quasars Catalog) catagloue from plain text file*

http://heasarc.gsfc.nasa.gov/w3browse/all/milliquas.html

:Author:
    David Young

:Date Created:
    March  6, 2018

Usage:
    sherlock_catalogue_importer_milliquas.py <pathToDataFile> <cat_version> [-s <pathToSettingsFile>]

Options:
    pathToDataFile        path to the plain text milliquas catalogue downloaded from "http://heasarc.gsfc.nasa.gov/w3browse/all/milliquas.html"
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
    *The main function used when ``sherlock_catalogue_importer_milliquas.py`` is run as a single script from the cl*
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
        catalogueName="milliquas"
    )
    catalogue.ingest()

    return


class importer(_base_importer):
    """
    *Import the* `Milliquas (Million Quasars Catalog) <http://heasarc.gsfc.nasa.gov/w3browse/all/milliquas.html>`_ *catagloue into the sherlock-catalogues database*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary
        - ``pathToDataFIle`` -- path to the milliquas data file
        - ``version`` -- version of the milliquas catalogue
        - ``catalogueName`` -- name of the catalogue to be imported (milliquas)
    """
    # INITIALISATION

    def ingest(self):
        """Ingest the milliquas catalogue into the sherlock catalogues database

        The method first generates a list of python dictionaries from the milliquas datafile, imports this list of dictionaries into a database table and then generates the HTMIDs for that table. 

        **Usage:**

            See class docstring for usage
        """
        self.log.info('starting the ``get`` method')

        dictList = self._create_dictionary_of_milliquas()

        tableName = self.dbTableName
        createStatement = """
CREATE TABLE `%(tableName)s` (
  `primaryId` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'An internal counter',
  `b_psf_class` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
  `bmag` float DEFAULT NULL,
  `comment` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
  `decDeg` double DEFAULT NULL,
  `descrip` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
  `name` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
  `qso_prob` int(11) DEFAULT NULL,
  `raDeg` double DEFAULT NULL,
  `rmag` float DEFAULT NULL,
  `src_cat_name` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
  `x_name` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
  `src_cat_z` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
  `z` float DEFAULT NULL,
  `r_psf_class` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
  `r_name` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
  `alt_id1` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
  `alt_id2` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
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

        self.log.info('completed the ``get`` method')
        return None

    def _create_dictionary_of_milliquas(
            self):
        """create a list of dictionaries containing all the rows in the milliquas catalogue

        **Return:**
            - ``dictList`` - a list of dictionaries containing all the rows in the milliquas catalogue
        """
        self.log.info(
            'starting the ``_create_dictionary_of_milliquas`` method')

        dictList = []
        lines = string.split(self.catData, '\n')
        inserts = [
            11, 25, 51, 57, 64, 71, 75, 78, 81, 89, 97, 106, 110, 134, 158, 181]
        keys = ["raDeg", "decDeg", "name", "descrip", "rmag", "bmag", "comment", "r_psf_class", "b_psf_class", "z",
                "src_cat_name", "src_cat_z", "qso_prob", "x_name", "r_name", "alt_id1", "alt_id2"]

        totalCount = len(lines)

        print "adding milliquas data to memory" % locals()
        for line in lines:

            thisDict = {}
            for insert in inserts:
                line.replace("–", "-")
                line = line[:insert] + "|" + line[insert:]

            theseValues = string.split(line, '|')
            for k, v in zip(keys, theseValues):
                v = v.strip()
                if len(v) == 0 or v == "-":
                    v = None
                thisDict[k] = v
            dictList.append(thisDict)

        self.log.info(
            'completed the ``_create_dictionary_of_milliquas`` method')
        return dictList

if __name__ == '__main__':
    main()
