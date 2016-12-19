#!/usr/local/bin/python
# encoding: utf-8
"""
*Import Milliquas (Million Quasars Catalog) catagloue from plain text file*

http://heasarc.gsfc.nasa.gov/w3browse/all/milliquas.html

:Author:
    David Young

:Date Created:
    November 18, 2016
"""
################# GLOBAL IMPORTS ####################
import sys
import os
os.environ['TERM'] = 'vt100'
import readline
import glob
import pickle
import codecs
import string

import re
from docopt import docopt
from ._base_importer import _base_importer


class milliquas(_base_importer):

    """
    *importer object for the* `Milliquas (Million Quasars Catalog) <http://heasarc.gsfc.nasa.gov/w3browse/all/milliquas.html>`_ *catagloue*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary
        - ``pathToDataFIle`` -- path to the milliquas data file
        - ``version`` -- version of the milliquas catalogue

    **Usage:**

      To import the plain text milliquas catalogue, run the following:

      .. code-block:: python 

        from sherlock.imports import milliquas
        catalogue = milliquas(
            log=log,
            settings=settings,
            pathToDataFile="/path/to/milliquas.txt",
            version="1.0",
            catalogueName="milliquas"
        )
        catalogue.ingest()
    """
    # INITIALISATION

    def ingest(self):
        """ingest the milliquas catalogue into the catalogues database

        The method first generates a list of python dictionaries from the milliquas datafile, imports this list of dictionaries into a database table and then generates the HTMIDs for that table. 
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

        self._add_data_to_database_table(
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

    # use the tab-trigger below for new method
    # xt-class-method
