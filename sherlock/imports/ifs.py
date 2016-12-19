#!/usr/local/bin/python
# encoding: utf-8
"""
*import Multi Unit Spectroscopic Explorer (MUSE) IFS galaxy stream into sherlock's crossmatch catalogues database*

:Author:
    David Young

:Date Created:
    December 12, 2016
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
from astrocalc.coords import unit_conversion
from fundamentals.download import multiobject_download
from ._base_importer import _base_importer


class ifs(_base_importer):

    """
    *importer object for the Multi Unit Spectroscopic Explorer (MUSE) IFS galaxy catalogue stream*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary

    **Usage:**

      To import the IFS catalogue stream, run the following:

      .. code-block:: python 

        from sherlock.imports import IFS
        stream = IFS(
            log=log,
            settings=settings
        )
        stream.ingest()
    """
    # INITIALISATION

    def ingest(self):
        """ingest the IFS catalogue into the catalogues database

        The method first generates a list of python dictionaries from the IFS datafile, imports this list of dictionaries into a database table and then generates the HTMIDs for that table. 
        """
        self.log.info('starting the ``get`` method')

        self.primaryIdColumnName = "primaryId"
        self.raColName = "raDeg"
        self.declColName = "decDeg"
        self.dbTableName = "tcs_cat_ifs_stream"
        self.databaseInsertbatchSize = 500

        dictList = self._create_dictionary_of_IFS()

        tableName = self.dbTableName
        createStatement = """
    CREATE TABLE `%(tableName)s` (
      `primaryId` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'An internal counter',
      `dateCreated` datetime DEFAULT  CURRENT_TIMESTAMP,
      `decDeg` double DEFAULT NULL,
      `name` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
      `raDeg` double DEFAULT NULL,
      `z` double DEFAULT NULL,
      `htm16ID` bigint(20) DEFAULT NULL,
      `htm10ID` bigint(20) DEFAULT NULL,
      `htm13ID` bigint(20) DEFAULT NULL,
      `dateLastModified` datetime DEFAULT CURRENT_TIMESTAMP,
      `updated` varchar(45) DEFAULT '0',
      PRIMARY KEY (`primaryId`),
      UNIQUE KEY `radeg_decdeg` (`raDeg`,`decDeg`),
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

    def _create_dictionary_of_IFS(
            self):
        """create a list of dictionaries containing all the rows in the IFS stream

        **Return:**
            - ``dictList`` - a list of dictionaries containing all the rows in the IFS stream
        """
        self.log.info(
            'starting the ``_create_dictionary_of_IFS`` method')

        localUrls = multiobject_download(
            urlList=[self.settings["ifs galaxies url"]],
            downloadDirectory="/tmp",
            log=self.log,
            timeStamp=True,
            timeout=180,
            concurrentDownloads=2,
            resetFilename=False,
            credentials=False,  # { 'username' : "...", "password", "..." }
            longTime=True,
            indexFilenames=False
        )
        urlDoc = localUrls[0]

        pathToReadFile = urlDoc
        try:
            self.log.debug("attempting to open the file %s" %
                           (pathToReadFile,))
            readFile = codecs.open(pathToReadFile, encoding='utf-8', mode='r')
            thisData = readFile.readlines()
            readFile.close()
        except IOError, e:
            message = 'could not open the file %s' % (pathToReadFile,)
            self.log.critical(message)
            raise IOError(message)

        dictList = []
        columns = ["name", "raDeg", "decDeg", "z"]
        for line in thisData:
            thisDict = {}
            line = line.strip()
            line = line.replace("\t", " ")
            values = line.split("|")
            if len(values) > 3:
                thisDict["name"] = values[0].strip()

                # ASTROCALC UNIT CONVERTER OBJECT
                converter = unit_conversion(
                    log=self.log
                )
                raDeg = converter.ra_sexegesimal_to_decimal(
                    ra=values[1].strip()
                )
                thisDict["raDeg"] = raDeg
                decDeg = converter.dec_sexegesimal_to_decimal(
                    dec=values[2].strip()
                )
                thisDict["decDeg"] = decDeg
                try:
                    z = float(values[3].strip())
                    if z > 0.:
                        thisDict["z"] = float(values[3].strip())
                    else:
                        thisDict["z"] = None
                except:
                    thisDict["z"] = None
                dictList.append(thisDict)

        self.log.info(
            'completed the ``_create_dictionary_of_IFS`` method')
        return dictList

    # use the tab-trigger below for new method
    # xt-class-method
