#!/usr/local/bin/python
# encoding: utf-8
"""
*Import Multi Unit Spectroscopic Explorer (MUSE) IFS galaxy stream into sherlock-catalogues database*

:Author:
    David Young
"""
from __future__ import print_function
from ._base_importer import _base_importer
from fundamentals.download import multiobject_download
from astrocalc.coords import unit_conversion
from docopt import docopt
import re
import requests
import string
import codecs
import pickle
import glob
import readline
import sys
import os
os.environ['TERM'] = 'vt100'


class ifs(_base_importer):
    """
    *Importer for the Multi Unit Spectroscopic Explorer (MUSE) IFS galaxy catalogue stream*

    **Key Arguments**

    - ``log`` -- logger
    - ``settings`` -- the settings dictionary


    **Usage**

    To import the IFS catalogue stream into the sherlock-catalogues database, run the following:


      ```python
      from sherlock.imports import IFS
      ```

        stream = IFS(
            log=log,
            settings=settings
        )
        stream.ingest()

    .. todo ::

        - abstract this module out into its own stand alone script
        - check sublime snippet exists
    """
    # INITIALISATION

    def ingest(self):
        """*Import the IFS catalogue into the sherlock-catalogues database*

        The method first generates a list of python dictionaries from the IFS datafile, imports this list of dictionaries into a database table and then generates the HTMIDs for that table. 

        **Usage**

        See class docstring for usage

        """
        self.log.debug('starting the ``get`` method')

        self.primaryIdColumnName = "primaryId"
        self.raColName = "raDeg"
        self.declColName = "decDeg"
        self.dbTableName = "tcs_cat_ifs_stream"
        self.databaseInsertbatchSize = 500

        dictList = self._create_dictionary_of_IFS()

        tableName = self.dbTableName
        createStatement = """
    CREATE TABLE IF NOT EXISTS `%(tableName)s` (
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

        self.add_data_to_database_table(
            dictList=dictList,
            createStatement=createStatement
        )

        self.log.debug('completed the ``get`` method')
        return None

    def _create_dictionary_of_IFS(
            self):
        """*Generate the list of dictionaries containing all the rows in the IFS stream*

        **Return**

        - ``dictList`` - a list of dictionaries containing all the rows in the IFS stream


        **Usage**

        ```python
        from sherlock.imports import IFS
        stream = IFS(
            log=log,
            settings=settings
        )
        dictList = stream._create_dictionary_of_IFS()
        ```

        """
        self.log.debug(
            'starting the ``_create_dictionary_of_IFS`` method')

        # GRAB THE CONTENT OF THE IFS CSV
        try:
            response = requests.get(
                url=self.settings["ifs galaxies url"],
            )
            thisData = response.content
            thisData = str(thisData).split("\n")
            status_code = response.status_code
        except requests.exceptions.RequestException:
            print('HTTP Request failed')
            sys.exit(0)

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
                try:
                    raDeg = converter.ra_sexegesimal_to_decimal(
                        ra=values[1].strip()
                    )
                    thisDict["raDeg"] = raDeg
                    decDeg = converter.dec_sexegesimal_to_decimal(
                        dec=values[2].strip()
                    )
                    thisDict["decDeg"] = decDeg
                except:
                    name = thisDict["name"]
                    self.log.warning(
                        'Could not convert the coordinates for IFS source %(name)s. Skipping import of this source.' % locals())
                    continue
                try:
                    z = float(values[3].strip())
                    if z > 0.:
                        thisDict["z"] = float(values[3].strip())
                    else:
                        thisDict["z"] = None
                except:
                    thisDict["z"] = None
                dictList.append(thisDict)

        self.log.debug(
            'completed the ``_create_dictionary_of_IFS`` method')
        return dictList

    # use the tab-trigger below for new method
    # xt-class-method
