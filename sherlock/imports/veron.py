#!/usr/local/bin/python
# encoding: utf-8
"""
*import veron catalogue into sherlock-catalogues database*

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
from ._base_importer import _base_importer


class veron(_base_importer):

    """
    *importer object for the* `VERON AGN catalogue <http://cdsarc.u-strasbg.fr/viz-bin/Cat?VII/258>`_

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary
        - ``pathToDataFIle`` -- path to the veron data file
        - ``version`` -- version of the veron catalogue

    **Usage:**

      To import the veron catalogue catalogue, run the following:

      .. code-block:: python 

        from sherlock.imports import veron
        catalogue = veron(
            log=log,
            settings=settings,
            pathToDataFile="/path/to/veron.txt",
            version="1.0",
            catalogueName="veron"
        )
        catalogue.ingest()

    Whenever downloading a version of the Veron catalogue from Vizier use the following column selection:

    .. image:: https://i.imgur.com/4k7MJuw.png
        :width: 800px
        :alt: Veron column selection in Vizier

    .. todo ::

        - update key arguments values and definitions with defaults
        - update return values and definitions
        - update usage examples and text
        - update docstring text
        - check sublime snippet exists
        - clip any useful text to docs mindmap
        - regenerate the docs and check redendering of this docstring

    """
    # INITIALISATION

    def ingest(self):
        """ingest the veron catalogue into the catalogues database

        See class docstring for usage.
        """
        self.log.info('starting the ``get`` method')

        dictList = self._create_dictionary_of_veron()

        tableName = self.dbTableName
        createStatement = """
    CREATE TABLE `%(tableName)s` (
      `primaryId` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'An internal counter',
      `B_V` float DEFAULT NULL,
      `U_B` float DEFAULT NULL,
      `abs_magnitude` float DEFAULT NULL,
      `dateCreated` datetime DEFAULT  CURRENT_TIMESTAMP,
      `decDeg` double DEFAULT NULL,
      `magnitude` float DEFAULT NULL,
      `raDeg` double DEFAULT NULL,
      `class` varchar(10) COLLATE utf8_unicode_ci DEFAULT NULL,
      `name` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
      `redshift` float DEFAULT NULL,
      `not_radio` varchar(10) COLLATE utf8_unicode_ci DEFAULT NULL,
      `magnitude_filter` varchar(10) COLLATE utf8_unicode_ci DEFAULT 'V',
      `htm16ID` bigint(20) DEFAULT NULL,
      `redshift_flag` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
      `spectral_classification` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
      `dateLastModified` datetime DEFAULT CURRENT_TIMESTAMP,
      `updated` varchar(45) DEFAULT '0',
      `htm10ID` bigint(20) DEFAULT NULL,
      `htm13ID` bigint(20) DEFAULT NULL,
      PRIMARY KEY (`primaryId`),
      UNIQUE KEY `radeg_decdeg` (`raDeg`,`decDeg`),
      KEY `idx_htm16ID` (`htm16ID`),
      KEY `idx_htm10ID` (`htm10ID`),
      KEY `idx_htm13ID` (`htm13ID`)
    ) ENGINE=MyISAM AUTO_INCREMENT=168945 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
""" % locals()

        self.add_data_to_database_table(
            dictList=dictList,
            createStatement=createStatement
        )

        self.log.info('completed the ``get`` method')
        return None

    def _create_dictionary_of_veron(
            self):
        """create a list of dictionaries containing all the rows in the veron catalogue

        **Return:**
            - ``dictList`` - a list of dictionaries containing all the rows in the veron catalogue

        .. todo ::

            - update key arguments values and definitions with defaults
            - update return values and definitions
            - update usage examples and text
            - update docstring text
            - check sublime snippet exists
            - clip any useful text to docs mindmap
            - regenerate the docs and check redendering of this docstring
        """
        self.log.info(
            'starting the ``_create_dictionary_of_veron`` method')

        dictList = []
        lines = string.split(self.catData, '\n')

        totalCount = len(lines)
        count = 0
        switch = 0
        for line in lines:
            if (len(line) == 0 or line[0] in ["#", " "]) and switch == 0:
                continue
            else:
                switch = 1
            count += 1
            if count > 1:
                # Cursor up one line and clear line
                sys.stdout.write("\x1b[1A\x1b[2K")
            print "%(count)s / %(totalCount)s veron data added to memory" % locals()

            if count == 1:
                theseKeys = []
                someKeys = string.split(line, '|')
                for key in someKeys:
                    if key == "_RAJ2000":
                        key = "raDeg"
                    if key == "_DEJ2000":
                        key = "decDeg"
                    if key == "Cl":
                        key = "class"
                    if key == "nR":
                        key = "not_radio"
                    if key == "Name":
                        key = "name"
                    if key == "l_z":
                        key = "redshift_flag"
                    if key == "z":
                        key = "redshift"
                    if key == "Sp":
                        key = "spectral_classification"
                    if key == "n_Vmag":
                        key = "magnitude_filter"
                    if key == "Vmag":
                        key = "magnitude"
                    if key == "B-V":
                        key = "B_V"
                    if key == "U-B":
                        key = "U_B"
                    if key == "Mabs":
                        key = "abs_magnitude"
                    theseKeys.append(key)
                continue

            if count in [2, 3]:
                continue

            thisDict = {}
            theseValues = string.split(line, '|')

            for k, v in zip(theseKeys, theseValues):
                v = v.strip()
                if len(v) == 0 or v == "-":
                    v = None
                thisDict[k] = v
            dictList.append(thisDict)

        self.log.info(
            'completed the ``_create_dictionary_of_veron`` method')
        return dictList

    # use the tab-trigger below for new method
    # xt-class-method
