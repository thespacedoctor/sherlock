#!/usr/local/bin/python
# encoding: utf-8
"""
*Import IFS galaxies from the IFS galaxy stream*

:Author:
    David Young

:Date Created:
    October  8, 2015

.. todo::
    
    @review: when complete pull all general functions and classes into dryxPython
"""
################# GLOBAL IMPORTS ####################
import sys
import os
os.environ['TERM'] = 'vt100'
import codecs
from dryxPython import logs as dl
from dryxPython import commonutils as dcu
from fundamentals import tools, times
from dryxPython import webcrawlers as dwc
from dryxPython import astrotools as dat
from ._base_importer import _base_importer


class ifs_galaxies(_base_importer):
    """
    *The worker class for the ifs_galaxies module*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary
        - ``pathToDataFIle`` -- path to the pessto_marshall_streams data file
        - ``version`` -- version of the pessto_marshall_streams catalogue

    .. todo::

        - @review: when complete, clean ifs_galaxies class
        - @review: when complete add logging
        - @review: when complete, decide whether to abstract class to another module
    """

    # 4. @flagged: what actions does each object have to be able to perform? Add them here
    # Method Attributes
    def get(self):
        """
        *get the ifs_galaxies object*

        **Return:**
            - ``ifs_galaxies``

        .. todo::

            - @review: when complete, clean get method
            - @review: when complete add logging
        """
        self.log.info('starting the ``get`` method')

        self.primaryIdColumnName = "primaryId"
        self.raColName = "raDeg"
        self.declColName = "decDeg"

        self.dbTableName = "tcs_cat_ifs_stream"
        self.databaseInsertbatchSize = 500
        self.dictList = self._download_and_parse_ifs_galaxy_csv()
        self.add_data_to_database_table()
        self.add_htmids_to_database_table()
        self._update_database_helper_table()

        self.log.info('completed the ``get`` method')
        return ifs_galaxies

    def _download_and_parse_ifs_galaxy_csv(
            self):
        """
        *download and parse ifs galaxy csv*

        **Key Arguments:**
            # -

        **Return:**
            - None

        .. todo::

            - @review: when complete, clean _download_and_parse_ifs_galaxy_csv method
            - @review: when complete add logging
        """
        self.log.info(
            'starting the ``_download_and_parse_ifs_galaxy_csv`` method')

        urlDoc = dwc.singleWebDocumentDownloader(
            url=self.settings["ifs galaxies url"],
            downloadDirectory="/tmp",
            log=self.log,
            timeStamp=1,
            credentials=False
        )

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

                try:
                    raDeg = dat.ra_sexegesimal_to_decimal.ra_sexegesimal_to_decimal(values[
                                                                                    1].strip())
                except:
                    raDeg = float(values[1].strip())
                thisDict["raDeg"] = raDeg
                try:
                    decDeg = dat.declination_sexegesimal_to_decimal.declination_sexegesimal_to_decimal(values[
                                                                                                       2].strip())
                except:
                    decDeg = float(values[2].strip())
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
            'completed the ``_download_and_parse_ifs_galaxy_csv`` method')
        return dictList

    # use the tab-trigger below for new method
    # xt-class-method

    # 5. @flagged: what actions of the base class(es) need ammending? ammend them here
    # Override Method Attributes
    # method-override-tmpx
