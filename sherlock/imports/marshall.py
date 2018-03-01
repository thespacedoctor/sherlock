#!/usr/local/bin/python
# encoding: utf-8
"""
*Import the ePESSTO marshall stream into sherlock-catalogues database*

:Author:
    David Young

:Date Created:
    December 13, 2016
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
import yaml
from fundamentals.mysql import readquery, directory_script_runner
from docopt import docopt
from ._base_importer import _base_importer


class marshall(_base_importer):

    """
    *Import the ePESSTO Marshall transient streams (includes multiple on-going transient survey streams) into the Sherlock-catalogues database*

    **Key Arguments:**
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary

    **Usage:**

      To import/update the marshall catalogue streams in the sherlock-catalogues database, run the following:

      .. code-block:: python 

        from sherlock.imports import marshall
        stream = marshall(
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
        """*Ingest the ePESSTO Marshall transient stream into the catalogues database*

        The method first creates the tables for the various marshall feeder surveys in the sherlock-catalogues database (if they do not yet exist). Then the marshall database is queried for each transient survey and the results imported into the sherlock-catalogues tables,

        See the class docstring for usage

        .. todo ::

            - convert the directory_script_runner to 'load in file'
        """
        self.log.info('starting the ``get`` method')

        # A YAML DICTIONARY OF sherlock-catalogues TABLE NAME AND THE SELECT
        # QUERY TO LIFT THE DATA FROM THE MARSHALL
        yamlFilePath = '/'.join(string.split(__file__, '/')
                                [:-1]) + "/resources/pessto_marshall_table_selects.yaml"
        stream = file(yamlFilePath, 'r')
        marshallQueries = yaml.load(stream)
        stream.close()
        self.primaryIdColumnName = "primaryId"
        self.raColName = "raDeg"
        self.declColName = "decDeg"

        # CREATE THE MARSHALL IMPORT CATALOGUE TABLES (IF THEY DO NOT EXIST)
        directory_script_runner(
            log=self.log,
            pathToScriptDirectory='/'.join(string.split(__file__,
                                                        '/')[:-1]) + "/resources",
            databaseName=self.settings["database settings"][
                "static catalogues"]["db"],
            loginPath=self.settings["database settings"][
                "static catalogues"]["loginPath"],
            successRule="delete",
            failureRule="failed"
        )

        for k, v in marshallQueries["pessto queries"].iteritems():
            self.dbTableName = k
            self.databaseInsertbatchSize = 500
            dictList = self._create_dictionary_of_marshall(
                marshallQuery=v["query"],
                marshallTable=v["marshallTable"]
            )

            tableName = self.dbTableName
            self.add_data_to_database_table(
                dictList=dictList
            )

        self.log.info('completed the ``get`` method')
        return None

    def _create_dictionary_of_marshall(
            self,
            marshallQuery,
            marshallTable):
        """create a list of dictionaries containing all the rows in the marshall stream

        **Key Arguments:**
            - ``marshallQuery`` -- the query used to lift the required data from the marshall database.
            - ``marshallTable`` -- the name of the marshall table we are lifting the data from.

        **Return:**
            - ``dictList`` - a list of dictionaries containing all the rows in the marshall stream
        """
        self.log.info(
            'starting the ``_create_dictionary_of_marshall`` method')

        dictList = []
        tableName = self.dbTableName

        rows = readquery(
            log=self.log,
            sqlQuery=marshallQuery,
            dbConn=self.pmDbConn,
            quiet=False
        )

        totalCount = len(rows)
        count = 0

        for row in rows:
            if "dateCreated" in row:
                del row["dateCreated"]
            count += 1
            if count > 1:
                # Cursor up one line and clear line
                sys.stdout.write("\x1b[1A\x1b[2K")
            print "%(count)s / %(totalCount)s `%(tableName)s` data added to memory" % locals()
            dictList.append(dict(row))

        self.log.info(
            'completed the ``_create_dictionary_of_marshall`` method')
        return dictList

    # use the tab-trigger below for new method
    # xt-class-method
