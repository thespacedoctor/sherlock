#!/usr/local/bin/python
# encoding: utf-8
"""
*import marshall stream into sherlock-catalogues database*

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
    *importer object for the Marshall transient streams (includes multiple on-going transient survey streams)*

    **Key Arguments:**
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary

    **Usage:**

      To import/update the marshall catalogue streams in the catalogues' database, run the following:

      .. code-block:: python 

        from sherlock.imports import marshall
        stream = marshall(
            log=log,
            settings=settings
        )
        stream.ingest()

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
        """ingest the marshall catalogue into the catalogues database

        The method first generates a list of python dictionaries from the marshall datafile, imports this list of dictionaries into a database table and then generates the HTMIDs for that table. 

        See the class docstring for usage

        .. todo ::

            - update key arguments values and definitions with defaults
            - update return values and definitions
            - update usage examples and text
            - update docstring text
            - check sublime snippet exists
            - clip any useful text to docs mindmap
            - regenerate the docs and check redendering of this docstring
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
