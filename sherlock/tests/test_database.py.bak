from __future__ import print_function
from builtins import str
import os
import unittest
import shutil
import yaml
from sherlock.utKit import utKit
from fundamentals import tools
from os.path import expanduser
home = expanduser("~")

packageDirectory = utKit("").get_project_root()
settingsFile = packageDirectory + "/test_settings.yaml"

su = tools(
    arguments={"settingsFile": settingsFile},
    docString=__doc__,
    logLevel="DEBUG",
    options_first=False,
    projectName=None,
    defaultSettingsFile=False
)
arguments, settings, log, dbConn = su.setup()

# SETUP PATHS TO COMMON DIRECTORIES FOR TEST DATA
moduleDirectory = os.path.dirname(__file__)
pathToInputDir = moduleDirectory + "/input/"
pathToOutputDir = moduleDirectory + "/output/"

try:
    shutil.rmtree(pathToOutputDir)
except:
    pass
# COPY INPUT TO OUTPUT DIR
shutil.copytree(pathToInputDir, pathToOutputDir)

# Recursively create missing directories
if not os.path.exists(pathToOutputDir):
    os.makedirs(pathToOutputDir)


class test_database(unittest.TestCase):

    def test_tunnel(self):

        from sherlock import database
        db = database(
            log=log,
            settings=settings
        )
        sshPort = db._setup_tunnel(
            tunnelParameters=settings["database settings"][
                "static catalogues2"]["tunnel"]
        )

        return

    def test_database_function(self):

        # SETUP ALL DATABASE CONNECTIONS
        # SETUP ALL DATABASE CONNECTIONS
        from sherlock import database
        db = database(
            log=log,
            settings=settings
        )
        dbConns, dbVersions = db.connect()
        self.transientsDbConn = dbConns["transients"]
        self.cataloguesDbConn = dbConns["catalogues"]

        from fundamentals.mysql import readquery
        sqlQuery = u"""
            SELECT VERSION();
        """ % locals()
        rows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=self.transientsDbConn,
            quiet=False
        )
        print(rows)
        rows = readquery(
            log=log,
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            quiet=False
        )
        print(rows)

    def test_database_function_exception(self):

        from sherlock import database
        try:
            this = database(
                log=log,
                settings=settings,
                fakeKey="break the code"
            )
            this.get()
            assert False
        except Exception, e:
            assert True
            print(str(e))

    # x-class-to-test-named-worker-function
