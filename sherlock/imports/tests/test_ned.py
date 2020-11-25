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

# SETUP ALL DATABASE CONNECTIONS
from sherlock import database
db = database(
    log=log,
    settings=settings
)
dbConns, dbVersions = db.connect()
transientsDbConn = dbConns["transients"]
cataloguesDbConn = dbConns["catalogues"]

try:
    from fundamentals.mysql import writequery
    sqlQuery = """drop table IF EXISTS tcs_cat_ned_stream;""" % locals()
    writequery(
        log=log,
        sqlQuery=sqlQuery,
        dbConn=cataloguesDbConn
    )
except:
    pass


class test_ned(unittest.TestCase):

    def test_ned_function(self):
        coordinateList = ["23.2323 -43.23434"]
        from sherlock.imports import ned
        catalogue = ned(
            log=log,
            settings=settings,
            coordinateList=coordinateList,
            radiusArcsec=30
        )
        catalogue.ingest()

    def test_ned_function_exception(self):

        from sherlock.imports import ned
        try:
            this = ned(
                log=log,
                settings=settings,
                fakeKey="break the code"
            )
            this.get()
            assert False
        except Exception as e:
            assert True
            print(str(e))
