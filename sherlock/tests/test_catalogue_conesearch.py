from __future__ import print_function
from builtins import zip
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

settings["database settings"]["static catalogues"] = settings[
    "database settings"]["static catalogues2"]

# SETUP ALL DATABASE CONNECTIONS
from sherlock import database
db = database(
    log=log,
    settings=settings
)
dbConns, dbVersions = db.connect()
transientsDbConn = dbConns["transients"]
cataloguesDbConn = dbConns["catalogues"]

# GET THE COLUMN MAPS FROM THE CATALOGUE DATABASE
from sherlock.commonutils import get_crossmatch_catalogues_column_map
colMaps = get_crossmatch_catalogues_column_map(
    log=log,
    dbConn=cataloguesDbConn
)

class test_catalogue_conesearch(unittest.TestCase):

    def test_catalogue_conesearch_function(self):

        from sherlock import catalogue_conesearch
        cs = catalogue_conesearch(
            log=log,
            ra="23:01:07.99",
            dec="-01:58:04.5",
            radiusArcsec=60.,
            colMaps=colMaps,
            tableName="tcs_view_agn_milliquas_v5_2",
            dbConn=cataloguesDbConn,
            nearestOnly=False,
            physicalSearch=False
        )
        # catalogueMatches ARE ORDERED BY ANGULAR SEPARATION
        indices, catalogueMatches = cs.search()

        print(catalogueMatches)

    def test_catalogue_conesearch_function2(self):

        from sherlock import catalogue_conesearch
        cs = catalogue_conesearch(
            log=log,
            ra=["23:01:07.99", 45.36722, 13.875250],
            dec=["-01:58:04.5", 30.45671, -25.26721],
            radiusArcsec=60.,
            colMaps=colMaps,
            tableName="tcs_view_agn_milliquas_v5_2",
            dbConn=cataloguesDbConn,
            nearestOnly=False,
            physicalSearch=False
        )
        # catalogueMatches ARE ORDERED BY ANGULAR SEPARATION
        indices, catalogueMatches = cs.search()

        for i, c in zip(indices, catalogueMatches):
            print(i, c)

    def test_catalogue_conesearch_function_exception(self):

        from sherlock import catalogue_conesearch
        try:
            this = catalogue_conesearch(
                log=log,
                settings=settings,
                fakeKey="break the code"
            )
            this.get()
            assert False
        except Exception as e:
            assert True
            print(str(e))

    # x-class-to-test-named-worker-function
