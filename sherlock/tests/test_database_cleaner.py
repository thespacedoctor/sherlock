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

# from fundamentals.mysql import writequery
# sqlQuery = """drop table if exists `tcs_helper_catalogue_tables_info`, `tcs_helper_catalogue_views_info`"""
# writequery(
#     log=log,
#     sqlQuery=sqlQuery,
#     dbConn=cataloguesDbConn
# )

class test_database_cleaner(unittest.TestCase):

    def test_database_cleaner_function(self):

        from sherlock.database_cleaner import database_cleaner
        db = database_cleaner(
            log=log,
            settings=settings
        )
        db.clean()

    def test_database_cleaner_function_exception(self):

        from sherlock import database_cleaner
        try:
            this = database_cleaner(
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
