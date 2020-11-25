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

print(settingsFile)

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

# class test_update_wiki_pages(unittest.TestCase):

#     def test_update_wiki_pages_function(self):

#         from sherlock.commonutils import update_wiki_pages
#         wiki = update_wiki_pages(
#             log=log,
#             settings=settings
#         )
#         wiki.update()

#     def test_update_wiki_pages_function_exception(self):

#         from sherlock.commonutils import update_wiki_pages
#         try:
#             this = update_wiki_pages(
#                 log=log,
#                 settings=settings,
#                 fakeKey="break the code"
#             )
#             this.update()
#             assert False
#         except Exception as e:
#             assert True
#             print(str(e))

# x-class-to-test-named-worker-function
