import os
import nose
import shutil
import yaml
from sherlock import catalogue_conesearch, cl_utils
from sherlock.utKit import utKit

from fundamentals import tools

su = tools(
    arguments={"settingsFile": None},
    docString=__doc__,
    logLevel="DEBUG",
    options_first=False,
    projectName="sherlock"
)
arguments, settings, log, dbConn = su.setup()

# # load settings
# stream = file(
#     "/Users/Dave/.config/sherlock/sherlock.yaml", 'r')
# settings = yaml.load(stream)
# stream.close()

# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE
moduleDirectory = os.path.dirname(__file__)
utKit = utKit(moduleDirectory)
log, dbConn, pathToInputDir, pathToOutputDir = utKit.setupModule()
utKit.tearDownModule()

# load settings
stream = file(
    pathToInputDir + "/example_settings2.yaml", 'r')
settings = yaml.load(stream)
stream.close()

import shutil
try:
    shutil.rmtree(pathToOutputDir)
except:
    pass
# COPY INPUT TO OUTPUT DIR
shutil.copytree(pathToInputDir, pathToOutputDir)

# Recursively create missing directories
if not os.path.exists(pathToOutputDir):
    os.makedirs(pathToOutputDir)

# xt-setup-unit-testing-files-and-folders

# SETUP ALL DATABASE CONNECTIONS
from sherlock import database
db = database(
    log=log,
    settings=settings
)
dbConns, dbVersions = db.connect()
transientsDbConn = dbConns["transients"]
cataloguesDbConn = dbConns["catalogues"]
pmDbConn = dbConns["marshall"]

# GET THE COLUMN MAPS FROM THE CATALOGUE DATABASE
from sherlock.commonutils import get_crossmatch_catalogues_column_map
colMaps = get_crossmatch_catalogues_column_map(
    log=log,
    dbConn=cataloguesDbConn
)


class test_catalogue_conesearch():

    def test_catalogue_conesearch_function(self):

        from sherlock import catalogue_conesearch
        cs = catalogue_conesearch(
            log=log,
            ra="23:01:07.99",
            dec="-01:58:04.5",
            radiusArcsec=60.,
            colMaps=colMaps,
            tableName="tcs_view_agn_milliquas_v4_5",
            dbConn=cataloguesDbConn,
            nearestOnly=False,
            physicalSearch=False
        )
        # catalogueMatches ARE ORDERED BY ANGULAR SEPARATION
        indices, catalogueMatches = cs.search()

        print catalogueMatches

    def test_catalogue_conesearch_function2(self):

        from sherlock import catalogue_conesearch
        cs = catalogue_conesearch(
            log=log,
            ra=["23:01:07.99", 45.36722, 13.875250],
            dec=["-01:58:04.5", 30.45671, -25.26721],
            radiusArcsec=60.,
            colMaps=colMaps,
            tableName="tcs_view_agn_milliquas_v4_5",
            dbConn=cataloguesDbConn,
            nearestOnly=False,
            physicalSearch=False
        )
        # catalogueMatches ARE ORDERED BY ANGULAR SEPARATION
        indices, catalogueMatches = cs.search()

        for i, c in zip(indices, catalogueMatches):
            print i, c

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
        except Exception, e:
            assert True
            print str(e)

        # x-print-testpage-for-pessto-marshall-web-object

    # x-class-to-test-named-worker-function
