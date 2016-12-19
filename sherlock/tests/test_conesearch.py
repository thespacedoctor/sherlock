import os
import nose
import shutil
import yaml
from sherlock.utKit import utKit
from fundamentals import tools

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
    pathToInputDir + "/example_settings.yaml", 'r')
settings = yaml.load(stream)
stream.close()

su = tools(
    arguments={"settingsFile": pathToInputDir + "/example_settings.yaml"},
    docString=__doc__,
    logLevel="DEBUG",
    options_first=False,
    projectName="sherlock"
)
arguments, settings, log, dbConn = su.setup()

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

from sherlock.commonutils import get_crossmatch_catalogues_column_map
colMaps = get_crossmatch_catalogues_column_map(
    log=log,
    dbConn=dbConn
)

for k, v in colMaps.iteritems():
    print k


class test_conesearch():

    def test_conesearch_function(self):

        from sherlock import conesearch
        cs = conesearch(
            log=log,
            settings=settings,
            ra=56.23423672301499,
            dec=0.2221599106133527,
            tableName="tcs_view_galaxy_sdss_photo_stars_galaxies_dr12",
            radius="40.0",
            colMaps=colMaps,
            dbConn=dbConn,
            nearestOnly=False,
            physicalSearch=False,
            transType=False)
        cs.match()

    def test_conesearch_function_02(self):

        from sherlock import conesearch
        cs = conesearch(
            log=log,
            settings=settings,
            ra=56.23423672301499,
            dec=0.2221599106133527,
            tableName="tcs_view_galaxy_sdss_photo_stars_galaxies_dr12",
            radius="40.0",
            colMaps=colMaps,
            dbConn=dbConn,
            nearestOnly=False,
            physicalSearch=True,
            transType=False)
        cs.match()

    def test_conesearch_function_03(self):

        from sherlock import conesearch
        cs = conesearch(
            log=log,
            settings=settings,
            ra=56.23423672301499,
            dec=0.2221599106133527,
            tableName="tcs_view_galaxy_sdss_photo_stars_galaxies_dr12",
            radius="40.0",
            colMaps=colMaps,
            dbConn=dbConn,
            nearestOnly=False,
            physicalSearch=True,
            transType="SN")
        cs.match()

    def test_conesearch_function_04(self):

        from sherlock import conesearch
        cs = conesearch(
            log=log,
            settings=settings,
            ra=56.23423672301499,
            dec=0.2221599106133527,
            tableName="tcs_view_galaxy_sdss_photo_stars_galaxies_dr12",
            radius="40.0",
            colMaps=colMaps,
            dbConn=dbConn,
            nearestOnly=True,
            physicalSearch=True,
            transType=False)
        cs.match()

    def test_conesearch_function_exception(self):

        from sherlock import conesearch
        try:
            cs = conesearch(
                log=log,
                settings=settings,
                fakeKey="break the code"
            )
            cs.match()
            assert False
        except Exception, e:
            assert True
            print str(e)

        # x-print-testpage-for-pessto-marshall-web-object

    # x-class-to-test-named-worker-function
