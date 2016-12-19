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


class test_get_crossmatch_catalogues_column_map():

    def test_get_crossmatch_catalogues_column_map_function(self):

        from sherlock.commonutils import get_crossmatch_catalogues_column_map
        colMaps = get_crossmatch_catalogues_column_map(
            log=log,
            dbConn=dbConn
        )
        print colMaps

    def test_get_crossmatch_catalogues_column_map_function_exception(self):

        from sherlock.commonutils import get_crossmatch_catalogues_column_map
        try:
            this = get_crossmatch_catalogues_column_map(
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
