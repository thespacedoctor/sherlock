import os
import nose
import shutil
import yaml
from sherlock import conesearcher
from sherlock import database
from sherlock.utKit import utKit

# load settings
stream = file(
    "/Users/Dave/git_repos/sherlock/example_settings.yaml", 'r')
settings = yaml.load(stream)
stream.close()


# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE
moduleDirectory = os.path.dirname(__file__)
utKit = utKit(moduleDirectory)
log, dbConn, pathToInputDir, pathToOutputDir = utKit.setupModule()
utKit.tearDownModule()


# xnose-class-to-test-main-command-line-function-of-module
class test_conesearcher():

    def test_conesearcher_function(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs["settings"] = settings
        # xt-kwarg_key_and_value
        testObject = database(**kwargs)
        transientsDbConn, cataloguesDbConn = testObject.get()

        kwargs["ra"] = 56.23423672301499
        kwargs["dec"] = 0.2221599106133527
        kwargs["tableName"] = "tcs_cat_v_sdss_dr9_spect_qsos"
        kwargs["radius"] = "40.0"
        kwargs["dbConn"] = cataloguesDbConn
        kwargs["settings"] = settings
        kwargs["htmLevel"] = 16
        kwargs["queryType"] = 1
        testObject = conesearcher(**kwargs)
        testObject.get()

        # x-print-testpage-for-pessto-marshall-web-object

    # x-class-to-test-named-worker-function
