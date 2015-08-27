import os
import nose
import shutil
import yaml
from .. import milliquas
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


class test_milliquas():

    def test_milliquas_function(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs["dbConn"] = dbConn
        kwargs["settings"] = settings
        kwargs["pathToDataFile"] = "/Users/Dave/Desktop/milliquas.txt"
        # xt-kwarg_key_and_value

        testObject = milliquas(**kwargs)
        testObject.get()

        # x-print-testpage-for-pessto-marshall-web-object

    # x-class-to-test-named-worker-function
