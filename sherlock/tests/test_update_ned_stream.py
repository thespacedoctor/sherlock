import os
import nose
import shutil
import yaml
from sherlock import update_ned_stream, cl_utils
from sherlock.utKit import utKit

from dryxPython.projectsetup import setup_main_clutil

su = setup_main_clutil(
    arguments={},
    docString=__doc__,
    logLevel="DEBUG",
    options_first=False,
    projectName="sherlock"
)
arguments, settings, log, dbConn = su.setup()

# load settings
stream = file(
    "/Users/Dave/.config/sherlock/sherlock.yaml", 'r')
settings = yaml.load(stream)
stream.close()

# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE
moduleDirectory = os.path.dirname(__file__)
utKit = utKit(moduleDirectory)
log, dbConn, pathToInputDir, pathToOutputDir = utKit.setupModule()
utKit.tearDownModule()


class test_update_ned_stream():

    def test_update_ned_stream_function(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs["settings"] = settings
        # xt-kwarg_key_and_value

        testObject = update_ned_stream(**kwargs)
        testObject.get()

        # x-print-testpage-for-pessto-marshall-web-object

    # x-class-to-test-named-worker-function
