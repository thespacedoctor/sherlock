import os
import nose
import shutil
import yaml
from sherlock import database
from sherlock.utKit import utKit

# load settings
stream = file("/Users/Dave/git_repos/sherlock/example_settings.yaml", 'r')
settings = yaml.load(stream)
stream.close()


# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE
moduleDirectory = os.path.dirname(__file__)
utKit = utKit(moduleDirectory)
log, dbConn, pathToInputDir, pathToOutputDir = utKit.setupModule()
utKit.tearDownModule()

# xnose-class-to-test-main-command-line-function-of-module
# xnose-class-to-test-named-worker-function


class test_database():

    def test_database_function(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs["settings"] = settings
        # xt-kwarg_key_and_value
        testObject = database(**kwargs)
        testObject.get()
        testObject.close()
