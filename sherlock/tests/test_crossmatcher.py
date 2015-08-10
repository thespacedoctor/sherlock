import os
import nose
import shutil
import yaml
from sherlock import crossmatcher
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


class test_crossmatcher():

    def test_crossmatcher_function(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs["settings"] = settings
        # xt-kwarg_key_and_value
        testObject = database(**kwargs)
        transientsDbConn, cataloguesDbConn = testObject.get()

        kwargs = {}
        kwargs["log"] = log
        kwargs["dbConn"] = cataloguesDbConn
        kwargs["settings"] = settings
        kwargs["transients"] = [{'ps1_designation': u'PS1-14aef', 'name': u'4L3Piiq', 'detection_list_id': 2, 'local_comments': u'', 'ra': 0.02548233704918263, 'followup_id': 2065412L, 'dec': -4.284933417540423, 'id': 1000006110041705700L, 'object_classification': 0L}, {'ps1_designation': u'PS1-13dcr', 'name': u'3I3Phzx', 'detection_list_id': 2, 'local_comments': u'', 'ra': 4.754236999477372, 'followup_id': 1140386L, 'dec': 28.276703631398625, 'id': 1001901011281636100L, 'object_classification': 0L}, {'ps1_designation': u'PS1-13dhc', 'name': u'3I3Pixd', 'detection_list_id': 2, 'local_comments': u'', 'ra': 1.3324973428505413, 'followup_id': 1202386L, 'dec': 32.98869220595689, 'id': 1000519791325919200L, 'object_classification': 0L}]
        # xt-kwarg_key_and_value

        testObject = crossmatcher(**kwargs)
        testObject.get()

        # x-print-testpage-for-pessto-marshall-web-object

    # x-class-to-test-named-worker-function
