import os
import nose
import shutil
import yaml
from sherlock import classifier
from sherlock.utKit import utKit


# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE
moduleDirectory = os.path.dirname(__file__)
utKit = utKit(moduleDirectory)
log, dbConn, pathToInputDir, pathToOutputDir = utKit.setupModule()
utKit.tearDownModule()

# load settings
stream = file(pathToInputDir + "/sherlock.yaml", 'r')
settings = yaml.load(stream)
stream.close()


# xnose-class-to-test-main-command-line-function-of-module

# @action: reset object_classifications to 0 for testing
# update tcs_transient_objects set object_classification = 0 where
# detection_list_id = 2;

class test_classifier():

    def test_classifier_function(self):
        kwargs = {}
        kwargs["log"] = log
        kwargs["settings"] = settings
        kwargs["transientIdList"] = [
            1000006110041705700, 1001901011281636100, 1000519791325919200]
        kwargs["update"] = True
        # xt-kwarg_key_and_value
        testObject = classifier(**kwargs)
        classifications = testObject.get()
        print classifications
        testObject.close()

    # def test_classifier_function2(self):
    #     kwargs = {}
    #     kwargs["log"] = log
    #     kwargs["settings"] = settings
    #     # xt-kwarg_key_and_value
    #     testObject = classifier(**kwargs)
    #     testObject.get()
    #     testObject.close()

    # def test_classifier_function2(self):
    #     kwargs = {}
    #     kwargs["log"] = log
    #     kwargs["settings"] = settings
    #     kwargs["workflowListId"] = "good"
    #     # xt-kwarg_key_and_value
    #     testObject = classifier(**kwargs)
    #     testObject.get()
    #     testObject.close()

        # x-print-testpage-for-pessto-marshall-web-object

    # x-class-to-test-named-worker-function
