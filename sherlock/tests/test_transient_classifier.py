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

settings["database settings"]["static catalogues"] = settings[
    "database settings"]["static catalogues2"]

# SETUP ALL DATABASE CONNECTIONS
from sherlock import database
db = database(
    log=log,
    settings=settings
)
dbConns, dbVersions = db.connect()
transientsDbConn = dbConns["transients"]
cataloguesDbConn = dbConns["catalogues"]

from fundamentals.mysql import directory_script_runner
directory_script_runner(
    log=log,
    pathToScriptDirectory=pathToInputDir.replace(
        "/input", "/resources") + "/transient_database",
    dbConn=transientsDbConn
)


class test_transient_classifier(unittest.TestCase):

    def test_transient_update_classified_annotations_function(self):

        from sherlock import transient_classifier
        this = transient_classifier(
            log=log,
            settings=settings,
            update=True
        )
        # this.update_peak_magnitudes()
        this.update_classification_annotations_and_summaries()

    def test_transient_classifier_function(self):

        from sherlock import transient_classifier
        this = transient_classifier(
            log=log,
            settings=settings,
            update=True,
            updateNed=False,
            oneRun=True
        )
        this.classify()

    def test_transient_classifier_single_source_function(self):

        from sherlock import transient_classifier
        this = transient_classifier(
            log=log,
            settings=settings,
            ra="08:57:57.19",
            dec="+43:25:44.1",
            name="PS17gx",
            updateNed=False,
            verbose=0
        )
        classifications, crossmatches = this.classify()

    def test_get_transient_metadata_from_database_list(self):

        from sherlock import transient_classifier
        classifier = transient_classifier(
            log=log,
            settings=settings,
            updateNed=False
        )
        transientsMetadataList = classifier._get_transient_metadata_from_database_list()
        # classifier._update_ned_stream(
        #     transientsMetadataList=transientsMetadataList
        # )

    def test_full_classifier(self):

        from sherlock import transient_classifier
        classifier = transient_classifier(
            log=log,
            settings=settings,
            verbose=2,
            update=True,
            updateNed=False,
            updatePeakMags=True
        )
        classifier.classify()

    def test_classification_annotations(self):

        from sherlock import database
        db = database(
            log=log,
            settings=settings
        )
        dbConns, dbVersions = db.connect()
        transientsDbConn = dbConns["transients"]
        cataloguesDbConn = dbConns["catalogues"]

        from sherlock.commonutils import get_crossmatch_catalogues_column_map
        colMaps = get_crossmatch_catalogues_column_map(
            log=log,
            dbConn=cataloguesDbConn
        )

        from sherlock import transient_classifier
        classifier = transient_classifier(
            log=log,
            settings=settings
        )
        classifier.classification_annotations()

    def test_transient_classifier_function_exception(self):

        from sherlock import transient_classifier
        try:
            this = transient_classifier(
                log=log,
                settings=settings,
                fakeKey="break the code"
            )
            this.get()
            assert False
        except Exception, e:
            assert True
            print(str(e))

    # x-class-to-test-named-worker-function
