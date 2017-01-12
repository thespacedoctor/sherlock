import os
import nose
import shutil
import yaml
from sherlock.utKit import utKit
from fundamentals import tools


# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE
moduleDirectory = os.path.dirname(__file__)
utKit = utKit(moduleDirectory)
log, dbConn, pathToInputDir, pathToOutputDir = utKit.setupModule()
utKit.tearDownModule()

# load settings
stream = file(
    pathToInputDir + "/example_settings.yaml",
    'r')
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

transients = [
    {'ps1_designation': u'PS1-14aef',
     'name': u'4L3Piiq',
     'detection_list_id': 2,
     'local_comments': u'',
     'ra': 0.02548233704918263,
     'followup_id': 2065412L,
     'dec': -4.284933417540423,
     'id': 1000006110041705700L,
     'object_classification': 0L
     },

    {'ps1_designation': u'PS1-13dcr',
     'name': u'3I3Phzx',
     'detection_list_id': 2,
     'local_comments': u'',
     'ra': 4.754236999477372,
     'followup_id': 1140386L,
     'dec': 28.276703631398625,
     'id': 1001901011281636100L,
     'object_classification': 0L
     },

    {'ps1_designation': u'PS1-13dhc',
     'name': u'3I3Pixd',
     'detection_list_id': 2,
     'local_comments': u'',
     'ra': 1.3324973428505413,
     'followup_id': 1202386L,
     'dec': 32.98869220595689,
     'id': 1000519791325919200L,
     'object_classification': 0L
     }
]

transients = [
    {'ps1_designation': u'ATLAS17aeu',
     'name': u'ATLAS17aeu',
     'detection_list_id': 2,
     'local_comments': u'',
     'ra': 138.30789,
     'followup_id': 1202386L,
     'dec': +61.09267,
     'id': 1000519791325919200L,
     'object_classification': 0L
     }
]


sa = settings["search algorithm"]


class test_transient_catalogue_crossmatch():

    def test_transient_catalogue_crossmatch_function(self):

        from sherlock import transient_catalogue_crossmatch
        this = transient_catalogue_crossmatch(
            log=log,
            settings=settings,
            dbConn=dbConn,
            colMaps=colMaps,
            transients=transients
        )
        classifications = this.match()
        for c in classifications:

            print c["id"], c["object_classification_new"]
            print c["crossmatches"]

    def test_transient_catalogue_crossmatch_search_catalogue_function(self):

        from sherlock import transient_catalogue_crossmatch
        this = transient_catalogue_crossmatch(
            log=log,
            settings=settings,
            dbConn=dbConn,
            colMaps=colMaps,
            transients=transients
        )
        search_name = "ned_d spec sn"
        searchPara = sa[search_name]
        matchedObjects = this.angular_crossmatch_against_catalogue(
            objectList=transients,
            searchPara=searchPara,
            search_name=search_name
        )
        print matchedObjects

        search_name = "ned phot sn"
        searchPara = sa[search_name]
        matchedObjects = this.angular_crossmatch_against_catalogue(
            objectList=transients,
            searchPara=searchPara,
            search_name=search_name
        )
        print matchedObjects

        search_name = "ned phot other"
        searchPara = sa[search_name]
        matchedObjects = this.angular_crossmatch_against_catalogue(
            objectList=transients,
            searchPara=searchPara,
            search_name=search_name
        )
        print matchedObjects

    def test_transient_catalogue_phyiscal_crossmatch_search_catalogue_function(self):

        from sherlock import transient_catalogue_crossmatch
        this = transient_catalogue_crossmatch(
            log=log,
            settings=settings,
            dbConn=dbConn,
            colMaps=colMaps,
            transients=transients
        )
        search_name = "ned spec sn"
        searchPara = sa[search_name]
        matchedObjects = this.physical_separation_crossmatch_against_catalogue(
            objectList=transients,
            searchPara=searchPara,
            search_name=search_name
        )
        print matchedObjects

        search_name = "sdss spec sn"
        searchPara = sa[search_name]
        matchedObjects = this.physical_separation_crossmatch_against_catalogue(
            objectList=transients,
            searchPara=searchPara,
            search_name=search_name
        )
        print matchedObjects

    def test_transient_catalogue_crossmatch_function_exception(self):

        from sherlock import transient_catalogue_crossmatch
        try:
            this = transient_catalogue_crossmatch(
                log=log,
                settings=settings,
                fakeKey="break the code"
            )
            this.match()
            assert False
        except Exception, e:
            assert True
            print str(e)

        # x-print-testpage-for-pessto-marshall-web-object

    # x-class-to-test-named-worker-function
