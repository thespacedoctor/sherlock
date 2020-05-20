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

from sherlock.commonutils import get_crossmatch_catalogues_column_map
colMaps = get_crossmatch_catalogues_column_map(
    log=log,
    dbConn=cataloguesDbConn
)

transients = [
    {'ps1_designation': u'PS1-14aef',
     'name': u'4L3Piiq',
     'detection_list_id': 2,
     'local_comments': u'',
     'ra': 0.02548233704918263,
     'followup_id': 2065412,
     'dec': -4.284933417540423,
     'id': 1,
     'object_classification': 0
     },

    {'ps1_designation': u'PS1-13dcr',
     'name': u'3I3Phzx',
     'detection_list_id': 2,
     'local_comments': u'',
     'ra': 4.754236999477372,
     'followup_id': 1140386,
     'dec': 28.276703631398625,
     'id': 2,
     'object_classification': 0
     },

    {'ps1_designation': u'PS1-13dhc',
     'name': u'3I3Pixd',
     'detection_list_id': 2,
     'local_comments': u'',
     'ra': 1.3324973428505413,
     'followup_id': 1202386,
     'dec': 32.98869220595689,
     'id': 3,
     'object_classification': 0
     },

    {'ps1_designation': u'faint-sdss-star',
     'name': u'faint-star',
     'detection_list_id': 2,
     'local_comments': u'',
     'ra': 134.74209,
     'followup_id': 1202386,
     'dec': 59.18086,
     'id': 4,
     'object_classification': 0
     },

    {'ps1_designation': u'faint-sdss-galaxy',
     'name': u'faint-star',
     'detection_list_id': 2,
     'local_comments': u'',
     'ra': 134.77235,
     'followup_id': 1202386,
     'dec': 59.20961,
     'id': 5,
     'object_classification': 0
     },

    {'ps1_designation': u'faint-sdss-medium-galaxy',
     'name': u'faint-star',
     'detection_list_id': 2,
     'local_comments': u'',
     'ra': 134.76298,
     'followup_id': 1202386,
     'dec': 59.18248,
     'id': 6,
     'object_classification': 0
     },

    {'ps1_designation': u'faint-sdss-bright-star',
     'name': u'faint-star',
     'detection_list_id': 2,
     'local_comments': u'',
     'ra': 134.81694,
     'followup_id': 1202386,
     'dec': 59.19204,
     'id': 7,
     'object_classification': 0
     },

    {'ps1_designation': u'faint-sdss-medium-star',
     'name': u'faint-star',
     'detection_list_id': 2,
     'local_comments': u'',
     'ra': 134.84064,
     'followup_id': 1202386,
     'dec': 59.21254,
     'id': 8,
     'object_classification': 0
     }
]

# transients = [
#     {'ps1_designation': u'ATLAS17aeu',
#      'name': u'ATLAS17aeu',
#      'detection_list_id': 2,
#      'local_comments': u'',
#      'ra': 138.30789,
#      'followup_id': 1202386L,
#      'dec': +61.09267,
#      'id': 1000519791325919200L,
#      'object_classification': 0L
#      }
# ]

sa = settings["search algorithm"]

class test_transient_catalogue_crossmatch(unittest.TestCase):

    def test_transient_catalogue_crossmatch_function(self):

        from sherlock import transient_catalogue_crossmatch
        this = transient_catalogue_crossmatch(
            log=log,
            dbConn=cataloguesDbConn,
            settings=settings,
            colMaps=colMaps,
            transients=transients
        )
        classifications = this.match()

    def test_transient_catalogue_crossmatch_search_catalogue_function(self):

        # brightnessFilters = ["bright", "faint", "general"]
        # classificationType = ["synonym", "annotation", "association"]
        from sherlock import transient_catalogue_crossmatch
        this = transient_catalogue_crossmatch(
            log=log,
            dbConn=cataloguesDbConn,
            settings=settings,
            colMaps=colMaps,
            transients=transients
        )
        search_name = "ned_d spec galaxy"
        searchPara = sa[search_name]
        print(searchPara)
        matchedObjects = this.angular_crossmatch_against_catalogue(
            objectList=transients,
            searchPara=searchPara,
            search_name=search_name + " angular",
            brightnessFilter="general",
            classificationType="synonym"
        )
        print(matchedObjects)

        matchedObjects = this.angular_crossmatch_against_catalogue(
            objectList=transients,
            searchPara=searchPara,
            search_name=search_name + " angular",
            brightnessFilter="general",
            classificationType="association"
        )
        print(matchedObjects)

        matchedObjects = this.angular_crossmatch_against_catalogue(
            objectList=transients,
            searchPara=searchPara,
            search_name=search_name + " angular",
            brightnessFilter="general",
            classificationType="association"
        )
        print(matchedObjects)

        search_name = "ned phot galaxy"
        searchPara = sa[search_name]
        matchedObjects = this.angular_crossmatch_against_catalogue(
            objectList=transients,
            searchPara=searchPara,
            search_name=search_name + " angular",
            brightnessFilter="general",
            classificationType="association"
        )
        print(matchedObjects)

        search_name = "ned phot galaxy-like"
        searchPara = sa[search_name]
        matchedObjects = this.angular_crossmatch_against_catalogue(
            objectList=transients,
            searchPara=searchPara,
            search_name=search_name + " angular",
            brightnessFilter="general",
            classificationType="annotation"
        )
        print(matchedObjects)

    def test_transient_catalogue_phyiscal_crossmatch_search_catalogue_function(self):

        from sherlock import transient_catalogue_crossmatch
        this = transient_catalogue_crossmatch(
            log=log,
            dbConn=cataloguesDbConn,
            settings=settings,
            colMaps=colMaps,
            transients=transients
        )
        search_name = "ned spec galaxy"
        searchPara = sa[search_name]
        matchedObjects = this.physical_separation_crossmatch_against_catalogue(
            objectList=transients,
            searchPara=searchPara,
            search_name=search_name + " distance",
            brightnessFilter="general",
            classificationType="association"
        )
        print(matchedObjects)

        search_name = "sdss spec galaxy"
        searchPara = sa[search_name]
        matchedObjects = this.physical_separation_crossmatch_against_catalogue(
            objectList=transients,
            searchPara=searchPara,
            search_name=search_name + " distance",
            brightnessFilter="general",
            classificationType="association"
        )
        print(matchedObjects)

    def test_transient_catalogue_faint_mag_search_catalogue_function(self):

        from sherlock import transient_catalogue_crossmatch
        this = transient_catalogue_crossmatch(
            log=log,
            dbConn=cataloguesDbConn,
            settings=settings,
            colMaps=colMaps,
            transients=transients
        )

        search_name = "sdss star"
        searchPara = sa[search_name]
        matchedObjects = this.angular_crossmatch_against_catalogue(
            objectList=transients,
            searchPara=searchPara,
            search_name=search_name + " angular",
            brightnessFilter="faint",
            classificationType="annotation"
        )
        print(matchedObjects)
        print()

        search_name = "2mass star"
        searchPara = sa[search_name]
        matchedObjects = this.angular_crossmatch_against_catalogue(
            objectList=transients,
            searchPara=searchPara,
            search_name=search_name + " angular",
            brightnessFilter="faint",
            classificationType="annotation"
        )
        print(matchedObjects)
        print()

        search_name = "GSC star 1"
        searchPara = sa[search_name]
        matchedObjects = this.angular_crossmatch_against_catalogue(
            objectList=transients,
            searchPara=searchPara,
            search_name=search_name + " angular",
            brightnessFilter="faint",
            classificationType="annotation"
        )
        print(matchedObjects)
        print()

    def test_transient_catalogue_bright_mag_search_catalogue_function(self):

        from sherlock import transient_catalogue_crossmatch
        this = transient_catalogue_crossmatch(
            log=log,
            dbConn=cataloguesDbConn,
            settings=settings,
            colMaps=colMaps,
            transients=transients,
        )

        search_name = "sdss star"
        searchPara = sa[search_name]
        matchedObjects = this.angular_crossmatch_against_catalogue(
            objectList=transients,
            searchPara=searchPara,
            search_name=search_name + " angular",
            brightnessFilter="bright",
            classificationType="association"
        )
        print(matchedObjects)
        print()

        search_name = "2mass star"
        searchPara = sa[search_name]
        matchedObjects = this.angular_crossmatch_against_catalogue(
            objectList=transients,
            searchPara=searchPara,
            search_name=search_name + " angular",
            brightnessFilter="general",
            classificationType="association"
        )
        print(matchedObjects)
        print()

        search_name = "GSC star 1"
        searchPara = sa[search_name]
        matchedObjects = this.angular_crossmatch_against_catalogue(
            objectList=transients,
            searchPara=searchPara,
            search_name=search_name + " angular",
            brightnessFilter="bright",
            classificationType="association"
        )
        print(matchedObjects)
        print()

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
        except Exception as e:
            assert True
            print(str(e))

    # x-class-to-test-named-worker-function
