#!/usr/local/bin/python
# encoding: utf-8
"""
*crossmatch a list of transients against a suite of catalogues according to given search algorithm*

:Author:
    David Young

:Date Created:
    December 19, 2016
"""
################# GLOBAL IMPORTS ####################
import sys
import os
import math
os.environ['TERM'] = 'vt100'
from fundamentals import tools
from sherlock.catalogue_conesearch import catalogue_conesearch
from astrocalc.distances import converter
from astrocalc.coords import separations
import time


class transient_catalogue_crossmatch():
    """
    *crossmatch a list of transients against a suite of catalogues according to given search algorithm*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection for the catalogues
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary
        - ``colMaps`` -- maps of the important column names for each table/view in the crossmatch-catalogues database
        - ``transients`` -- the list of transients

    **Usage:**

        To setup your logger, settings and database connections, please use the ``fundamentals`` package (`see tutorial here <http://fundamentals.readthedocs.io/en/latest/#tutorial>`_). 

        To initiate a transient_catalogue_crossmatch object, use the following:

        .. code-block:: python 

            from sherlock import transient_catalogue_crossmatch
            xmatcher = transient_catalogue_crossmatch(
                log=log,
                settings=settings,
                dbConn=dbConn,
                colMaps=colMaps,
                transients=transients
            )

        Then to run the transient through the search algorithm found in the settings file, use the ``match`` method:

        .. code-block:: python 

            classifications = xmatcher.match()

    .. todo ::

        - update key arguments values and definitions with defaults
        - update return values and definitions
        - update usage examples and text
        - update docstring text
        - check sublime snippet exists
        - clip any useful text to docs mindmap
        - regenerate the docs and check redendering of this docstring
    """
    # Initialisation

    def __init__(
            self,
            log,
            dbConn=False,
            settings=False,
            colMaps=False,
            transients=[]
    ):
        self.log = log
        log.debug("instansiating a new 'transient_catalogue_crossmatch' object")
        self.dbConn = dbConn
        self.settings = settings
        self.transients = transients
        self.colMaps = colMaps

        # xt-self-arg-tmpx
        return None

    def match(self):
        """
        *match the transients against the sherlock-catalogues according to the search algorithm and return matches alongside the predicted classification(s)*

        **Return:**
            - ``classification`` -- the crossmatch results and classifications assigned to the transients

        See the class docstring for usage.

        .. todo ::

            - update key arguments values and definitions with defaults
            - update return values and definitions
            - update usage examples and text
            - update docstring text
            - check sublime snippet exists
            - clip any useful text to docs mindmap
            - regenerate the docs and check redendering of this docstring
        """
        self.log.debug('starting the ``match`` method')

        classifications = []

        # COUNT NUMBER OF TRANSIENT TO CROSSMATCH
        numberOfTransients = len(self.transients)
        count = 0

        # GRAB SEARCH ALGORITHM
        sa = self.settings["search algorithm"]

        # FOR EACH TRANSIENT SOURCE IN THE LIST ...
        allCatalogueMatches = []
        catalogueMatches = []
        nonSynonymTransients = self.transients[:]

        # SYNONYM SEARCHES
        # ITERATE THROUGH SEARCH ALGORITHM IN ORDER
        # PRESENTED IN THE SETTINGS FILE
        brightnessFilters = ["bright", "faint", "general"]
        for search_name, searchPara in sa.iteritems():
            for bf in brightnessFilters:
                if bf not in searchPara:
                    continue
                if "synonym" not in searchPara[bf] or searchPara[bf]["synonym"] == False:
                    continue
                self.log.debug("""  searching: %(search_name)s""" % locals())
                if "physical radius kpc" in searchPara[bf]:
                    # THE PHYSICAL SEPARATION SEARCHES
                    self.log.debug(
                        'checking physical distance crossmatches in %(search_name)s' % locals())
                    catalogueMatches = self.physical_separation_crossmatch_against_catalogue(
                        objectList=self.transients,
                        searchPara=searchPara,
                        search_name=search_name + " physical",
                        brightnessFilter=bf,
                        classificationType="synonym"
                    )
                else:
                    # THE ANGULAR SEPARATION SEARCHES
                    self.log.debug(
                        'Crossmatching against %(search_name)s' % locals())
                    # RENAMED from searchCatalogue
                    catalogueMatches = self.angular_crossmatch_against_catalogue(
                        objectList=self.transients,
                        searchPara=searchPara,
                        search_name=search_name + " angular",
                        brightnessFilter=bf,
                        classificationType="synonym"
                    )

                # ADD CLASSIFICATION AND CROSSMATCHES IF FOUND
                if catalogueMatches:
                    allCatalogueMatches = allCatalogueMatches + catalogueMatches

        synonymIDs = []
        synonymIDs[:] = [xm["transient_object_id"] for xm in catalogueMatches]
        nonSynonymTransients = []
        nonSynonymTransients[:] = [
            t for t in self.transients if t["id"] not in synonymIDs]

        # ASSOCIATION SEARCHES
        # ITERATE THROUGH SEARCH ALGORITHM IN ORDER
        # PRESENTED IN THE SETTINGS FILE
        for search_name, searchPara in sa.iteritems():
            self.log.debug("""  searching: %(search_name)s""" % locals())
            for bf in brightnessFilters:
                if bf not in searchPara:
                    continue
                if "association" not in searchPara[bf] or searchPara[bf]["association"] == False:
                    continue
                if "physical radius kpc" in searchPara[bf]:

                    # THE PHYSICAL SEPARATION SEARCHES
                    self.log.debug(
                        'checking physical distance crossmatches in %(search_name)s' % locals())
                    catalogueMatches = self.physical_separation_crossmatch_against_catalogue(
                        objectList=nonSynonymTransients,
                        searchPara=searchPara,
                        search_name=search_name + " physical",
                        brightnessFilter=bf,
                        classificationType="association"
                    )
                else:
                    # THE ANGULAR SEPARATION SEARCHES
                    self.log.debug(
                        'Crossmatching against %(search_name)s' % locals())

                    # RENAMED from searchCatalogue
                    catalogueMatches = self.angular_crossmatch_against_catalogue(
                        objectList=nonSynonymTransients,
                        searchPara=searchPara,
                        search_name=search_name + " angular",
                        brightnessFilter=bf,
                        classificationType="association"
                    )

                # ADD CLASSIFICATION AND CROSSMATCHES IF FOUND
                if catalogueMatches:
                    allCatalogueMatches = allCatalogueMatches + catalogueMatches
                    catalogueMatches = []

        # ANNOTATION SEARCHES
        # ITERATE THROUGH SEARCH ALGORITHM IN ORDER
        # PRESENTED IN THE SETTINGS FILE
        brightnessFilters = ["bright", "faint", "general"]
        for search_name, searchPara in sa.iteritems():
            for bf in brightnessFilters:
                if bf not in searchPara:
                    continue
                if "annotation" not in searchPara[bf] or searchPara[bf]["annotation"] == False:
                    continue
                self.log.debug("""  searching: %(search_name)s""" % locals())
                if "physical radius kpc" in searchPara[bf]:
                    # THE PHYSICAL SEPARATION SEARCHES
                    self.log.debug(
                        'checking physical distance crossmatches in %(search_name)s' % locals())
                    if bf in searchPara:
                        catalogueMatches = self.physical_separation_crossmatch_against_catalogue(
                            objectList=self.transients,
                            searchPara=searchPara,
                            search_name=search_name + " physical",
                            brightnessFilter=bf,
                            classificationType="annotation"
                        )
                else:
                    # THE ANGULAR SEPARATION SEARCHES
                    self.log.debug(
                        'Crossmatching against %(search_name)s' % locals())
                    # RENAMED from searchCatalogue
                    if bf in searchPara:
                        catalogueMatches = self.angular_crossmatch_against_catalogue(
                            objectList=self.transients,
                            searchPara=searchPara,
                            search_name=search_name + " angular",
                            brightnessFilter=bf,
                            classificationType="annotation"
                        )

                # ADD CLASSIFICATION AND CROSSMATCHES IF FOUND
                if catalogueMatches:
                    allCatalogueMatches = allCatalogueMatches + catalogueMatches

        self.log.debug('completed the ``match`` method')
        return allCatalogueMatches

    def angular_crossmatch_against_catalogue(
        self,
        objectList,
        searchPara={},
        search_name="",
        brightnessFilter=False,
        physicalSearch=False,
        classificationType=False
    ):
        """*perform an angular separation crossmatch against a given catalogue in the database and annotate the crossmatch with some value added parameters (distances, physical separations, sub-type of transient etc)*

        **Key Arguments:**
            - ``objectList`` -- the list of transient locations to match against the crossmatch catalogue
            - ``searchPara`` -- the search parameters for this individual search as lifted from the search algorithm in the sherlock settings file
            - ``search_name`` -- the name of the search as given in the sherlock settings file
            - ``brightnessFilter`` -- is this search to be constrained by magnitude of the catalogue sources? Default *False*. [bright|faint|general]
            - ``physicalSearch`` -- is this angular search a sub-part of a physical separation search
            - ``classificationType`` -- synonym, association or annotation. Default *False*

         **Return:**
            - matchedObjects -- any sources matched against the object

        **Usage:**

            Take a list of transients from somewhere

            .. code-block:: python 


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

            Then run the ``angular_crossmatch_against_catalogue`` method to crossmatch against the catalogues and return results:

            .. code-block:: python 

                # ANGULAR CONESEARCH ON CATALOGUE
                search_name = "ned_d spec sn"
                searchPara = self.settings["search algorithm"][search_name]
                matchedObjects = xmatcher.angular_crossmatch_against_catalogue(
                    objectList=transients,
                    searchPara=searchPara,
                    search_name=search_name
                )

        .. todo ::

            - update key arguments values and definitions with defaults
            - update return values and definitions
            - update usage examples and text
            - update docstring text
            - check sublime snippet exists
            - clip any useful text to docs mindmap
            - regenerate the docs and check redendering of this docstring
        """
        self.log.debug(
            'starting the ``angular_crossmatch_against_catalogue`` method')

        self.log.info("STARTING %s SEARCH" %
                      (search_name,))

        start_time = time.time()

        # DEFAULTS

        # print search_name, classificationType

        magnitudeLimitFilter = None
        upperMagnitudeLimit = False
        lowerMagnitudeLimit = False

        catalogueName = searchPara["database table"]

        if not "mag column" in searchPara:
            searchPara["mag column"] = None

        if brightnessFilter:
            if "mag column" in searchPara and searchPara["mag column"]:
                magnitudeLimitFilter = self.colMaps[
                    catalogueName][searchPara["mag column"] + "ColName"]
            theseSearchPara = searchPara[brightnessFilter]
        else:
            theseSearchPara = searchPara

        # EXTRACT PARAMETERS FROM ARGUMENTS & SETTINGS FILE
        if classificationType == "synonym":
            radius = self.settings["synonym radius arcsec"]
            matchedType = theseSearchPara["synonym"]
        elif classificationType == "association":
            radius = theseSearchPara["angular radius arcsec"]
            matchedType = theseSearchPara["association"]
        elif classificationType == "annotation":
            radius = theseSearchPara["angular radius arcsec"]
            matchedType = theseSearchPara["annotation"]

        if brightnessFilter == "faint":
            upperMagnitudeLimit = theseSearchPara["mag limit"]
        elif brightnessFilter == "bright":
            lowerMagnitudeLimit = theseSearchPara["mag limit"]
        elif brightnessFilter == "general":
            if "faint" in searchPara:
                lowerMagnitudeLimit = searchPara["faint"]["mag limit"]
            if "bright" in searchPara:
                upperMagnitudeLimit = searchPara["bright"]["mag limit"]

        # VARIABLES
        matchedObjects = []
        matchSubset = []

        transRAs = []
        transRAs[:] = [t['ra'] for t in objectList]
        transDecs = []
        transDecs[:] = [t['dec'] for t in objectList]

        if len(transRAs) == 0:
            return []

        cs = catalogue_conesearch(
            log=self.log,
            ra=transRAs,
            dec=transDecs,
            radiusArcsec=radius,
            colMaps=self.colMaps,
            tableName=catalogueName,
            dbConn=self.dbConn,
            nearestOnly=False,
            physicalSearch=physicalSearch,
            upperMagnitudeLimit=upperMagnitudeLimit,
            lowerMagnitudeLimit=lowerMagnitudeLimit,
            magnitudeLimitFilter=magnitudeLimitFilter
        )
        # catalogueMatches ARE ORDERED BY ANGULAR SEPARATION
        indices, catalogueMatches = cs.search()
        count = 1
        annotatedcatalogueMatches = []
        for i, xm in zip(indices, catalogueMatches):

            # CALCULATE PHYSICAL PARAMETERS ... IF WE CAN
            if "cmSepArcsec" in xm:
                xm["separationArcsec"] = xm["cmSepArcsec"]
                # CALCULATE SEPARATION IN ARCSEC

                calculator = separations(
                    log=self.log,
                    ra1=objectList[i]["ra"],
                    dec1=objectList[i]["dec"],
                    ra2=xm["ra"],
                    dec2=xm["dec"]
                )
                angularSeparation, north, east = calculator.get()

                xm["northSeparationArcsec"] = north
                xm["eastSeparationArcsec"] = east
                del xm["cmSepArcsec"]

            xm["association_type"] = matchedType
            xm["catalogue_view_name"] = catalogueName
            xm["transient_object_id"] = objectList[i]["id"]
            xm["catalogue_table_name"] = self.colMaps[
                catalogueName]["description"]
            xm["catalogue_table_id"] = self.colMaps[
                catalogueName]["table_id"]
            xm["catalogue_view_id"] = self.colMaps[
                catalogueName]["id"]
            if classificationType == "synonym":
                xm["classificationReliability"] = 1
            elif classificationType == "association":
                xm["classificationReliability"] = 2
            elif classificationType == "annotation":
                xm["classificationReliability"] = 3

            xm = self._annotate_crossmatch_with_value_added_parameters(
                crossmatchDict=xm,
                catalogueName=catalogueName,
                searchPara=theseSearchPara,
                search_name=search_name
            )
            annotatedcatalogueMatches.append(xm)

        catalogueMatches = annotatedcatalogueMatches

        # IF BRIGHT STAR SEARCH
        if brightnessFilter == "bright" and "star" in search_name:
            catalogueMatches = self._bright_star_match(
                matchedObjects=catalogueMatches,
                catalogueName=catalogueName,
                lowerMagnitudeLimit=lowerMagnitudeLimit,
                magnitudeLimitFilter=searchPara["mag column"]
            )

        if brightnessFilter == "general" and "galaxy" in search_name and "galaxy-like" not in search_name and "physical radius kpc" not in theseSearchPara:
            catalogueMatches = self._galaxy_association_cuts(
                matchedObjects=catalogueMatches,
                catalogueName=catalogueName,
                lowerMagnitudeLimit=lowerMagnitudeLimit,
                upperMagnitudeLimit=upperMagnitudeLimit,
                magnitudeLimitFilter=searchPara["mag column"]
            )

        if "match nearest source only" in theseSearchPara and theseSearchPara["match nearest source only"] == True and len(catalogueMatches):
            nearestMatches = []
            transList = []
            for c in catalogueMatches:
                if c["transient_object_id"] not in transList:
                    transList.append(c["transient_object_id"])
                    nearestMatches.append(c)
            catalogueMatches = nearestMatches

        self.log.debug(
            'completed the ``angular_crossmatch_against_catalogue`` method')

        self.log.info("FINISHED %s SEARCH IN %0.5f s" %
                      (search_name, time.time() - start_time,))

        return catalogueMatches

    def _annotate_crossmatch_with_value_added_parameters(
            self,
            crossmatchDict,
            catalogueName,
            searchPara,
            search_name):
        """*annotate each crossmatch with physical parameters such are distances etc*

        **Key Arguments:**
            - ``crossmatchDict`` -- the crossmatch dictionary
            - ``catalogueName`` -- the name of the catalogue the crossmatch results from
            - ``searchPara`` -- the search parameters for this individual search as lifted from the search algorithm in the sherlock settings file
            - ``search_name`` -- the name of the search as given in the sherlock settings file

        **Return:**
            - ``crossmatchDict`` -- the annotated crossmatch dictionary

        .. todo ::

            - update key arguments values and definitions with defaults
            - update return values and definitions
            - update usage examples and text
            - update docstring text
            - check sublime snippet exists
            - clip any useful text to docs mindmap
            - regenerate the docs and check redendering of this docstring
        """
        self.log.debug(
            'starting the ``_annotate_crossmatch_with_value_added_parameters`` method')

        redshift = None
        z = None
        scale = None
        distance = None
        distance_modulus = None
        major_axis_arcsec = None
        direct_distance = None
        direct_distance_scale = None
        direct_distance_modulus = None

        # IF THERE'S A REDSHIFT, CALCULATE PHYSICAL PARAMETERS
        if 'z' in crossmatchDict:
            # THE CATALOGUE HAS A REDSHIFT COLUMN
            redshift = crossmatchDict['z']
        elif 'photoZ' in crossmatchDict:
            redshift = crossmatchDict['photoZ']
        if redshift and redshift > 0.0:
            # CALCULATE DISTANCE MODULUS, ETC
            c = converter(log=self.log)
            dists = c.redshift_to_distance(
                z=redshift,
                WM=0.3,
                WV=0.7,
                H0=70.0
            )

            if dists:
                z = dists['z']
                scale = dists["da_scale"]
                distance = dists["dl_mpc"]
                distance_modulus = dists["dmod"]
        # ADD MAJOR AXIS VALUE
        if "or within semi major axis" in searchPara and searchPara["or within semi major axis"] == True and "semiMajor" in crossmatchDict and crossmatchDict["semiMajor"]:
            major_axis_arcsec = crossmatchDict[
                "semiMajor"] * self.colMaps[catalogueName]["semiMajorToArcsec"]

        if "semiMajor" in crossmatchDict:
            del crossmatchDict["semiMajor"]
        # ADD DISTANCE VALUES
        if "distance" in crossmatchDict and crossmatchDict["distance"]:
            direct_distance = crossmatchDict["distance"]
            direct_distance_scale = direct_distance / 206.264806
            direct_distance_modulus = 5 * \
                math.log10(direct_distance * 1e6) - 5
        # crossmatchDict['z'] = z
        crossmatchDict['scale'] = scale
        crossmatchDict['distance'] = distance
        crossmatchDict['distance_modulus'] = distance_modulus
        crossmatchDict['major_axis_arcsec'] = major_axis_arcsec
        crossmatchDict['direct_distance'] = direct_distance
        crossmatchDict['direct_distance_scale'] = direct_distance_scale
        crossmatchDict['direct_distance_modulus'] = direct_distance_modulus

        crossmatchDict['catalogue_object_type'] = self.colMaps[
            catalogueName]["object_type"]
        crossmatchDict["search_name"] = search_name
        crossmatchDict["raDeg"] = crossmatchDict["ra"]

        crossmatchDict["decDeg"] = crossmatchDict["dec"]
        del crossmatchDict["ra"]
        del crossmatchDict["dec"]
        crossmatchDict["original_search_radius_arcsec"] = searchPara[
            "angular radius arcsec"]

        physical_separation_kpc = None
        # CALCULATE MOST ACCURATE PHYSICAL SEPARATION
        if crossmatchDict["direct_distance_scale"]:
            physical_separation_kpc = crossmatchDict[
                "direct_distance_scale"] * crossmatchDict["separationArcsec"]
        elif crossmatchDict["scale"]:
            physical_separation_kpc = crossmatchDict[
                "scale"] * crossmatchDict["separationArcsec"]

        crossmatchDict["physical_separation_kpc"] = physical_separation_kpc

        self.log.debug(
            'completed the ``_annotate_crossmatch_with_value_added_parameters`` method')
        return crossmatchDict

    def _bright_star_match(
            self,
            matchedObjects,
            catalogueName,
            magnitudeLimitFilter,
            lowerMagnitudeLimit):
        """*perform a bright star match on the crossmatch results if required by the catalogue search*

        **Key Arguments:**
            - ``matchedObjects`` -- the list of matched sources from the catalogue crossmatch
            - ``catalogueName`` -- the name of the catalogue the crossmatch results from
            - ``magnitudeLimitFilter`` -- the name of the column containing the magnitude to filter on
            - ``lowerMagnitudeLimit`` -- the lower magnitude limit to match bright stars against

        **Return:**
            - ``brightStarMatches`` -- the trimmed matched sources (bright stars associations only)

        .. todo ::

            - update key arguments values and definitions with defaults
            - update return values and definitions
            - update usage examples and text
            - update docstring text
            - check sublime snippet exists
            - clip any useful text to docs mindmap
            - regenerate the docs and check redendering of this docstring
        """
        self.log.debug('starting the ``_bright_star_match`` method')

        import decimal
        decimal.getcontext().prec = 10

        # MATCH BRIGHT STAR ASSOCIATIONS
        brightStarMatches = []
        for row in matchedObjects:
            mag = decimal.Decimal(row[magnitudeLimitFilter])
            if mag and mag < lowerMagnitudeLimit:
                sep = decimal.Decimal(row["separationArcsec"])
                if sep < decimal.Decimal(decimal.Decimal(10)**(-decimal.Decimal(0.2) * mag + decimal.Decimal(3.7))) and sep < 20.:
                    brightStarMatches.append(row)

        self.log.debug('completed the ``_bright_star_match`` method')
        return brightStarMatches

    def _galaxy_association_cuts(
            self,
            matchedObjects,
            catalogueName,
            magnitudeLimitFilter,
            upperMagnitudeLimit,
            lowerMagnitudeLimit):
        """*perform a bright star match on the crossmatch results if required by the catalogue search*

        **Key Arguments:**
            - ``matchedObjects`` -- the list of matched sources from the catalogue crossmatch
            - ``catalogueName`` -- the name of the catalogue the crossmatch results from
            - ``magnitudeLimitFilter`` -- the name of the column containing the magnitude to filter on
            - ``lowerMagnitudeLimit`` -- the lower magnitude limit to match general galaxies against
            - ``upperMagnitudeLimit`` -- the upper magnitude limit to match general galaxies against

        **Return:**
            - ``galaxyMatches`` -- the trimmed matched sources (associated galaxies only)

        .. todo ::

            - update key arguments values and definitions with defaults
            - update return values and definitions
            - update usage examples and text
            - update docstring text
            - check sublime snippet exists
            - clip any useful text to docs mindmap
            - regenerate the docs and check redendering of this docstring
        """
        self.log.debug('starting the ``_galaxy_association_cuts`` method')

        import decimal
        decimal.getcontext().prec = 10

        # MATCH BRIGHT STAR ASSOCIATIONS
        galaxyMatches = []
        for row in matchedObjects:
            if not magnitudeLimitFilter or row[magnitudeLimitFilter] == None:
                galaxyMatches.append(row)
            else:
                mag = decimal.Decimal(row[magnitudeLimitFilter])
                if mag and mag < lowerMagnitudeLimit and mag > upperMagnitudeLimit:
                    sep = decimal.Decimal(row["separationArcsec"])
                    if sep < decimal.Decimal(decimal.Decimal(10)**(decimal.Decimal((decimal.Decimal(25.) - mag) / decimal.Decimal(6.)))):
                        galaxyMatches.append(row)

        self.log.debug('completed the ``_galaxy_association_cuts`` method')
        return galaxyMatches

    def physical_separation_crossmatch_against_catalogue(
        self,
        objectList,
        searchPara,
        search_name,
        brightnessFilter=False,
        classificationType=False
    ):
        """*perform an physical separation crossmatch against a given catalogue in the database*

        This search is basically the same as the angular separation search except extra filtering is done to exclude sources outside the physical search radius (matched sources require distance info to calulate physical separations)

        **Key Arguments:**
            - ``objectList`` -- transients to be crossmatched
            - ``searchPara`` -- parameters of the search (from settings file)
            - ``search_name`` -- the name of the search
            - ``brightnessFilter`` -- is this search to be constrained by magnitude of the catalogue sources? Default *False*. [bright|faint|general]
            - ``classificationType`` -- synonym, association or annotation. Default *False*

        **Return:**
            - matchedObjects -- any sources matched against the object

        To run a physical separation crossmatch, run in a similar way to the angular separation crossmatch:

        **Usage:**

            .. code-block:: python 

                search_name = "ned spec sn"
                searchPara = self.settings["search algorithm"][search_name]
                matchedObjects = xmatcher.physical_separation_crossmatch_against_catalogue(
                    objectList=transients,
                    searchPara=searchPara,
                    search_name=search_name
                )

        .. todo ::

            - update key arguments values and definitions with defaults
            - update return values and definitions
            - update usage examples and text
            - update docstring text
            - check sublime snippet exists
            - clip any useful text to docs mindmap
            - regenerate the docs and check redendering of this docstring
        """
        self.log.debug(
            'starting the ``physical_separation_crossmatch_against_catalogue`` method')

        start_time = time.time()

        bf = brightnessFilter
        # SETUP PARAMETERS
        tableName = searchPara["database table"]
        if bf:
            angularRadius = searchPara[bf]["angular radius arcsec"]
            physicalRadius = searchPara[bf]["physical radius kpc"]
            matchedType = searchPara[bf][classificationType]
            if "match nearest source only" in searchPara[bf]:
                nearestOnly = searchPara[bf]["match nearest source only"]
            else:
                nearestOnly = False
        else:
            angularRadius = searchPara["angular radius arcsec"]
            physicalRadius = searchPara["physical radius kpc"]
            matchedType = searchPara[classificationType]
            if "match nearest source only" in searchPara:
                nearestOnly = searchPara["match nearest source only"]
            else:
                nearestOnly = False

        matchedObjects = []
        matchSubset = []

        # RETURN ALL ANGULAR MATCHES BEFORE RETURNING NEAREST PHYSICAL SEARCH
        tmpSearchPara = dict(searchPara)
        tmpSearchPara["match nearest source only"] = False

        # ANGULAR CONESEARCH ON CATALOGUE - RETURN ALL MATCHES
        catalogueMatches = self.angular_crossmatch_against_catalogue(
            objectList=objectList,
            searchPara=tmpSearchPara,
            search_name=search_name,
            physicalSearch=True,
            brightnessFilter=brightnessFilter,
            classificationType=classificationType
        )

        # OK - WE HAVE SOME ANGULAR SEPARATION MATCHES. NOW SEARCH THROUGH THESE FOR MATCHES WITH
        # A PHYSICAL SEPARATION WITHIN THE PHYSICAL RADIUS.
        if catalogueMatches:
            for row in catalogueMatches:
                thisMatch = False
                physical_separation_kpc = row["physical_separation_kpc"]
                newsearch_name = search_name

                # FIRST CHECK FOR MAJOR AXIS MEASUREMENT
                # BYPASS NED FAULTY AXES MEASUREMENTS:
                # https://gist.github.com/search?utf8=%E2%9C%93&q=user%3Athespacedoctor+ned
                if row["major_axis_arcsec"] and ("ned" not in search_name or (row["unkMag"] and row["unkMag"] < 20.)):
                    if row["separationArcsec"] < row["major_axis_arcsec"] * self.settings["galaxy radius stetch factor"]:
                        thisMatch = True
                        newsearch_name = newsearch_name + \
                            " (within %s * major axis)" % (
                                self.settings["galaxy radius stetch factor"],)
                        newAngularSep = row[
                            "major_axis_arcsec"] * self.settings["galaxy radius stetch factor"]
                    else:
                        continue
                # NOW CHECK FOR A DIRECT DISTANCE MEASUREMENT
                elif row["direct_distance_scale"] and physical_separation_kpc < physicalRadius:
                    if row["separationArcsec"] > 300.:
                        continue
                    thisMatch = True
                    newsearch_name = newsearch_name + " (direct distance)"
                    newAngularSep = physicalRadius / \
                        row["direct_distance_scale"]
                # NEW CHECK FOR A REDSHIFT DISTANCE
                elif row["scale"] and physical_separation_kpc < physicalRadius:
                    thisMatch = True
                    newsearch_name = newsearch_name + " (redshift distance)"
                    newAngularSep = physicalRadius / row["scale"]

                if thisMatch == True:
                    row["physical_separation_kpc"] = physical_separation_kpc
                    row["original_search_radius_arcsec"] = newAngularSep
                    if physical_separation_kpc:
                        self.log.debug(
                            "\t\tPhysical separation = %.2f kpc" % (physical_separation_kpc,))
                    row["search_name"] = newsearch_name
                    matchSubset.append(row)

        if matchSubset:

            from operator import itemgetter
            matchSubset = sorted(matchSubset, key=itemgetter(
                'physical_separation_kpc'), reverse=False)

            if nearestOnly == True:
                theseMatches = matchSubset[0]
            else:
                theseMatches = matchSubset

            matchedObjects = matchSubset
        self.log.debug(
            'completed the ``physical_separation_crossmatch_against_catalogue`` method')

        self.log.info("FINISHED %s SEARCH IN %0.5f s" %
                      (search_name, time.time() - start_time,))

        return matchedObjects

    # use the tab-trigger below for new method
    # xt-class-method
