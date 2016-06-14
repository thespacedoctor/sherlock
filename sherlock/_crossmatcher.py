#!/usr/local/bin/python
# encoding: utf-8
"""
*The transient to catalogue sources crossmatcher for sherlock*

:Author:
    David Young

:Date Created:
    July 1, 2015
"""
################# GLOBAL IMPORTS ####################
import sys
import os
os.environ['TERM'] = 'vt100'
import readline
import glob
import pickle
import math
from docopt import docopt
from dryxPython import logs as dl
from dryxPython import astrotools as dat
from dryxPython import commonutils as dcu
from fundamentals import tools, times


class crossmatcher():

    """
    *The worker class for the crossmatcher module*

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection for the catalogues
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary
        - ``colMaps`` -- maps of the important column names for each table/view in the crossmatch-catalogues database
        - ``transients`` -- the list of transients
    """
    # INITIALISATION

    def __init__(
            self,
            log,
            dbConn=False,
            settings=False,
            colMaps=False,
            transients=[]
    ):
        self.log = log
        log.debug("instansiating a new '_crossmatcher' object")
        self.dbConn = dbConn
        self.settings = settings
        self.transients = transients
        self.colMaps = colMaps
        # xt-self-arg-tmpx

        return None

    # METHOD ATTRIBUTES
    def get(self):
        """
        *get the crossmatcher object*

        **Return:**
            - ``classifications`` - list of all classifications
        """
        self.log.debug('starting the ``get`` method')

        classifications = self._crossmatch_transients_against_catalogues()

        self.log.debug('completed the ``get`` method')
        return classifications

    def _crossmatch_transients_against_catalogues(
            self):
        """
        *crossmatch transients against catalogues*
        """

        classifications = []

        self.log.debug(
            'starting the ``_crossmatch_transients_against_catalogues`` method')

        # COUNT NUMBER OF TRANSIENT TO CROSSMATCH
        classification = []
        numberOfTransients = len(self.transients)
        count = 0

        # FOR EACH TRANSIENT SOURCE IN THE LIST ...
        for thisIndex, transient in enumerate(self.transients):

            tId = transient['id']
            tName = transient['name']

            # PRINT A STATUS UPDATE
            count += 1
            if count > 1:
                # CURSOR UP ONE LINE AND CLEAR LINE
                sys.stdout.write("\x1b[1A\x1b[2K")
            thisCount = thisIndex + 1
            message = "Classifying object `%(tName)s` [%(thisCount)s/%(numberOfTransients)d]" % locals(
            )
            print message
            self.log.info('\n\n\nmessage: %(message)s' % locals())

            # RESET MATCH VALUES
            match = False  # SET TO TRUE WHENEVER A MATCH IS FIRST FOUND
            # THE LIST OF OBJECTS FOUND ASSOCIATED WITH THE SOURCE
            matches = []
            allMatches = []
            # SET TO TRUE WHENEVER WE WANT TO TERMINATE THE SEARCH
            searchDone = False
            self.stopAlgoritm = False

            # GRAB SEARCH ALGORITHM AND ITERATE THROUGH IT IN ORDER
            # PRESENTED IN THE SETTINGS FILE
            sa = self.settings["search algorithm"]
            for searchName, searchPara in sa.iteritems():
                self.log.info("""  searching: %(searchName)s""" % locals())
                if "physical radius kpc" in searchPara:
                    # THE PHYSICAL SEPARATION SEARCHES
                    self.log.info(
                        'checking physical distance crossmatches in %(searchName)s' % locals())
                    searchDone, matches = self._physical_separation_search(
                        objectRow=transient,
                        searchPara=searchPara,
                        searchName=searchName
                    )
                else:
                    # THE ANGULAR SEPARATION SEARCHES
                    self.log.info(
                        'Crossmatching against %(searchName)s' % locals())
                    searchDone, matches = self.searchCatalogue(
                        objectList=[transient],
                        searchPara=searchPara,
                        searchName=searchName
                    )

                # ADD CLASSIFICATION AND CROSSMATCHES IF FOUND
                if searchDone and matches:
                    self.log.info("     match found for %(tName)s" % locals())
                    match = True
                    # @action update objectType -- it gets overwritten with each subsequent search
                    objectType = searchPara["transient classification"]
                    allMatches = allMatches + matches
                    del matches

                    if "stop algorithm on match" in searchPara and searchPara["stop algorithm on match"] == True:
                        self.stopAlgoritm = True
                        self.log.info(
                            "     stopping algorithm for %(tName)s" % locals())
                        break

            # IF NO MATCH IS FOUND THEN WE HAVE AN 'ORPHAN'
            if not match:
                self.log.info(
                    "   %(tName)s classified as an orphan" % locals())
                objectType = "ORPHAN"

            # PERFORM ANY SUPPLIMENTARY SEARCHES
            if "supplementary search" in self.settings:
                ss = self.settings["supplementary search"]
                for searchName, searchPara in ss.iteritems():
                    self.log.info("""  searching: %(searchName)s""" % locals())
                    searchDone, supMatches = self.searchCatalogue(
                        objectList=[transient],
                        searchPara=searchPara,
                        searchName=searchName
                    )
                    if searchDone and supMatches:
                        self.log.info(
                            "     match found for %(tName)s" % locals())
                        objectType = objectType + \
                            searchPara["transient classification"]
                        allMatches = allMatches + supMatches

            # ADD DETAILS TO THE lOGS
            oldClass = transient['object_classification']
            # oldClass = self._lookup_classification_dict(
            #     flag=transient['object_classification'])
            # newClass = self._lookup_classification_dict(
            #     flag=objectType)
            # self.log.info(
            #     """%(transient)s:  *** Object ID = %(tId)s (%(tName)s): CLASSIFICATION = %(newClass)s (PREVIOUSLY = %(oldClass)s)""" % locals())

            # BUILD THE LIST OF CROSSMATCH DICTIONARIES AND ADD THEM THE THE
            # LIST OF ALL SOURCE CLASSIFICATIONS
            crossmatches = []
            if allMatches:
                inputRow = allMatches[0][0]
                for m in allMatches:
                    catalogueTableName = m[2]
                    thisObjectType = m[3]
                    catalogueTableId = self.colMaps[
                        catalogueTableName]["table_id"]
                    catalogueViewId = self.colMaps[
                        catalogueTableName]["view_id"]

                    for row in m[1]:
                        crossmatch = {}
                        crossmatch["transientObjectId"] = inputRow["id"]
                        crossmatch["catalogueObjectId"] = row[1][
                            self.colMaps[catalogueTableName]["objectNameColName"]]
                        crossmatch["catalogueTableId"] = catalogueTableId
                        crossmatch["catalogueViewId"] = catalogueViewId
                        crossmatch["catalogueTableName"] = self.colMaps[
                            catalogueTableName]["table_name"]
                        crossmatch["catalogueViewName"] = self.colMaps[
                            catalogueTableName]["view_name"]
                        crossmatch["separation"] = row[0]
                        crossmatch["z"] = row[1]["xmz"]
                        crossmatch["scale"] = row[1]["xmscale"]
                        crossmatch["distance"] = row[1]["xmdistance"]
                        crossmatch["distanceModulus"] = row[
                            1]["xmdistanceModulus"]
                        crossmatch["association_type"] = thisObjectType
                        crossmatch["sourceType"] = row[1]["sourceType"]
                        crossmatch["sourceSubType"] = row[1]["sourceSubType"]
                        crossmatch["searchName"] = row[1]["searchName"]
                        crossmatch["xmdirectdistance"] = row[
                            1]["xmdirectdistance"]
                        crossmatch["xmdirectdistanceModulus"] = row[
                            1]["xmdirectdistanceModulus"]
                        crossmatch["xmdirectdistancescale"] = row[
                            1]["xmdirectdistancescale"]
                        crossmatch["originalSearchRadius"] = row[
                            1]["originalSearchRadius"]
                        crossmatch["sourceRa"] = row[
                            1]["sourceRa"]
                        crossmatch["sourceDec"] = row[
                            1]["sourceDec"]
                        crossmatch["xmmajoraxis"] = row[1]["xmmajoraxis"]
                        if crossmatch["sourceSubType"] == "null":
                            crossmatch["sourceSubType"] = None
                        if "physical_separation_kpc" in row[
                                1].keys():
                            crossmatch["physical_separation_kpc"] = row[
                                1]["physical_separation_kpc"]
                        else:
                            crossmatch["physical_separation_kpc"] = None
                        crossmatches.append(crossmatch)

            # self.log.info('crossmatches: %(crossmatches)s' % locals())

            classification = {'id': transient['id'], 'object_classification_old': transient[
                'object_classification'], 'object_classification_new': objectType, 'crossmatches': crossmatches}
            classifications.append(classification)

        self.log.debug(
            'completed the ``_crossmatch_transients_against_catalogues`` method')
        return classifications

    def searchCatalogue(self, objectList, searchPara={}, searchName=""):
        """*Cone Search wrapper to make it a little more user friendly*"""

        from sherlock import conesearcher

        message = "  Starting `%(searchName)s` catalogue conesearch" % locals(
        )
        # print message

        # EXTRACT PARAMETERS FROM ARGUMENTS & SETTINGS FILE
        if "physical radius kpc" in searchPara:
            physicalSearch = True
            searchName = searchName + " physical"
        else:
            physicalSearch = False
            searchName = searchName + " angular"
        radius = searchPara["angular radius arcsec"]
        catalogueName = searchPara["database table"]
        matchedType = searchPara["transient classification"]

        # VARIABLES
        matchedObjects = []
        matchSubset = []
        searchDone = True

        # FAINT & BRIGHT STAR EXTRAS
        try:
            faintMagColumn = searchPara["faint mag column"]
            faintLimit = searchPara["faint limit"]
        except:
            faintMagColumn = False
            faintLimit = False
        try:
            filterColumns = searchPara["filter names"]
            magColumns = searchPara["mag columns"]
            magErrColumns = searchPara["mag err columns"]
        except:
            filterColumns = False
            magColumns = False
            magErrColumns = False

        totalCount = len(objectList)
        count = 0
        for row in objectList:
            count += 1
            if count > 1:
                # Cursor up one line and clear line
                sys.stdout.write("\x1b[1A\x1b[2K")
            if count > totalCount:
                count = totalCount
            percent = (float(count) / float(totalCount)) * 100.
            cs = conesearcher(
                log=self.log,
                ra=row['ra'],
                dec=row['dec'],
                radius=radius,
                colMaps=self.colMaps,
                tableName=catalogueName,
                queryType=2,
                dbConn=self.dbConn,
                settings=self.settings,
                physicalSearch=physicalSearch,
                transType=searchPara["transient classification"]
            )
            message, xmObjects = cs.get()
            del cs
            resultLen = len(xmObjects)
            print "  %(count)s / %(totalCount)s (%(percent)1.1f%%) transients conesearched against with %(searchName)s - %(resultLen)s sources matched" % locals()

            # DID WE SEARCH THE CATALOGUES CORRECTLY?
            if message and (message.startswith('Error') or 'not recognised' in message):
                # SUCCESSFUL CONE SEARCHES SHOULD NOT RETURN AN ERROR MESSAGE,
                # OTHERWISE SOMETHING WENT WRONG.
                print "Database error - cone search unsuccessful.  Message was:"
                print "\t%s" % message
                searchDone = False

            if xmObjects:
                numberOfMatches = len(xmObjects)
                nearestSep = xmObjects[0][0]
                nearestCatRow = xmObjects[0][1]
                nearestCatId = nearestCatRow[
                    self.colMaps[catalogueName]["objectNameColName"]]

                # CALCULATE PHYSICAL PARAMETERS ... IF WE CAN
                for xm in xmObjects:
                    redshift = None
                    xmz = None
                    xmscale = None
                    xmdistance = None
                    xmdistanceModulus = None
                    xmmajoraxis = None
                    xmdirectdistance = None
                    xmdirectdistancescale = None
                    xmdirectdistanceModulus = None

                    # IF THERE'S A REDSHIFT, CALCULATE PHYSICAL PARAMETERS
                    if self.colMaps[catalogueName]["redshiftColName"]:
                        # THE CATALOGUE HAS A REDSHIFT COLUMN
                        redshift = xm[1][
                            self.colMaps[catalogueName]["redshiftColName"]]
                    if redshift and redshift > 0.0:
                        # CALCULATE DISTANCE MODULUS, ETC
                        redshiftInfo = dat.convert_redshift_to_distance(
                            z=redshift
                        )
                        if redshiftInfo:
                            xmz = redshiftInfo['z']
                            xmscale = redshiftInfo['da_scale']
                            xmdistance = redshiftInfo['dl_mpc']
                            xmdistanceModulus = redshiftInfo['dmod']
                    # ADD MAJOR AXIS VALUE
                    if "or within semi major axis" in searchPara and searchPara["or within semi major axis"] == True and self.colMaps[catalogueName]["semiMajorColName"] and xm[1][self.colMaps[catalogueName]["semiMajorColName"]]:
                        xmmajoraxis = xm[1][
                            self.colMaps[catalogueName]["semiMajorColName"]] * self.colMaps[catalogueName]["semiMajorToArcsec"]
                    # ADD DISTANCE VALUES
                    if self.colMaps[catalogueName]["distanceColName"] and xm[1][self.colMaps[catalogueName]["distanceColName"]]:
                        xmdirectdistance = xm[1][
                            self.colMaps[catalogueName]["distanceColName"]]
                        xmdirectdistancescale = xmdirectdistance / 206.264806
                        xmdirectdistanceModulus = 5 * \
                            math.log10(xmdirectdistance * 1e6) - 5
                    xm[1]['xmz'] = xmz
                    xm[1]['xmscale'] = xmscale
                    xm[1]['xmdistance'] = xmdistance
                    xm[1]['xmdistanceModulus'] = xmdistanceModulus
                    xm[1]['xmmajoraxis'] = xmmajoraxis
                    xm[1]['xmdirectdistance'] = xmdirectdistance
                    xm[1]['xmdirectdistancescale'] = xmdirectdistancescale
                    xm[1]['xmdirectdistanceModulus'] = xmdirectdistanceModulus

                # GRAB SOURCE TYPES
                for xm in xmObjects:
                    subType = None
                    # IF THERE'S A SUBTYPE - ADD IT
                    if self.colMaps[catalogueName]["subTypeColName"]:
                        # THE CATALOGUE HAS A REDSHIFT COLUMN
                        subType = xm[1][
                            self.colMaps[catalogueName]["subTypeColName"]]
                        if subType == "null":
                            subType = None

                    xm[1]['sourceSubType'] = subType
                    xm[1]['sourceType'] = self.colMaps[
                        catalogueName]["object_type"]
                    xm[1]["searchName"] = searchName
                    xm[1]["sourceRa"] = xm[1][
                        self.colMaps[catalogueName]["raColName"]]
                    xm[1]["sourceDec"] = xm[1][
                        self.colMaps[catalogueName]["decColName"]]
                    xm[1]["originalSearchRadius"] = radius

                    # SOURCE MAG AND FILTER - CHOOSE MAG WITH LOWEST ERROR

                    xm[1]["sourceFilter"] = self.colMaps[
                        catalogueName]["filterName1ColName"]

                    xm[1]["sourceMagnitude"] = xm[1][
                        self.colMaps[catalogueName]["filter1ColName"]]
                    if self.colMaps[catalogueName]["filterErr1ColName"]:
                        xm[1]["sourceMagnitudeErr"] = xm[1][
                            self.colMaps[catalogueName]["filterErr1ColName"]]
                    else:
                        xm[1]["sourceMagnitudeErr"] = None
                    thisMagErr = xm[1]["sourceMagnitude"]
                    index = 2
                    while thisMagErr is not None and index < 6:
                        if self.colMaps[catalogueName][
                                "filterErr%(index)sColName" % locals()]:
                            thisMagErr = xm[1][self.colMaps[catalogueName][
                                "filterErr%(index)sColName" % locals()]]
                        else:
                            thisMagErr = None
                        if thisMagErr is not None and thisMagErr < xm[1]["sourceMagnitudeErr"]:
                            filterName = self.colMaps[
                                catalogueName]["filterName%(index)sColName" % locals()][:4]
                            if "col_" not in filterName:
                                xm[1]["sourceFilter"] = filterName
                            else:
                                xm[1]["sourceFilter"] = xm[1][filterName]
                            xm[1]["sourceMagnitude"] = xm[1][
                                self.colMaps[catalogueName]["filter%(index)sColName" % locals()]]
                            xm[1]["sourceMagnitudeErr"] = thisMagErr
                        index += 1

                matchedObjects.append(
                    [row, xmObjects, catalogueName, matchedType])

        # FAINT STAR CUTS
        if searchDone and matchedObjects and faintMagColumn:
            faintStarMatches = []
            matchSubset = []
            for row in matchedObjects[0][1]:
                rMag = row[1][faintMagColumn]
                if rMag and rMag > faintLimit:
                    matchSubset.append(row)

            if matchSubset:
                faintStarMatches.append(
                    [matchedObjects[0][0], matchSubset, matchedObjects[0][2], matchedObjects[0][3]])
            matchedObjects = faintStarMatches
        # BRIGHT STAR MATCH
        elif "tcs_view_star_sdss" in catalogueName:
            sdssStarMatches = []
            matchSubset = []

            if searchDone and matchedObjects:
                for row in matchedObjects[0][1]:
                    rMag = row[1]["petroMag_r"]
                    separation = row[0]
                    # Line passes through (x,y) = (2.5,18) and (19,13)
                    lineTwo = -((18 - 13) / (19 - 2.5)) * \
                        separation + 13 + 19 * ((18 - 13) / (19 - 2.5))
                    if rMag < 13.0:
                        matchSubset.append(row)
                    elif rMag >= 13.0 and rMag < 18.0:
                        if rMag < lineTwo:
                            matchSubset.append(row)
                    elif rMag >= 18.0 and separation < 2.5:
                        matchSubset.append(row)
            if matchSubset:
                sdssStarMatches.append(
                    [matchedObjects[0][0], matchSubset, matchedObjects[0][2], matchedObjects[0][3]])
            matchedObjects = sdssStarMatches

        if "match nearest source only" in searchPara and searchPara["match nearest source only"] == True and len(matchedObjects):
            matchedObjects = [[matchedObjects[0][0], [
                matchedObjects[0][1][0]], matchedObjects[0][2], matchedObjects[0][3]]]

        message = "  Finished `%(searchName)s` catalogue conesearch" % locals(
        )
        # print message

        return searchDone, matchedObjects

    def _lookup_classification_dict(
            self,
            flag):
        """
        *append reversed classification dictionary*

        **Key Arguments:**
            # -

        **Return:**
            - classifications
        """
        self.log.debug(
            'starting the ``_append_reversed_classification_dictionary`` method')

        flagTypes = []

        if flag == 0:
            return 'unclassified'

        for i in range(16):
            mask = (1 << 15) >> i

            try:
                result = flag & mask
                if result:
                    for code in self.settings["classifications"]:
                        if self.settings["classifications"][code]["flag"] == result:
                            flagTypes.append(
                                self.settings["classifications"][code]["human"])
            except TypeError:
                # We got a None (i.e. NULL) valueflags
                return ''

        self.log.debug(
            'completed the ``_append_reversed_classification_dictionary`` method')
        return ' + '.join(flagTypes)

    def _physical_separation_search(self, objectRow, searchPara, searchName):
        """
        *physical separation search*

        **Key Arguments:**
            - ``objectRow`` -- transient to be crossmatched
            - ``searchPara`` -- parameters of the search (from settings file)
            - ``searchName`` -- the name of the search

        **Return:**
            - searchDone -- did search complete successfully
            - matchedObjects -- any sources matched against the object
        """
        self.log.debug('starting the ``_physical_separation_search`` method')

        # SETUP PARAMETERS
        tableName = searchPara["database table"]
        angularRadius = searchPara["angular radius arcsec"]
        matchedObjects = []
        matchSubset = []
        physicalRadius = searchPara["physical radius kpc"]
        matchedType = searchPara["transient classification"]

        # RETURN ALL ANGULAR MATCHES BEFORE RETURNING NEAREST PHYSICAL SEARCH
        nearestOnly = searchPara["match nearest source only"]
        tmpSearchPara = dict(searchPara)
        tmpSearchPara["match nearest source only"] = False

        # ANGULAR CONESEARCH ON CATALOGUE
        searchDone, matches = self.searchCatalogue(
            objectList=[objectRow],
            searchPara=tmpSearchPara,
            searchName=searchName
        )
        if "physical radius kpc" in searchPara:
            physicalSearch = True
            searchName = searchName + " physical"
        else:
            physicalSearch = False
            searchName = searchName + " angular"

        # OK - WE HAVE SOME ANGULAR SEPARATION MATCHES. NOW SEARCH THROUGH THESE FOR MATCHES WITH
        # A PHYSICAL SEPARATION WITHIN THE PHYSICAL RADIUS.
        if searchDone and matches:
            objectName = matches[0][0]["name"]
            for row in matches[0][1]:

                thisMatch = False
                physicalSeparation = None
                newSearchName = searchName

                # CALCULATE MOST ACCURATE PHYSICAL SEPARATION
                if row[1]["xmdirectdistancescale"]:
                    physicalSeparation = row[1][
                        "xmdirectdistancescale"] * row[0]
                elif row[1]["xmscale"]:
                    physicalSeparation = row[1]["xmscale"] * row[0]

                # FIRST CHECK FOR MAJOR AXIS MEASUREMENT
                if row[1]["xmmajoraxis"] and row[0] < row[1]["xmmajoraxis"] * self.settings["galaxy radius stetch factor"]:
                    self.log.info(
                        '%(objectName)s is found within the major axis measurement of the source' % locals())
                    thisMatch = True
                    newSearchName = newSearchName + \
                        " (within %s * major axis)" % (
                            self.settings["galaxy radius stetch factor"],)
                    newAngularSep = row[1][
                        "xmmajoraxis"] * self.settings["galaxy radius stetch factor"]
                # NOW CHECK FOR A DIRECT DISTANCE MEASUREMENT
                elif row[1]["xmdirectdistancescale"] and physicalSeparation < physicalRadius:
                    self.log.info(
                        '%(objectName)s is found within %(physicalSeparation)s Kpc of the source centre (direct distance measurement)' % locals())
                    thisMatch = True
                    newSearchName = newSearchName + " (direct distance)"
                    newAngularSep = physicalRadius / \
                        row[1]["xmdirectdistancescale"]
                # NEW CHECK FOR A REDSHIFT DISTANCE
                elif row[1]["xmscale"] and physicalSeparation < physicalRadius:
                    self.log.info(
                        '%(objectName)s is found within %(physicalSeparation)s Kpc of the source centre (redshift distance measurement)' % locals())
                    thisMatch = True
                    newSearchName = newSearchName + " (redshift distance)"
                    newAngularSep = physicalRadius / row[1]["xmscale"]
                else:
                    self.log.info(
                        '%(objectName)s is not associated with the source (from inspecting the transient/source physical separation calculations)' % locals())

                if thisMatch == True:
                    row[1]["physical_separation_kpc"] = physicalSeparation
                    row[1]["originalSearchRadius"] = newAngularSep
                    if physicalSeparation:
                        self.log.info(
                            "\t\tPhysical separation = %.2f kpc" % (physicalSeparation,))
                    row[1]["searchName"] = newSearchName
                    matchSubset.append([physicalSeparation, row])

        if matchSubset:
            from operator import itemgetter
            matchSubset = sorted(matchSubset, key=itemgetter(0))

            if nearestOnly == True:
                theseMatches = [matchSubset[0][1]]
            else:
                theseMatches = []
                for i in matchSubset:
                    theseMatches.append(i[1])

            matchedObjects.append(
                [matches[0][0], theseMatches, matches[0][2], matchedType])
        self.log.debug('completed the ``_physical_separation_search`` method')

        return searchDone, matchedObjects

    # use the tab-trigger below for new method
    # xt-class-method
