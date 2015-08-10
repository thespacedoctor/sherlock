#!/usr/local/bin/python
# encoding: utf-8
"""
_crossmatcher.py
================
:Summary:
    The transient to catalogue sources crossmatcher for sherlock

:Author:
    David Young

:Date Created:
    July 1, 2015

:dryx syntax:
    - ``_someObject`` = a 'private' object that should only be changed for debugging

:Notes:
    - If you have any questions requiring this script/module please email me: d.r.young@qub.ac.uk
"""
################# GLOBAL IMPORTS ####################
import sys
import os
os.environ['TERM'] = 'vt100'
import readline
import glob
import pickle
from docopt import docopt
from dryxPython import logs as dl
from dryxPython import astrotools as dat
from dryxPython import commonutils as dcu
from dryxPython.projectsetup import setup_main_clutil


class crossmatcher():

    """
    The worker class for the crossmatcher module

    **Key Arguments:**
        - ``dbConn`` -- mysql database connection for the catalogues
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary
        - ``transients`` -- the list of transients
    """
    # INITIALISATION

    def __init__(
            self,
            log,
            dbConn=False,
            settings=False,
            transients=[]
    ):
        self.log = log
        log.debug("instansiating a new '_crossmatcher' object")
        self.dbConn = dbConn
        self.settings = settings
        self.transients = transients
        # xt-self-arg-tmpx

        # VARIABLE DATA ATRRIBUTES
        self.classifications = []
        return None

    # METHOD ATTRIBUTES
    def get(self):
        """get the crossmatcher object

        **Return:**
            - ``classifications`` - list of all classifications
        """
        self.log.info('starting the ``get`` method')

        self._crossmatch_transients_against_catalogues()

        self.log.info('completed the ``get`` method')
        return self.classifications

    def _crossmatch_transients_against_catalogues(
            self):
        """ crossmatch transients against catalogues
        """

        self.log.info(
            'starting the ``_crossmatch_transients_against_catalogues`` method')

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
            print "Classifying object %(tId)s (%(tName)s) [%(thisIndex)s/%(numberOfTransients)d]" % locals()

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

                if self.stopAlgoritm == False:
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
                if len(matches) and "stop algorithm on match" in searchPara and searchPara["stop algorithm on match"]:
                    self.stopAlgoritm = True
                # ADD CLASSIFICATION AND CROSSMATCHES IF FOUND
                if searchDone and matches:
                    match = True
                    objectType = self.settings[
                        "classifications"][searchPara["transient classification"]]["flag"]
                    allMatches = allMatches + matches

            # IF NO MATCH IS FOUND THEN WE HAVE AN 'ORPHAN'
            if not match:
                objectType = self.settings["classifications"]["ORPHAN"]["flag"]

            # PERFORM ANY SUPPLIMENTARY SEARCHES
            ss = self.settings["supplementary search"]
            for searchName, searchPara in ss.iteritems():
                searchDone, supMatches = self.searchCatalogue(
                    objectList=[transient],
                    searchPara=searchPara,
                    searchName=searchName
                )
                if searchDone and supMatches:
                    objectType = objectType + \
                        self.settings["classifications"][
                            searchPara["transient classification"]]["flag"]
                allMatches = allMatches + matches

            # ADD DETAILS TO THE lOGS
            oldClass = transient['object_classification']
            oldClass = self._lookup_classification_dict(
                flag=transient['object_classification'])
            newClass = self._lookup_classification_dict(
                flag=objectType)
            self.log.info(
                """%(transient)s:  *** Object ID = %(tId)s (%(tName)s): CLASSIFICATION = %(newClass)s (PREVIOUSLY = %(oldClass)s)""" % locals())

            # BUILD THE LIST OF CROSSMATCH DICTIONARIES AND ADD THEM THE THE
            # LIST OF ALL SOURCE CLASSIFICATIONS
            crossmatches = []
            if allMatches:
                inputRow = allMatches[0][0]
                for m in allMatches:
                    catalogueTableName = m[2]
                    thisObjectType = m[3]
                    catalogueTableId = self.settings[
                        "CAT_ID_RA_DEC_COLS"][catalogueTableName][1]
                    for row in m[1]:
                        crossmatch = {}
                        crossmatch["transientObjectId"] = inputRow["id"]
                        crossmatch["catalogueObjectId"] = row[1][
                            self.settings["CAT_ID_RA_DEC_COLS"][catalogueTableName][0][0]]
                        crossmatch["catalogueTableId"] = catalogueTableId
                        crossmatch["separation"] = row[0]
                        crossmatch["z"] = row[1]["xmz"]
                        crossmatch["scale"] = row[1]["xmscale"]
                        crossmatch["distance"] = row[1]["xmdistance"]
                        crossmatch["distanceModulus"] = row[
                            1]["xmdistanceModulus"]
                        crossmatch["searchParametersId"] = self.settings[
                            "search parameter id"]
                        crossmatch["association_type"] = thisObjectType
                        crossmatches.append(crossmatch)

            classification = {'id': transient['id'], 'object_classification_old': transient[
                'object_classification'], 'object_classification_new': objectType, 'crossmatches': crossmatches}
            self.classifications.append(classification)

        self.log.info(
            'completed the ``_crossmatch_transients_against_catalogues`` method')
        return None

    def searchCatalogue(self, objectList, searchPara={}, searchName=""):
        """Cone Search wrapper to make it a little more user friendly"""

        from sherlock import conesearcher

        # EXTRACT PARAMETERS
        radius = searchPara["angular radius arcsec"]
        catalogueName = searchPara["database table"]
        matchedType = self.settings[
            "classifications"][searchPara["transient classification"]]["flag"]

        # VARIABLES
        matchedObjects = []
        matchSubset = []
        searchDone = True

        # FAINT STAR EXTRAS
        try:
            magColumn = searchPara["mag column"]
            faintLimit = searchPara["faint limit"]
        except:
            magColumn = False
            faintLimit = False

        for row in objectList:
            cs = conesearcher(
                log=self.log,
                ra=row['ra'],
                dec=row['dec'],
                radius=radius,
                tableName=catalogueName,
                queryType=2,
                dbConn=self.dbConn,
                settings=self.settings
            )

            message, xmObjects = cs.get()

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
                    self.settings["CAT_ID_RA_DEC_COLS"][catalogueName][0][0]]

                redshift = None
                xmz = None
                xmscale = None
                xmdistance = None
                xmdistanceModulus = None

                # CALCULATE PHYSICAL PARAMETERS ... IF WE CAN
                for xm in xmObjects:
                    # IF THERE'S A REDSHIFT, CALCULATE PHYSICAL PARAMETERS
                    if len(self.settings["CAT_ID_RA_DEC_COLS"][catalogueName][0]) > 3:
                        # THE CATALOGUE HAS A REDSHIFT COLUMN
                        redshift = xm[1][
                            self.settings["CAT_ID_RA_DEC_COLS"][catalogueName][0][3]]
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
                    xm[1]['xmz'] = xmz
                    xm[1]['xmscale'] = xmscale
                    xm[1]['xmdistance'] = xmdistance
                    xm[1]['xmdistanceModulus'] = xmdistanceModulus
                matchedObjects.append(
                    [row, xmObjects, catalogueName, matchedType])

        # FAINT STAR CUTS
        if searchDone and matchedObjects and magColumn and searchName != "sdss star":
            faintStarMatches = []
            matchSubset = []
            for row in matchedObjects[0][1]:
                rMag = row[1][magColumn]
                if rMag and rMag > faintLimit:
                    matchSubset.append(row)
            if matchSubset:
                faintStarMatches.append(
                    [matchedObjects[0][0], matchSubset, matchedObjects[0][2], matchedObjects[0][3]])
            matchedObjects = faintStarMatches
        elif searchName == "sdss star":
            sdssStarMatches = []
            matchSubset = []
            if searchDone and matchedObjects:
                for row in matchedObjects[0][1]:
                    rMag = row[1]["petroMag_r"]
                    separation = row[0]
                    # Line passes through (x,y) = (2.5,18) and (19,13)
                    lineTwo = -((18 - 13) / (19 - 2.5)) * separation + \
                        13 + 19 * ((18 - 13) / (19 - 2.5))
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

        return searchDone, matchedObjects

    def _lookup_classification_dict(
            self,
            flag):
        """ append reversed classification dictionary

        **Key Arguments:**
            # -

        **Return:**
            - classifications
        """
        self.log.info(
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

        self.log.info(
            'completed the ``_append_reversed_classification_dictionary`` method')
        return ' + '.join(flagTypes)

    def _physical_separation_search(self, objectRow, searchPara, searchName):
        """ physical separation search

        **Key Arguments:**
            # -

        **Return:**
            - searchDone -- did search complete successfully 
            - matchedObjects -- any sources matched against the object
        """
        self.log.info('starting the ``_physical_separation_search`` method')

        # SETUP PARAMETERS
        tableName = searchPara["database table"]
        angularRadius = searchPara["angular radius arcsec"]
        matchedObjects = []
        matchSubset = []
        physicalRadius = searchPara["physical radius kpc"]
        matchedType = self.settings[
            "classifications"][searchPara["transient classification"]]["flag"]

        # ANGULAR CONESEARCH ON CATALOGUE
        searchDone, matches = self.searchCatalogue(
            objectList=[objectRow],
            searchPara=searchPara,
            searchName=searchName
        )

        # OK - WE HAVE SOME ANGULAR SEPARATION MATCHES. NOW SEARCH THROUGH THESE FOR MATCHES WITH
        # A PHYSICAL SEPARATION WITHIN THE PHYSICAL RADIUS.
        if searchDone and matches:
            for row in matches[0][1]:
                if row[1]["xmscale"] and row[1]["xmscale"] * row[0] < physicalRadius:
                    self.log.info(
                        "\t\tPhysical separation = %.2f kpc" % (row[1]["xmscale"] * row[0]))
                    matchSubset.append(row)

        if matchSubset:
            matchedObjects.append(
                [matches[0][0], matchSubset, matches[0][2], matchedType])

        self.log.info('completed the ``_physical_separation_search`` method')

        return searchDone, matchedObjects

    # use the tab-trigger below for new method
    # xt-class-method
