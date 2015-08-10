#!/usr/local/bin/python
# encoding: utf-8
"""
classifier.py
=============
:Summary:
    The classifier object for Sherlock

:Author:
    David Young

:Date Created:
    June 29, 2015

:dryx syntax:
    - ``_someObject`` = a 'private' object that should only be changed for debugging

:Notes:
    - If you have any questions requiring this script/module please email me: d.r.young@qub.ac.uk

:Tasks:
    @review: when complete pull all general functions and classes into dryxPython
"""
################# GLOBAL IMPORTS ####################
import sys
import os
os.environ['TERM'] = 'vt100'
import readline
import glob
import pickle
import time
import MySQLdb as ms
from docopt import docopt
from dryxPython import logs as dl
from dryxPython import commonutils as dcu
from dryxPython import mysql as dms
from dryxPython.projectsetup import setup_main_clutil


class classifier():

    """
    The classifier object for Sherlock

    **Key Arguments:**
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary
        - ``update`` -- update the transient database with crossmatch results (boolean)
        - ``workflowListId`` -- which list to lift the unclassified transients from (default = eyeball list = 4)
        - ``transientIdList`` -- a list of individual transient ids (overrides `workflowListId` - used if user want to run the classifier on specific transients)

    **Todo**
        - @review: when complete, clean classifier class
        - @review: when complete add logging
        - @review: when complete, decide whether to abstract class to another module
    """
    # INITIALISATION

    def __init__(
            self,
            log,
            settings=False,
            update=False,
            workflowListId=4,
            transientIdList=[]
    ):
        self.log = log
        log.debug("instansiating a new 'classifier' object")
        self.settings = settings
        self.update = update
        self.workflowListId = workflowListId
        self.transientIdList = transientIdList
        # xt-self-arg-tmpx

        # VARIABLE DATA ATRRIBUTES
        self.transientsMetadataList = []

        # INITIAL ACTIONS
        # SETUP DATABASE CONNECTIONS
        from sherlock import database
        db = database(
            log=self.log,
            settings=self.settings
        )
        self.transientsDbConn, self.cataloguesDbConn = db.get()

        # DETERMINE WORKFLOW LIST TO LIFT UNCLASSIFIED TRANSIENT FOUND
        if isinstance(self.workflowListId, str):
            self._determine_workflow_list_id()

        return None

    def close(self):
        del self
        return None

    # METHOD ATTRIBUTES
    def get(self):
        """get the classifier object

        **Return:**
            - ``classifier``

        **Todo**
            - @review: when complete, clean get method
            - @review: when complete add logging
        """
        self.log.info('starting the ``get`` method')

        # CHOOSE METHOD TO GRAB TRANSIENT METADATA BEFORE CLASSIFICATION
        if len(self.transientIdList) > 0:
            self._get_individual_transient_metadata()
        else:
            self._get_transient_metadata_from_database_list()

        self._crossmatch_transients_against_catalogue()

        if self.update:
            self._update_transient_database()

        self.log.info('completed the ``get`` method')
        return self.classifications

    def _get_individual_transient_metadata(
            self):
        """ get individual transient metadata from the transient database
        """
        self.log.info(
            'starting the ``_get_individual_transient_metadata`` method')

        for tran in self.transientIdList:
            sqlQuery = u"""
                select id, followup_id, ra_psf 'ra', dec_psf 'dec', local_designation 'name', ps1_designation, object_classification, local_comments, detection_list_id
                from tcs_transient_objects
                where id = %(tran)s
            """ % locals()
            rows = dms.execute_mysql_read_query(
                sqlQuery=sqlQuery,
                dbConn=self.transientsDbConn,
                log=self.log
            )
            if len(rows):
                self.transientsMetadataList.append(rows[0])
            else:
                log.warning(
                    'could not find transient in database with id %(tran)s' % locals())

        self.log.info(
            'completed the ``_get_individual_transient_metadata`` method')
        return None

    def _get_transient_metadata_from_database_list(
            self):
        """ get transient metadata from a given workflow list in the transient database
        """
        self.log.info(
            'starting the ``_get_transient_metadata_from_database_list`` method')

        workflowListId = self.workflowListId
        sqlQuery = u"""
            select id, followup_id, ra_psf 'ra', dec_psf 'dec', local_designation 'name', object_classification
            from tcs_transient_objects
            where detection_list_id = %(workflowListId)s
            and object_classification = 0
            order by followup_id
        """ % locals()
        self.transientsMetadataList = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.transientsDbConn,
            log=self.log
        )

        self.log.info(
            'completed the ``_get_transient_metadata_from_database_list`` method')
        return None

    def _determine_workflow_list_id(
            self):
        """ determine workflow list id from settings file
        """
        self.log.info('starting the ``_determine_workflow_list_id`` method')
        self.workflowListId = self.settings[
            "workflow lists"][self.workflowListId.upper()]
        self.log.info('completed the ``_determine_workflow_list_id`` method')
        return None

    # use the tab-trigger below for new method
    def _crossmatch_transients_against_catalogue(
            self):
        """ crossmatch transients against catalogue

        **Key Arguments:**
            # -

        **Return:**
            - None

        **Todo**
            - @review: when complete, clean _crossmatch_transients_against_catalogue method
            - @review: when complete add logging
        """
        self.log.info(
            'starting the ``_crossmatch_transients_against_catalogue`` method')

        from sherlock import crossmatcher
        self.allClassifications = []

        cm = crossmatcher(
            log=self.log,
            dbConn=self.cataloguesDbConn,
            transients=self.transientsMetadataList,
            settings=self.settings
        )
        self.classifications = cm.get()
        # tid = trans["id"]
        # name = tran["name"]
        # classification = self.getFlagDefs(
        #     objectType, CLASSIFICATION_FLAGS, delimiter=' ').upper()
        # oldClassification = self.getFlagDefs(
        #     trans['object_classification'], CLASSIFICATION_FLAGS, delimiter=' ').upper()
        # print "*** Object ID = %(tid)s (%(name)s): CLASSIFICATION =
        # %(classification)s (PREVIOUSLY = %(oldClassification)s)" % locals()

        self.log.info(
            'completed the ``_crossmatch_transients_against_catalogue`` method')
        return None

    # use the tab-trigger below for new method
    def _update_transient_database(
            self):
        """ update transient database

        **Key Arguments:**
            # -

        **Return:**
            - None

        **Todo**
            - @review: when complete, clean _update_transient_database method
            - @review: when complete add logging
        """
        self.log.info('starting the ``_update_transient_database`` method')

        from datetime import datetime, date, time
        now = datetime.now()
        now = now.strftime("%Y-%m-%d %H:%M:%S")

        for c in self.classifications:

            objectType = c["object_classification_new"]
            transientObjectId = c["id"]

            sqlQuery = u"""
                    update tcs_transient_objects set object_classification = %(objectType)s, object_classification_date = "%(now)s"
                        where id = %(transientObjectId)s
                        and object_classification != %(objectType)s
                """ % locals()
            dms.execute_mysql_write_query(
                sqlQuery=sqlQuery,
                dbConn=self.transientsDbConn,
                log=self.log
            )

            # DELETE PREVIOUS CROSSMATCHES
            sqlQuery = u"""
                    delete from tcs_cross_matches where transient_object_id = %(transientObjectId)s and association_type is null
                """ % locals()
            dms.execute_mysql_write_query(
                sqlQuery=sqlQuery,
                dbConn=self.transientsDbConn,
                log=self.log
            )

            # INSERT NEW CROSSMATCHES
            for crossmatch in c["crossmatches"]:
                for k, v in crossmatch.iteritems():
                    if v == None:
                        crossmatch[k] = "null"

                sqlQuery = u"""
                        insert into tcs_cross_matches (
                           transient_object_id,
                           catalogue_object_id,
                           catalogue_table_id,
                           separation,
                           z,
                           scale,
                           distance,
                           distance_modulus,
                           search_parameters_id,
                           date_added,
                           association_type)
                        values (
                           %s,
                           "%s",
                           %s,
                           %s,
                           %s,
                           %s,
                           %s,
                           %s,
                           %s,
                           "%s",
                           "%s")
                        """ % (crossmatch["transientObjectId"], crossmatch["catalogueObjectId"], crossmatch["catalogueTableId"], crossmatch["separation"], crossmatch["z"], crossmatch["scale"], crossmatch["distance"], crossmatch["distanceModulus"], crossmatch["searchParametersId"], now, crossmatch["association_type"])
                dms.execute_mysql_write_query(
                    sqlQuery=sqlQuery,
                    dbConn=self.transientsDbConn,
                    log=self.log
                )

        self.log.info('completed the ``_update_transient_database`` method')
        return None

    # use the tab-trigger below for new method
    # xt-class-method


if __name__ == '__main__':
    main()
