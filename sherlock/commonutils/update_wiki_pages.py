#!/usr/local/bin/python
# encoding: utf-8
"""
update_wiki_pages.py
====================
:Summary:
    Update the Github wiki pages with useful info

:Author:
    David Young

:Date Created:
    September 9, 2015

:dryx syntax:
    - ``_someObject`` = a 'private' object that should only be changed for debugging

:Notes:
    - If you have any questions requiring this script/module please email me: davidrobertyoung@gmail.com

:Tasks:
    @review: when complete pull all general functions and classes into dryxPython

# xdocopt-usage-tempx
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
from dryxPython import mysql as dms
from dryxPython import commonutils as dcu
from fundamentals import tools, times
# from ..__init__ import *


###################################################################
# CLASSES                                                         #
###################################################################
class update_wiki_pages():

    """
    The worker class for the update_wiki_pages module

    **Key Arguments:**
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary


    **Todo**
        - @review: when complete, clean update_wiki_pages class
        - @review: when complete add logging
        - @review: when complete, decide whether to abstract class to another module
    """
    # Initialisation
    # 1. @flagged: what are the unique attrributes for each object? Add them
    # to __init__

    def __init__(
            self,
            log,
            settings=False,

    ):
        self.log = log
        log.debug("instansiating a new 'update_wiki_pages' object")
        self.settings = settings
        # xt-self-arg-tmpx

        # 2. @flagged: what are the default attrributes each object could have? Add them to variable attribute set here
        # Variable Data Atrributes

        # 3. @flagged: what variable attrributes need overriden in any baseclass(es) used
        # Override Variable Data Atrributes

        # Initial Actions
        from sherlock import database
        db = database(
            log=self.log,
            settings=self.settings
        )
        self.transientsDbConn, self.cataloguesDbConn, self.pmDbConn = db.get()

        return None

    # 4. @flagged: what actions does each object have to be able to perform? Add them here
    # Method Attributes
    def get(self):
        """get the update_wiki_pages object

        **Return:**
            - ``update_wiki_pages``

        **Todo**
            - @review: when complete, clean get method
            - @review: when complete add logging
        """
        self.log.info('starting the ``get`` method')

        if "sherlock wiki root" not in self.settings:
            print "Sherlock wiki settings not found in settings file"
            return

        self._get_table_infos()
        self._get_view_infos()
        self._get_stream_view_infos()
        self._create_md_tables()
        self._write_wiki_pages()
        self._update_github()

        # RECODE INTO ASCII
        self.mdTable = self.mdTable.encode("utf-8", "ignore")

        self.log.info('completed the ``get`` method')
        return update_wiki_pages

    def _get_table_infos(
            self):
        """ get table infos

        **Key Arguments:**
            # -

        **Return:**
            - None

        **Todo**
            - @review: when complete, clean _get_table_infos method
            - @review: when complete add logging
        """
        self.log.info('starting the ``_get_table_infos`` method')

        sqlQuery = u"""
            SELECT * FROM crossmatch_catalogues.tcs_helper_catalogue_tables_info where legacy_table = 0 and table_name not like "legacy%%" order by number_of_rows desc;
        """ % locals()
        self.tableInfo = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )

        self.log.info('completed the ``_get_table_infos`` method')
        return None

    def _get_view_infos(
            self):
        """ get table infos

        **Key Arguments:**
            # -

        **Return:**
            - None

        **Todo**
            - @review: when complete, clean _get_view_infos method
            - @review: when complete add logging
        """
        self.log.info('starting the ``_get_view_infos`` method')

        sqlQuery = u"""
            SELECT * FROM crossmatch_catalogues.tcs_helper_catalogue_views_info where legacy_view = 0 and view_name not like "legacy%%" order by number_of_rows desc;
        """ % locals()
        self.viewInfo = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )

        self.log.info('completed the ``_get_view_infos`` method')
        return None

    def _get_stream_view_infos(
            self):
        """ get table infos

        **Key Arguments:**
            # -

        **Return:**
            - None

        **Todo**
            - @review: when complete, clean _get_view_infos method
            - @review: when complete add logging
        """
        self.log.info('starting the ``_get_stream_view_infos`` method')

        sqlQuery = u"""
            SELECT * FROM crossmatch_catalogues.tcs_helper_catalogue_tables_info where legacy_table = 0 and table_name not like "legacy%%"  and table_name like "%%stream" order by number_of_rows desc;
        """ % locals()
        self.streamInfo = dms.execute_mysql_read_query(
            sqlQuery=sqlQuery,
            dbConn=self.cataloguesDbConn,
            log=self.log
        )

        self.log.info('completed the ``_get_stream_view_infos`` method')
        return None

    # use the tab-trigger below for new method
    def _create_md_tables(
            self):
        """ create md table

        **Key Arguments:**
            # -

        **Return:**
            - None

        **Todo**
            - @review: when complete, clean _create_md_tables method
            - @review: when complete add logging
        """
        self.log.info('starting the ``_create_md_tables`` method')

        header = u"""
| <sub>Table Name</sub> | <sub>Description</sub> | <sub>Reference</sub> | <sub>Number Rows</sub> | <sub>Vizier</sub> | <sub>NED</sub> | <sub>Objects</sub> | <sub>Weight (1-10)</sub> |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |"""

        rows = u""
        for ti in self.tableInfo:
            table_name = ti["table_name"]
            description = ti["description"]
            url = ti["url"]
            number_of_rows = ti["number_of_rows"]
            reference_url = ti["reference_url"]
            reference_text = ti["reference_text"]
            notes = ti["notes"]
            vizier_link = ti["vizier_link"]
            in_ned = ti["in_ned"]
            object_types = ti["object_types"]
            version_number = ti["version_number"]
            last_updated = ti["last_updated"]
            legacy_table = ti["legacy_table"]
            old_table_name = ti["old_table_name"]
            weight = ti["object_type_accuracy"]

            number_of_rows = str(number_of_rows)
            thisLen = len(number_of_rows)
            newNumber = ""
            count = 0
            while count < thisLen:
                count += 1
                newNumber = number_of_rows[-count] + newNumber
                if count % 3 == 0:
                    newNumber = "," + newNumber
            if newNumber[0] == ",":
                newNumber = newNumber[1:]

            if len(vizier_link) and vizier_link != 0 and vizier_link != "0":
                vizier_link = u"[✓](%(vizier_link)s)" % locals()
            else:
                vizier_link = u""

            if in_ned:
                in_ned = u"✓"
            else:
                in_ned = u""

            rows += u"""
| <sub>%(table_name)s</sub> | <sub>[%(description)s](%(url)s)</sub> | <sub>[%(reference_text)s](%(reference_url)s)</sub> | <sub>%(newNumber)s</sub> | <sub>%(vizier_link)s</sub> | <sub>%(in_ned)s</sub> | <sub>%(object_types)s</sub> | <sub>%(weight)s</sub> |""" % locals()

        self.mdTable = header + rows

        header = u"""
| <sub>View Name</sub> | <sub>Number Rows</sub> | <sub>Object Type</sub> |
| :--- | :--- | :--- |"""

        rows = u""
        for ti in self.viewInfo:
            view_name = ti["view_name"]
            number_of_rows = ti["number_of_rows"]
            object_type = ti["object_type"]

            number_of_rows = str(number_of_rows)
            thisLen = len(number_of_rows)
            newNumber = ""
            count = 0
            while count < thisLen:
                count += 1
                newNumber = number_of_rows[-count] + newNumber
                if count % 3 == 0:
                    newNumber = "," + newNumber
            if newNumber[0] == ",":
                newNumber = newNumber[1:]

            rows += u"""
| <sub>%(view_name)s</sub> | <sub>%(newNumber)s</sub> | <sub>%(object_type)s</sub> |""" % locals()

        self.mdView = header + rows

        header = u"""
| <sub>Table Name</sub> | <sub>Description</sub> | <sub>Reference</sub> | <sub>Number Rows</sub> | <sub>Objects</sub> |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |"""

        rows = u""
        for ti in self.streamInfo:
            table_name = ti["table_name"]
            description = ti["description"]
            url = ti["url"]
            number_of_rows = ti["number_of_rows"]
            reference_url = ti["reference_url"]
            reference_text = ti["reference_text"]
            notes = ti["notes"]
            vizier_link = ti["vizier_link"]
            in_ned = ti["in_ned"]
            object_types = ti["object_types"]
            version_number = ti["version_number"]
            last_updated = ti["last_updated"]
            legacy_table = ti["legacy_table"]
            old_table_name = ti["old_table_name"]

            number_of_rows = str(number_of_rows)
            thisLen = len(number_of_rows)
            newNumber = ""
            count = 0
            while count < thisLen:
                count += 1
                newNumber = number_of_rows[-count] + newNumber
                if count % 3 == 0:
                    newNumber = "," + newNumber
            if newNumber[0] == ",":
                newNumber = newNumber[1:]

            if len(vizier_link) and vizier_link != 0 and vizier_link != "0":
                vizier_link = u"[✓](%(vizier_link)s)" % locals()
            else:
                vizier_link = u""

            if in_ned:
                in_ned = u"✓"
            else:
                in_ned = u""

            rows += u"""
| <sub>%(table_name)s</sub> | <sub>[%(description)s](%(url)s)</sub> | <sub>[%(reference_text)s](%(reference_url)s)</sub> | <sub>%(newNumber)s</sub> | <sub>%(object_types)s</sub> |""" % locals()

        self.mdStream = header + rows

        self.log.info('completed the ``_create_md_tables`` method')
        return None

    # use the tab-trigger below for new method
    def _write_wiki_pages(
            self):
        """ write wiki pages

        **Key Arguments:**
            # -

        **Return:**
            - None

        **Todo**
            - @review: when complete, clean _write_wiki_pages method
            - @review: when complete add logging
        """
        self.log.info('starting the ``_write_wiki_pages`` method')

        import codecs
        from datetime import datetime, date, time
        pathToWriteFile = self.settings[
            "sherlock wiki root"] + "/Crossmatch-Catalogue Tables.md"
        writeFile = codecs.open(pathToWriteFile, encoding='utf-8', mode='w')
        now = datetime.now()
        now = now.strftime("%Y-%m-%d %H:%M")
        lastUpdated = """Last Updated %(now)s  
""" % locals()

        writeFile.write(lastUpdated + self.mdTable)
        writeFile.close()

        pathToWriteFile = self.settings[
            "sherlock wiki root"] + "/Crossmatch-Catalogue Views.md"
        writeFile = codecs.open(pathToWriteFile, encoding='utf-8', mode='w')
        now = datetime.now()
        now = now.strftime("%Y-%m-%d %H:%M")
        lastUpdated = """Last Updated %(now)s  
""" % locals()

        writeFile.write(lastUpdated + self.mdView)
        writeFile.close()

        pathToWriteFile = self.settings[
            "sherlock wiki root"] + "/Crossmatch-Catalogue Streams.md"
        writeFile = codecs.open(pathToWriteFile, encoding='utf-8', mode='w')
        now = datetime.now()
        now = now.strftime("%Y-%m-%d %H:%M")
        lastUpdated = """Last Updated %(now)s  
""" % locals()

        writeFile.write(lastUpdated + self.mdStream)
        writeFile.close()

        self.log.info('completed the ``_write_wiki_pages`` method')
        return None

    # use the tab-trigger below for new method
    def _update_github(
            self):
        """ update github

        **Key Arguments:**
            # -

        **Return:**
            - None

        **Todo**
            - @review: when complete, clean _update_github method
            - @review: when complete add logging
        """
        self.log.info('starting the ``_update_github`` method')

        from subprocess import Popen, PIPE, STDOUT
        gdir = self.settings["sherlock wiki root"]
        cmd = """cd %(gdir)s && git pull origin master && git add --all && git commit -m "x" && git push origin master""" % locals()
        p = Popen(cmd, stdout=PIPE, stdin=PIPE, shell=True)
        output = p.communicate()[0]
        print output
        self.log.debug('output: %(output)s' % locals())

        self.log.info('completed the ``_update_github`` method')
        return None

    # use the tab-trigger below for new method
    # xt-class-method

    # 5. @flagged: what actions of the base class(es) need ammending? ammend them here
    # Override Method Attributes
    # method-override-tmpx

# xt-class-tmpx


###################################################################
# PUBLIC FUNCTIONS                                                #
###################################################################
# xt-worker-def

# use the tab-trigger below for new function
# xt-def-with-logger

###################################################################
# PRIVATE (HELPER) FUNCTIONS                                      #
###################################################################

if __name__ == '__main__':
    main()
