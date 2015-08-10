#!/usr/local/bin/python
# encoding: utf-8
"""
_database.py
============
:Summary:
    Class to setup database object for the sherlock

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
import time
import MySQLdb as ms
from docopt import docopt
from dryxPython import logs as dl
from dryxPython import commonutils as dcu
from dryxPython import mysql as dms
from dryxPython.projectsetup import setup_main_clutil


class database():

    """
    The worker class for the database module

    **Key Arguments:**
        - ``log`` -- logger
        - ``settings`` -- the settings dictionary
    """
    # INITIALISATION

    def __init__(
            self,
            log,
            settings=False,

    ):
        self.log = log
        log.debug("instansiating a new '_database' object")
        self.settings = settings
        return None

    def close(self):
        del self
        return None

    # METHOD ATTRIBUTES
    def get(self):
        """get the database object

        **Return:**
            - ``self.transientsDbConn, self.cataloguesDbConn`` -- two database connections
        """
        self.log.info('starting the ``get`` method')
        self._setup_database_connections()
        self.log.info('completed the ``get`` method')
        return self.transientsDbConn, self.cataloguesDbConn

    def _setup_database_connections(
            self):
        """ setup database connections for transient and catalogue databases
        """
        self.log.info('starting the ``_setup_database_connections`` method')

        from subprocess import Popen, PIPE, STDOUT

        # SETUP TUNNEL IF REQUIRED
        if self.settings["ssh tunnel"]["use tunnel"] is True:
            # TEST TUNNEL DOES NOT ALREADY EXIST
            sshPort = self.settings["ssh tunnel"]["port"]
            connected = self._checkServer(
                self.settings["database settings"]["static catalogues"]["host"], sshPort)
            if connected:
                self.log.info('ssh tunnel already exists - moving on')
            else:
                # GRAB TUNNEL SETTINGS FROM SETTINGS FILE
                ru = self.settings["ssh tunnel"]["remote user"]
                rip = self.settings["ssh tunnel"]["remote ip"]
                rh = self.settings["ssh tunnel"]["remote datbase host"]

                cmd = "ssh -fnN %(ru)s@%(rip)s -L %(sshPort)s:%(rh)s:3306" % locals()
                print cmd
                p = Popen(cmd, shell=True, close_fds=True)
                output = p.communicate()[0]
                self.log.debug('output: %(output)s' % locals())

                # TEST CONNECTION - QUIT AFTER SO MANY TRIES
                connected = False
                count = 0
                while not connected:
                    connected = self._checkServer(
                        self.settings["database settings"]["static catalogues"]["host"], sshPort)
                    time.sleep(1)
                    count += 1
                    if count == 5:
                        self.log.error(
                            'cound not setup tunnel to remote datbase' % locals())
                        sys.exit(0)

        # SETUP A DATABASE CONNECTION FOR THE STATIC CATALOGUES
        host = self.settings["database settings"]["static catalogues"]["host"]
        user = self.settings["database settings"]["static catalogues"]["user"]
        passwd = self.settings["database settings"][
            "static catalogues"]["password"]
        dbName = self.settings["database settings"]["static catalogues"]["db"]
        thisConn = ms.connect(
            host=host,
            user=user,
            passwd=passwd,
            db=dbName,
            port=sshPort,
            use_unicode=True,
            charset='utf8'
        )
        thisConn.autocommit(True)
        self.log.debug('cataloguesDbConn: %s' % (thisConn,))
        self.cataloguesDbConn = thisConn

        # SETUP DATABASE CONNECTION FOR TRANSIENTS DATABASE
        host = self.settings["database settings"]["transients"]["host"]
        user = self.settings["database settings"]["transients"]["user"]
        passwd = self.settings["database settings"][
            "transients"]["password"]
        dbName = self.settings["database settings"]["transients"]["db"]
        thisConn = ms.connect(
            host=host,
            user=user,
            passwd=passwd,
            db=dbName,
            port=sshPort,
            use_unicode=True,
            charset='utf8'
        )
        thisConn.autocommit(True)
        self.log.debug('transientsDbConn: %s' % (thisConn,))
        self.transientsDbConn = thisConn

        self.log.info('completed the ``_setup_database_connections`` method')
        return None

    def _checkServer(self, address, port):
        """Check that the TCP Port we've decided to use for tunnelling is available
        """
        self.log.info('starting the ``_checkServer`` method')

        # CREATE A TCP SOCKET
        import socket
        s = socket.socket()
        self.log.debug(
            """Attempting to connect to `%(address)s` on port `%(port)s`""" % locals())
        try:
            s.connect((address, port))
            self.log.info(
                """Connected to `%(address)s` on port `%(port)s`""" % locals())
            return True
        except socket.error, e:
            self.log.warning(
                """Connection to `%(address)s` on port `%(port)s` failed - try again: %(e)s""" % locals())
            return False

        return None

    # xt-class-method


if __name__ == '__main__':
    main()
