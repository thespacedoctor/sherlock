#!/usr/local/bin/python
# encoding: utf-8
"""
*the database object for sherlock, setting up ssh tunnels and various database connections*

:Author:
    David Young
"""
import os
os.environ['TERM'] = 'vt100'


class database(object):
    """
    *the database object for sherlock, setting up ssh tunnels and various database connections*

    The returned dictionary of database connections contain the following databases:
        - ``transients`` -- the database hosting the transient source data
        - ``catalogues`` -- connection to the database hosting the contextual catalogues the transients are to be crossmatched against

    **Key Arguments**

    - ``log`` -- logger
    - ``settings`` -- the settings dictionary


    **Return**

    - ``dbConns`` -- a dictionary of the database connections required by sherlock


    **Usage**

    To setup the sherlock database connections, run the following:

    ```python
    # SETUP ALL DATABASE CONNECTIONS
    from sherlock import database
    db = database(
        log=log,
        settings=settings
    )
    dbConns, dbVersions = db.connect()
    transientsDbConn = dbConns["transients"]
    cataloguesDbConn = dbConns["catalogues"]
    ```


    .. todo ::

        - update key arguments values and definitions with defaults
        - update return values and definitions
        - update usage examples and text
        - update docstring text
        - check sublime snippet exists
        - clip any useful text to docs mindmap
        - regenerate the docs and check redendering of this docstring
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

    def connect(self):
        """connect to the various databases, the credientals and settings of which are found in the sherlock settings file

        **Return**

        - ``transientsDbConn`` -- the database hosting the transient source data
        - ``cataloguesDbConn`` -- connection to the database hosting the contextual catalogues the transients are to be crossmatched against


        See the class docstring for usage

        .. todo ::

            - update key arguments values and definitions with defaults
            - update return values and definitions
            - update usage examples and text
            - update docstring text
            - check sublime snippet exists
            - clip any useful text to docs mindmap
            - regenerate the docs and check redendering of this docstring
        """
        self.log.debug('starting the ``get`` method')

        from fundamentals.mysql import readquery

        # CATALOGUE DATABASE ALWAYS NEEDED
        catalogueSettings = self.settings[
            "database settings"]["static catalogues"]

        # TRANSIENT DATABASE OPTIONAL
        if "transients" in self.settings[
                "database settings"]:
            transientSettings = self.settings[
                "database settings"]["transients"]
        else:
            transientSettings = False

        dbConns = []
        for dbSettings in [transientSettings, catalogueSettings]:
            port = False
            if dbSettings and "tunnel" in dbSettings and dbSettings["tunnel"]:
                port = self._setup_tunnel(
                    tunnelParameters=dbSettings["tunnel"]
                )
            elif dbSettings and "port" in dbSettings and dbSettings["port"]:
                port = int(dbSettings["port"])

            if dbSettings:
                import pymysql as ms
                # SETUP A DATABASE CONNECTION FOR THE STATIC CATALOGUES
                host = dbSettings["host"]
                user = dbSettings["user"]
                passwd = dbSettings["password"]
                dbName = dbSettings["db"]
                thisConn = ms.connect(
                    host=host,
                    user=user,
                    passwd=passwd,
                    db=dbName,
                    port=port,
                    use_unicode=True,
                    charset='utf8',
                    client_flag=ms.constants.CLIENT.MULTI_STATEMENTS,
                    connect_timeout=3600
                )
                thisConn.autocommit(True)
                dbConns.append(thisConn)

            else:
                dbConns.append(None)

        # CREATE A DICTIONARY OF DATABASES
        dbConns = {
            "transients": dbConns[0],
            "catalogues": dbConns[1]
        }

        dbVersions = {}
        for k, v in list(dbConns.items()):
            if v:
                sqlQuery = u"""
                    SELECT VERSION() as v;
                """ % locals()
                rows = readquery(
                    log=self.log,
                    sqlQuery=sqlQuery,
                    dbConn=v,
                    quiet=False
                )
                version = rows[0]['v']
                dbVersions[k] = version
            else:
                dbVersions[k] = None

        self.log.debug('completed the ``get`` method')
        return dbConns, dbVersions

    def _setup_tunnel(
            self,
            tunnelParameters):
        """
        *setup a ssh tunnel for a database connection to port through*

        **Key Arguments**

        - ``tunnelParameters`` -- the tunnel parameters found associated with the database settings


        **Return**

        - ``sshPort`` -- the port the ssh tunnel is connected via


        .. todo ::

            - update key arguments values and definitions with defaults
            - update return values and definitions
            - update usage examples and text
            - update docstring text
            - check sublime snippet exists
            - clip any useful text to docs mindmap
            - regenerate the docs and check redendering of this docstring
        """
        self.log.debug('starting the ``_setup_tunnel`` method')

        from subprocess import Popen
        import time
        import sys

        # TEST TUNNEL DOES NOT ALREADY EXIST
        sshPort = tunnelParameters["port"]
        connected = self._checkServer(
            "127.0.0.1", sshPort)
        if connected:
            self.log.debug('ssh tunnel already exists - moving on')
        else:
            # GRAB TUNNEL SETTINGS FROM SETTINGS FILE
            ru = tunnelParameters["remote user"]
            rip = tunnelParameters["remote ip"]
            rh = tunnelParameters["remote datbase host"]

            cmd = "ssh -fnN %(ru)s@%(rip)s -L %(sshPort)s:%(rh)s:3306" % locals()
            p = Popen(cmd, shell=True, close_fds=True)
            output = p.communicate()[0]
            self.log.debug('output: %(output)s' % locals())

            # TEST CONNECTION - QUIT AFTER SO MANY TRIES
            connected = False
            count = 0
            while not connected:
                connected = self._checkServer(
                    "127.0.0.1", sshPort)
                time.sleep(1)
                count += 1
                if count == 5:
                    self.log.error(
                        'cound not setup tunnel to remote datbase' % locals())
                    sys.exit(0)
        return sshPort

    def _checkServer(self, address, port):
        """Check that the TCP Port we've decided to use for tunnelling is available

        .. todo ::

            - update key arguments values and definitions with defaults
            - update return values and definitions
            - update usage examples and text
            - update docstring text
            - check sublime snippet exists
            - clip any useful text to docs mindmap
            - regenerate the docs and check redendering of this docstring
        """
        self.log.debug('starting the ``_checkServer`` method')

        # CREATE A TCP SOCKET
        import socket
        s = socket.socket()
        self.log.debug(
            """Attempting to connect to `%(address)s` on port `%(port)s`""" % locals())
        try:
            s.connect((address, port))
            self.log.debug(
                """Connected to `%(address)s` on port `%(port)s`""" % locals())
            return True
        except socket.error as e:
            self.log.warning(
                """Connection to `%(address)s` on port `%(port)s` failed - try again: %(e)s""" % locals())
            return False

        return None

    # xt-class-method
