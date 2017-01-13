Command-Line Tutorial
=====================

Before you begin using sherlock you will need to populate some custom settings within the sherlock settings file.

To setup the default settings file at ``~/.config/sherlock/sherlock.yaml`` run the command:

.. code-block:: bash 
    
    sherlock init

This should create and open the settings file; follow the instructions in the file to populate the missing settings values (usually given an ``XXX`` placeholder). 

The Settings File
-----------------

Before you begin you will need to run the following code once to set a login-path for each of your mysql servers:

.. code-block:: bash 

    mysql_config_editor set --login-path=<uniqueLoginName> --host=localhost --user=<myUsername> --password --port=<port>

This stores your credentials in an encrypted file located at '~/.mylogin.cnf'.
Use `mysql_config_editor print --all` to see all of the login-paths set.


.. todo::

    - add tutorial
