Installation
============

The easiest way to install sherlock is to use ``pip``:

.. code:: bash

    pip install qub-sherlock

Or you can clone the `github repo <https://github.com/thespacedoctor/sherlock>`__ and install from a local version of the code:

.. code:: bash

    git clone git@github.com:thespacedoctor/sherlock.git
    cd sherlock
    python setup.py install

To upgrade to the latest version of sherlock use the command:

.. code:: bash

    pip install qub-sherlock --upgrade



.. todo::

    - make a note about how to setup mysql login paths and have them associated with the database setting in the sherlock settings file

.. code:: bash

    mysql_config_editor set --login-path=xxx --host=127.0.0.1 --user=myuser --password --port=xxx

