sherlock 
=========================

*A contextual classifier for astronomical transient sources. Sherlock mines a library of historical and on-going survey data to search for the source of a transient event, and predicting the classification of the event based on that associated data.*.

Usage
======

.. code-block:: bash 
   
    sherlock match [--update] [-s <pathToSettingsFile>]
    sherlock clean [-s <pathToSettingsFile>]
    sherlock wiki [-s <pathToSettingsFile>]

    -h, --help            show this help message
    -v, --version         show version
    -s, --settings        the settings file
    -l, --transientlistId          the id of the transient list to classify
    -u, --update          update the transient database with new classifications and crossmatches
    
Documentation
=============

Documentation for sherlock is hosted by `Read the Docs <http://sherlock.readthedocs.org/en/stable/>`__ (last `stable version <http://sherlock.readthedocs.org/en/stable/>`__ and `latest version <http://sherlock.readthedocs.org/en/latest/>`__).

Installation
============

The easiest way to install sherlock us to use ``pip``:

.. code:: bash

    pip install sherlock

Or you can clone the `github repo <https://github.com/thespacedoctor/sherlock>`__ and install from a local version of the code:

.. code:: bash

    git clone git@github.com:thespacedoctor/sherlock.git
    cd sherlock
    python setup.py install

To upgrade to the latest version of sherlock use the command:

.. code:: bash

    pip install sherlock --upgrade


Development
-----------

If you want to tinker with the code, then install in development mode.
This means you can modify the code from your cloned repo:

.. code:: bash

    git clone git@github.com:thespacedoctor/sherlock.git
    cd sherlock
    python setup.py develop

`Pull requests <https://github.com/thespacedoctor/sherlock/pulls>`__
are welcomed!


Issues
------

Please report any issues
`here <https://github.com/thespacedoctor/sherlock/issues>`__.

License
=======

Copyright (c) 2016 David Young

Permission is hereby granted, free of charge, to any person obtaining a
copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be included
in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

