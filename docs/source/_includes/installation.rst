Installation
============

Although you can get Sherlock from a simple ``pip`` install, it's best to install it within a Conda environment under Anaconda. If you're not familiar with Anaconda, you'll find a `good tutorial here <http://astronotes.co.uk/blog/2017/10/04/An-Astronomer's-Guide-to-dotstar-Conda.html>`_ to get you up and running. 

Once you have Anaconda installed, go ahead and create a new Conda environment to host Sherlock:

.. code:: bash

    conda create -n sherlock python=2.7 pip

Now activate the environment and install sherlock:

.. code:: bash

    source activate sherlock
    pip install qub-sherlock

At any point in the future you can upgrade to the latest version of sherlock with the command:

.. code:: bash

    pip install qub-sherlock --upgrade
    
If instead you want to clone the `github repo <https://github.com/thespacedoctor/sherlock>`__ and install from a local version of the code:

.. code:: bash

    git clone git@github.com:thespacedoctor/sherlock.git
    cd sherlock
    source activate sherlock
    python setup.py install





