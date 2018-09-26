Sherlock Tutorial
=================

Before you begin using sherlock you will need to populate some custom parameters within the sherlock settings file.

To setup the default settings file at ``~/.config/sherlock/sherlock.yaml`` run the command:

.. code-block:: bash 
    
    sherlock init

This should create and open a new config file; follow the instructions in the file to populate the missing parameters values (usually given an ``XXX`` placeholder). 


.. todo::

    - add tutorial

Initialisation and Setup
------------------------

Populating Sherlock's Settings File
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The settings file now contains every option required to change the way the code runs, including database settings and the actual search algorithm. 


Database Settings
^^^^^^^^^^^^^^^^^

.. code-block:: yaml

    database settings:
        static catalogues:
            db: crossmatch_catalogues
            host: 127.0.0.1
            user: pessto
            password: p355t0

        transients:
            user: pessto
            password: p355t0
            db: ps13pipublic
            host: 127.0.0.1
            transient table: tcs_transient_objects
            transient query: "select id as 'id', followup_id as 'alt_id', ra_psf 'ra', dec_psf 'dec', local_designation 'name', object_classification as 'object_classification'
                from tcs_transient_objects
                where detection_list_id = 2
                and object_classification is null
                order by followup_id"
            transient id column: id
            transient classification column: object_classification

        pessto marshall:
            user: pessto
            password: p355t0
            db: pessto_marshall
            host: 127.0.0.1

The `static catalogues` settings are the settings for connecting to the static catalogues database. Do not edit these settings unless you know what you're doing. If you have your RSA key on starbase, the code will setup a ssh-tunnel for you so that you can connect to this database remotely.

The `transients` settings are for the database you have your transients stored in. `transient table` is the name of the table containing your transients, `transient query` is the SQL query that need executed to get the following info for the transients needing classified:

* `id` - the primary ID for the transient in the database
* `alt_id` - human readable name (optional)
* `ra` - the ra of the object
* `dec` - the dec of the object
* `name` - a further alt id (optional)

The `transient id column` is the primary ID column in the transient database and `transient classification column` is the column you wish to add the classification to.

The Search Algorithm
^^^^^^^^^^^^^^^^^^^^

The order searches appear in the `search algorithm` section is the order they shall be run in the actual code:

.. code-block:: yaml

    search algorithm:
        sdss qso:
            angular radius arcsec: 2.0
            transient classification: AGN
            database table: tcs_cat_v_sdss_dr9_spect_qsos
        milliquas:
            angular radius arcsec: 3.0
            transient classification: AGN
            database table: tcs_cat_milliquas
        veron:
            angular radius arcsec: 2.0
            transient classification: AGN
            database table: tcs_veron_cat
        ned qso:
            angular radius arcsec: 2.0
            transient classification: AGN
            database table: tcs_cat_v_ned_qsos
        ned nt:
            angular radius arcsec: 3.0
            physical radius kpc: 0.5
            transient classification: NT
            database table: tcs_cat_v_ned_galaxies
        sdss spec nt:
            angular radius arcsec: 3.0
            physical radius kpc: 0.5
            transient classification: NT
            database table: tcs_cat_v_sdss_dr9_spect_galaxies
        sdss phot nt: 
            angular radius arcsec: 0.5
            transient classification: NT
            database table: tcs_cat_v_sdss_dr9_galaxies_notspec
        ...

The first time you run `sherlock` you will be told to add your settings to the empty settings file that's been created in `~/.config/sherlock/sherlock.yaml`.

For details about all of the catalogue in the catalogues database, run:

.. code-block:: bash 
    
    sherlock info 



Classifying Transients
----------------------


A Single Transient Classification
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Classifying Transients in a Transient Database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


The Classification Workflow
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. figure:: https://camo.githubusercontent.com/dd84c3c74b99d24d1343a9ab29ca289ee2f16c9f/68747470733a2f2f692e696d6775722e636f6d2f546147693970622e706e67

Synonyms vs Associations
^^^^^^^^^^^^^^^^^^^^^^^^

Sherlock distinguishes between what it views as transient objects
synonymous with a catalogued source (the same as or very closely linked
to), *synonyms*, and those it deems as merely associated with the
catalogued source, *associations*.

Examples of transient-synonym matches are CVs, AGN and variable stars
(VS) that match within 1-2 arcsec of their catalogue counterpart.
Stretching the definition of *synonym* a little, Sherlock will also
match transients close to the centre of galaxies as synonyms [1]_.
Transient-associations include those transients that are located near,
but not on top of, a catalogued source. Example of these associations
are 'transients' matching close to bright-stars and are classified as
bright-star artefacts (BS) resulting from poor image subtractions near
bright stars (:math:`~>14-16^{th}` mag) or transients matched near to a galaxy
which may be classified as supernovae (SN). By definition synonyms are a
more secure match than associations.

Each search algorithm module should contain a *synonym* and an
*association* key-value sets. For example here is a Guide-Star Catalogue
search module:

.. code-block:: yaml 
    
    gsc bright stars:
     angular radius arcsec: 100.0
     synonym: VS
     association: BS
     database table: tcs_view_star_guide_star_catalogue_v2_3
     bright mag column: B
     bright limit: 16. 

If a transient is matched on top of a source in the GSC it's identified as a synonym and classified as a variable star, but if it is match near to the source but not co-located if may been identified as an association and classified as a potential bright-star artefact (BS).


There's also a top-level ``synonym radius arcsec`` parameter in the
Sherlock settings file that defines the maximum transient-catalogue
source separation that secures a synonym identification.

.. code-block:: yaml 
    
    synonym radius arcsec: 0.5

Sherlock performs a two-staged catalogue match, first looking for
synonym matches and then for associations. For an individual transient
if a synonym match is found within the first search stage the second
search stage for associations is skipped as it becomes irrelevant. For
example consider the image below (transients marked in red):

figure:: https://farm3.staticflickr.com/2772/33007793206_6dd3e34a21_o.jpg%20title=%22Sherlock%20synonyms%20and%20associations%22%20width=600px

The first stage search should match transients A, C and E as synonyms
(NT, VS, VS), these transients are then removed from a further
association search. The second stage search then flags B as associated
with the large galaxy at the centre of the image and transient D as
either associated with the bright-star in the bottom right corner of the
image or with the galaxy in the centre.

.. [1]
   could be classified as a nuclear transient or supernova depending on
   search algorithm parameters

NED Stream Updater
^^^^^^^^^^^^^^^^^^

The settings in the settings file relating to the NED stream are:

.. code-block:: yaml

    ned stream search radius arcec: 300
    first pass ned search radius arcec: 240
    ned stream refresh rate in days: 90


To update the NED stream, for each transient coordinates the code does a conesearch on the `tcs_helper_ned_query_history` table to see if a search has already been performed within the designated `ned stream refresh rate in days`. If a match isn't found then NED is queried and the `tcs_helper_ned_query_history` is updated for the transient coordinates.

Search Algoritm
^^^^^^^^^^^^^^^

The algorithm is written and modified within the `sherlock.yaml` settings file. This means you can modify the algorithm without affecting anyone else's search (as long as you are working off the different transient databases).

.. code-block:: yaml

    search algorithm:
        sdss qso:
            angular radius arcsec: 2.0
            transient classification: AGN
            database table: tcs_view_qso_sdss_spect_galaxies_qsos_dr12
            stop algorithm on match: False
            match nearest source only: False
        milliquas:
            angular radius arcsec: 3.0
            transient classification: AGN
            database table: tcs_view_agn_milliquas_v4_5
            stop algorithm on match: False
            match nearest source only: False
        veron:
            angular radius arcsec: 2.0
            transient classification: AGN
            database table: tcs_view_agn_veron_v13
            stop algorithm on match: False
            match nearest source only: False
        ned qso:
            angular radius arcsec: 2.0
            transient classification: AGN
            ...

Note, to remove a module temporarily, simply comment it out in the settings file (yaml treats lines beginning with `#` as comments).

Behind the scenes there are 2 types of searches performed on the catalogues.

1. Angular Separation Search
2. Physical Separation Search

Angular Separation Search
^^^^^^^^^^^^^^^^^^^^^^^^^

An example of an angular separation search looks like this in the settings file:

.. code-block:: yaml

    milliquas:
        angular radius arcsec: 2.0
        transient classification: AGN
        database table: tcs_view_agn_milliquas_v4_5
        stop algorithm on match: False
        match nearest source only: False

The code performs a cone-search on `database table` using the `angular radius arcsec`. If matches are found the associated transient is given a `transient classification` and the results are added to the `tcs_cross_matches` table of the transients database. If `stop algorithm on match` is true the code breaks out of the search algorithm and starts afresh with the next transient to be classified, otherwise the algorithm contines and all matches are recorded in the `tcs_cross_matches` table. If `match nearest source only` is true only the closest match from each catalogue query is be recorded in the `tcs_cross_matches` table.

Physical Separation Search
^^^^^^^^^^^^^^^^^^^^^^^^^^

If the `physical radius kpc` key is found in the conesearch module then a physical separation search is performed. First of all an angular cone-search is performed at the coordinates using a suitably large search radius. After this a further search is done on the physical distance parameters returned (distance, physical separation distance, semi-major axis length ...) for each match.

A physical match is found if:

* The transient falls within 1.5 x semi-major axis of a galaxy
* The transient is within the `physical radius kpc` of a galaxy

As before, all matches are recorded in the `tcs_cross_matches` table.


Classification Rankings
^^^^^^^^^^^^^^^^^^^^^^^

If transients are found:

* within 2.0 arc of source, **OR**
* within 20 kpc of host galaxy **AND** assigned a SN classification, **OR**
* within 1.2 times the semi-major axis of the host **AND** assigned a SN classification

they are all given the same top level ranking for classification. After this catalogue weights come into effect to determine the orders of classifications. The catalogue weights are found in the [`tcs_helper_catalogue_tables_info`](Crossmatch Catalogue Tables) table of the catalogues database and give an indication of the accuracy of the classifications of sources in the catalogue. For example the `tcs_cat_sdss_spect_galaxies_qsos_dr12` is given a greater weight than `tcs_cat_sdss_photo_stars_galaxies_dr12` as classifications of the objects based on spectral observations is more accurate than photometry alone.

Once the classifications for each individual transient are ranked, a final, ordered classification listing is given to the transient within its original database table. For example `SN/VARIABLE STAR` means the the transient is most likely a SN but may also be a variable star.

A transient is matched against a source in the sherlock-catalogues because it is either synonymous with a point-like catalogue source (e.g. a variable star or an AGN) or it is hosted by the catalogue source (e.g. supernova, nuclear transient).

A synonymous crossmatch is always a simple angular crossmatch with a search radius that reflects the astrometric accuracy of the RMS combined astrometric errors of the transient source location and that of the catalogue being matched against.  


Sherlock's Catalogue Database
-----------------------------

Database Table Naming Scheme
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There's a [strict table naming syntax for the crossmatch-catalogues](Crossmatch-Catalogues Database Scheme) database to help deal with catalogue versioning (as updated versions of out sherlock-catalogues are released) and to help ease the burden of modifying crossmatch algorithms employed.

[See here for an up-to-date list of the crossmatch-catalogues](Crossmatch Catalogue Tables) and the [views](Crossmatch Catalogue Views) found on those tables.

Table Classes
^^^^^^^^^^^^^

There are 4 classes of tables in the `crossmatch_catalogues` database:

| Table Type  | Prefix | Notes | Example |
| :------------ | :----------- | :----------- | :----------- | 
| Catalogue     | `tcs_cat`  | The table is named with the scheme `tcs_cat_` <catalogue name> <version> | `tcs_cat_ned_d_v10_2_0` |
| View     | `tcs_view`  | The view is named with the scheme `tcs_view_` <object type contained> <source table name> | `tcs_view_galaxies_ned_d` |
| Helper     | `tcs_helper`  | Mostly used to store relational information, notes on database tables and book-keeper info | `tcs_helper_catalogue_tables_info` |
| Legacy     | `legacy_tcs_`  | Legacy tables used in previous incarnations of the transient classifier | `legacy_tcs_cat_md01_chiappetti2005` |

Versioning
^^^^^^^^^^

Each catalogue is versioned by appending a version indicator to the end of the table name. There are 3 indicator types:

1. `_final` to show that the catalogue is now at it's final version and shall remain unchanged.
2. `_stream` to show that the catalogue is constantly being updated
3. `_vX_X` to show a version number for the catalogue, e.g. for v10.2 this would be `_v10_2`. We can also have data-release versions (e.g. `_dr12`).


Maintainance and Updates of Catalogues Database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. todo::

    - write about marshall stream updates
    - write about helper table updates
    - write that some tasks need automated

There are various cron-scripts that run on PESSTO-VM03 to automate some tasks. These tasks include 

* updating of data-streams into the crossmatch-catalogues database and 
* the updates of certain helper tables in the crossmatch-catalogues database.

Currently there are scripts running every:

* 5 mins
* 30 mins
* 1 hr
* 3 hrs
* 12 hrs
* 24 hrs
  

  
Updating Catalogues and Adding New Catalogues to the Database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. todo::

    - list current catalogue importers and how to use them
    - add tutorial about creating a new importer
    -  add steps for adding a catalogue to the database and the search algorithm
    -  add details about updating the column map
    - write code into conf.py to generate tables for docs and link them from here (views, tables and streams)
      
Using the `sherlock-import` command it's possible to **import and update various catalogues and data-streams** including Milliquas, Veron AGN and the NED-D catalogues. [See here for details](Catalogue Importers). 

.. code-block:: bash

    sherlock-importers cat <cat_name> <pathToDataFile> <cat_version> [-s <pathToSettingsFile>]
    sherlock-importers stream <stream_name> [-s <pathToSettingsFile>]

The command to **import new versions of catalogues** and **data streams** into the `crossmatch_catalogues` database is:

.. code-block:: python 
    
    Usage:
        sherlock-importers cat <cat_name> <pathToDataFile> <cat_version> [-s <pathToSettingsFile>]
        sherlock-importers stream <stream_name> [-s <pathToSettingsFile>]

For example:

.. code-block:: bash

    > sherlock-importers cat milliquas ~/Desktop/milliquas.txt 4.5
    1153111 / 1153111 milliquas data added to memory
    1153111 / 1153111 rows inserted into tcs_cat_milliquas_v4_5
    5694 / 5694 htmIds added to tcs_cat_milliquas_v4_5

The command currently supports imports for the following **catalogues**:

* Milliquas
* Veron AGN
* NED-D

Using the command:

.. code-block:: bash

    sherlock-importers stream pessto

will import all of the various **data-streams** added to the PESSTO marshall (ASASSN, CRTS, LSQ, PSST ...).


THE COLUMN MAP LIFTED FROM `tcs_helper_catalogue_tables_info` TABLE IN CATALOGUE DATABASE (COLUMN NAMES ENDDING WITH 'ColName')

