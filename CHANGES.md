
## Release Notes

* Speed improvements (5-10 times processing speed increase)
* Hidden `database-batch-size` and `cpu-pool-size` settings added - power users can access and change in settings files
* Default search algorithm now version-controlled and ship alongside code. Users can override the default algorithm if they need to.
* some URLs to NED objects, found in the human readable Sherlock annotations, that were not resolving now do. 

**v2.1.8 - April 16, 2021** `#bugFixFriday`

* **REFACTOR**: catalogue quality weights now used *within* the merged source sets to sort the data. This fixed *some* mis-matched annotation issues.
* **REFACTOR**: updating db connection settings to use QUB jumpbox.
* **REFACTOR/FIX** Reduced associated source merging radius from 3 arcsec to 1 arcsec (1/9th of the original matching area). This seems to correct many of the cases in which 2 or more distinct catalogued sources were getting merged/blended into one association. This resulted in in-correct classification prediction and wrongly associated distances.
* **FIXED**: annotation mismatches. If transient is classified as a SN then the underlying sources is identified as a galaxy in the annotation.

**v2.1.7 - September 29, 2020**

* **FIXED**: lite version introduced a couple of small bugs. Fixed.

**v2.1.6 - September 7, 2020**

* **ENHANCEMENT**: transient human-readable annotations added to the `classifications` dictionary return of the `transient_classifier.classify()` method.
* **ENHANCEMENT**: a `lite` parameter has been added to the Sherlock `transient_classifier` object. When set to `True` only top-ranked crossmatches are returned with a limited column set.
* **ENHANCEMENT**: snapshot annotation is now added into the classification dictionary return. The classification dictionary is now `{'MyTransientId01': ['Type', 'Annotation'], 'MyTransientId02': ['Type', 'Annotation'],}`
* **REFACTOR**: small change to query that writes NED source magnitudes that vastly improves speed
* **FIXED**: small issue where a missing error in photoz was causing annotations to not complete

**v2.1.5 - June 22, 2020**

* **refactor**: tunnel changes due to new jump box installation at QUB
* **fixed:** some NED galaxies not reported if redshift does not exist but a semi-major axis is given

**v2.1.4 - June 18, 2020**

* **fixed**: an empty crossmatch set could cause a transient database update when not required

**v2.1.3 - June 5, 2020**

* **refactor**: stop sherlock checking for transient database triggers if running in non-update mode
* **fixed**: another sdss photoz issue where photoz ranking about specz

**v2.1.2 - May 24, 2020**

* **fixed:** merged result parameters are now merged correctly

**v2.1.1 - May 21, 2020**

* **fixed:** fixes to cl utils

**v2.1.0 - May 20, 2020**

* Now compatible with Python 3.*
