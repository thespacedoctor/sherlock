
## Release Notes

**v3.0.4 October 30, 2025**

* **REFACTOR**: Increasing the 'galaxy stretch factor" from 1.5 to 2.5. This increases the catch radius for galaxies with shape parameters. Some SN on the edges of their galaxies were not getting associated.
* **REFACTOR**: Removing synonym matching from typical, photometry-only (no distance) galaxy searches (too many transients incorrectly flagged as NT). Transients in the core of angularly large galaxies and galaxies with a distance measurement still get an NT classification.
* **REFACTOR**: Default synonym radius reduced from 1.5" to 0.5".
* **REFACTOR**: Revised UNCLEAR classifications from synonym to association. This enables more certain classifications to rise to the top of the ranking algorithm.
* **REFACTOR**: updated the 'denied galaxy' code to exclude individually flagged bad catalogue data.

**v3.0.3 October 13, 2025**

* **FIXED**: fixed a ranking error when comparing a VS classification against a nearby NT (NT should win).

**v3.0.2 October 7, 2025** 

* **FIXED**: Adding minimum versions of some dependencies.

**v3.0.1 October 3, 2025** 

* **REFACTOR**: small optimisation

**v3.0 October 2, 2025** 

* **ENHANCEMENT**: 'distance' has been removed from Sherlock's outputs and replaced with 'best_distance', 'best_distance_flag' and 'best_distance_source'. 'distance' was calculated from the spectroscopic redshift (luminosity distance), but this was not clear to the end-user. Sherlock now processes through a distance measurement list for each transient and reports a single 'best_distance' in Mpc. From best to worst, this list is direct distance (redshift independent), spectroscopic redshift (converted to Mpc), and photometric redshift (converted to Mpc). A 'best_distance_flag' indicates how the best distance is derived (`dd`, `sz`, `pz`). The 'best_distance_source' reveals which catalogue is to blame for the distance measurement.
* **ENHANCEMENT**: DESI Legacy Survey DR10 added to the default algorithm.
* **ENHANCEMENT**: Gaia DR2 is replaced by Gaia DR3 in the default algorithm.
* **ENHANCEMENT**: Upgraded Milliquas from v5.2 to v8 (final version of the catalogue). Milliquas algorithm has also been refined.
* **REFACTOR**: 2MASS PSC algorithm refined
* **REFACTOR**: added indexes to GSC and SDSS PhotoObjAll MySQL tables to improve query speeds.
* **REFACTOR**: database and code optimisations to make Sherlock 5-10 times faster.
* **REFACTOR**: increasing the default 'galaxy radius stretch factor', used to multiply a galaxy semi-major axis to get a search radius, from 1.2 to 1.5
* **FIXED**: a bug in the synonym matching, where the default synonym radius (1.5") was getting used even if a search modules synonym was smaller.
* **FIXED**: a bug when using 'mag column' within a yaml search module, but then only giving a 'general' search filter (no bright and faint filters). No matches are return even if a positive match exists, resulting in an 'ORPHAN' classification.
* **FIXED**: extended sources have been removed from the 2MASS PSC search


**v2.3.1 - October 10, 2023** 

* **REFACTOR**: increasing precision of reported catalogue values from 2dp to 3dp when running in lite mode. Note values in full mode are still reported with the same precision as found in the source catalogues. 

**v2.3.0 - September 21, 2023** 

* **ENHANCEMENT**: updating NED-D import and moving NED-D catalogue to v17.1.2
* **FIXED**: NED-D catalogue >= v17.1.2 now reports the median redshift independent distance (not the minimum)

**v2.2.4 - August 28, 2023** 

* **FIXED**: increasing the brightness threshold for which a PS source identified as a galaxy is flagged as a bright star by Sherlock

**v2.2.3 - April 3, 2023** 

* **FIXED**: GAIA DR2 stars not getting matched as synonyms in default algorithm.  

**v2.2.2 - May 18, 2022** 

* **FIXED**: NED name parsing errors fixed

**v2.2.1 - January 21, 2022** 

* **ENHANCEMENT**: added separate treatment of galaxies which extend > 10 arcmin in the sky. Search radii are reduced to reduce the chance of a background source getting incorrectly associated with these local neighbours.  
* **ENHANCEMENT**: added a 'deny list' in settings for galaxies with incorrect morphology reports in NED (only morphology ignored, all other metadata read as usual). So far list members only include "Sagittarius Dwarf Spheroidal" and "WISEA J193037.70-521726.0"  
* **REFACTOR**: reduced cl chattiness when verbosity is set to 0.  
* **FIXED**: synonym matching was not switched off in hidden settings  
* **FIXED**: semi major axis (diameter) now divided by 2 (to get radius) before comparing with separations to determine associations.   
* **FIXED**: typo in 'galaxy radius stretch factor' (was 'galaxy radius stetch factor')  


**v2.2.0 - August 2, 2021** 

*  **FEATURE**: Default search algorithm now version-controlled and ship alongside code. Users can override the default algorithm if they need to.
* **FEATURE**: Hidden `database-batch-size` and `cpu-pool-size` settings added - power users can access and change in settings files.
* **ENHANCEMENT**: Speed improvements (5-10 times processing speed increase).
*  **REFACTOR/FIX**:  synonym match break now removed by default so a location now gets matched against all catalogues regardless of whether or not a synonym match is initially found (e.g. right on top of a catalogued star). Resolves edge cases where true SNe are mis-classified as VS as they are located just too close to a stellar source, or galaxy source mis-identified as stellar in one catalogue but correctly as galaxy in another.
*  **FIX**: some URLs to NED objects, found in the human readable Sherlock annotations, that were not resolving now do. 

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
