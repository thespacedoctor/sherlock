# v2.0.0

* enhancement: annotations provided for transient webpages are now more readable readable and the user is given a link to an external resource where possible. e.g.

    > The transient is possibly associated with [SDSS J175627.38+033241.9](http://skyserver.sdss.org/dr12/en/tools/explore/Summary.aspx?id=1237668571478362594); a B=15.67 mag galaxy found in the SDSS/GLADE/2MASS/PS1 catalogues. It's located 5.62" S, 0.27" W from the galaxy centre. 

* fixed: sometimes in the catalogues saturated stars spawn multiple nearby catalogues entries, all marked as galaxies. In the SDSS catalogue these spurious 'galaxy' are sometimes detected well below the saturation limit of SDSS (12-14th) and so bypass our BS checker. This has been resolved in the MySQL database views.
* fixed: entries with null values in some magnitude columns in the new PS1 catalogue generating fake NT
* fixed: SDSS galaxies in the PhotoObjAll table with photoz getting prioritised over assoicated galaxies which have specz, but are at a great angukar distance than SDSS sources. 
* fixed: AGN given NT classifications instead of 'AGN'
