Implements the CouchDB API for twisted.

## Release Notes (git trunk...will be 0.3)

* Added CouchDB authentication support (supply username and password args when instantiating)
* Re-factored code into formal Python package. API compatible with old package layout.

## Release Notes (0.1->0.2)

* Updated CouchDB support up to version CouchDB 1.0.1


## Known issues

* Doesn't keep connections alive between requests.

## Notes

This isn't under heavy maintenance by me, I only use a subset of the functionality and wrap the rest away in a non-portable internal library.  Please fork and make it better.


For David's initial repo, see https://launchpad.net/paisley.  David has asked me to make github the official repo since we're actively keeping it up with CouchDB version bumps.