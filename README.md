Implements the CouchDB API for twisted.

Known issues:

- Doesn't keep connections alive between requests.

This isn't under heavy maintence by me, I only use a subset of the functionality and wrap the rest away in a non-portable internal library.  Please fork and make it better.


For the initial repo, see http://github.com/smcq/paisley.  David has asked me to make this the official repo since we're actively keeping it up with couchdb version bumps.