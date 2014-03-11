#NodeRaven
###a simplified ravendb library for nodejs.
##usage
###var raven = require('nodeRaven');
###var store = raven('http://<server address>:<port>');
##api
* store.<b>indexQuery</b>(db, index, whereClause, start, limit, sortBys, fetchings, cb)
* store.<b>suggest</b>(db, index, term, field, cb)
* store.<b>facets</b>(db, indexName, facetDoc, query, cb)
* store.<b>load</b>(db, id, cb)
* store.<b>update</b>(db, id, doc, meta, cb)
* store.<b>patch</b>(db, id, operations, cb)
* store.<b>store</b>(db, entityName, doc, cb)