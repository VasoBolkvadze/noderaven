#NodeRaven
###a simplified ravendb library for nodejs.
##usage
###var raven = require('nodeRaven');
###var store = raven('http://ipaddress:port','dbname');
##api
* store.<b>indexQuery</b>(indexName,luceneQuery,start,limit,sortBys,cb)
  * indexName : string (name of the index)
  * luceneQuery : string (where clause, lucene syntax)
  * start : int (start index of records)
  * limit : int (count of records to be retrieved)
  * sortBys : array\<string\> (orderby column names)
  * cb : function(err,result) - result : object {docs,stats}
* store.<b>suggest</b>(indexName,term,fieldName,cb)
* store.<b>save</b>(id,document,metadata,cb);