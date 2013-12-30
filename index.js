var request = require('request');
var _ = require('underscore');
function bulkPost(host, db, operations, cb) {
	request.post({
		url: host + 'databases/' + db + '/bulk_docs',
		headers:{
			'Content-Type': 'application/json; charset=utf-8'
		},
		body: JSON.stringify(operations)
	}, function (error, response, resBody) {
		if (!error && response.statusCode === 200) {
			var result = JSON.parse(resBody);
			cb(null, result);
		} else {
			cb(error || response.statusCode, null);
		}
	});
}
module.exports = function(host,db){
	return {
		indexQuery:function(index,whereClause,start,limit,sortBys,cb){
			var url = host +
				'databases/' + db +
				'/indexes/'+ index +
				'?&query='+ encodeURIComponent(whereClause) +
				'&operator=AND'+
				'&pageSize='+limit+
				'&start='+start;
			var sorts = sortBys.map(function(prop){
				return '&sort='+prop;
			}).join('');
			url = url + sorts;
			console.log(url);
			request(url, function (error, response, body) {
				if (!error && response.statusCode === 200) {
					var result = JSON.parse(body);
					var stats = _.reduce(result, function (memo, value, key) {
						if (key !== "Results") {
							memo[key] = value;
						}
						return memo;
					}, {});
					cb(null, {docs:result.Results,stats:stats});
				}
				else {
					cb(error || response.statusCode, null);
				}
			});
		},
		suggest:function(index,term,field,cb){
			var url = host +
				'databases/' + db +
				'/suggest/'+ index +
				'?term='+ encodeURIComponent(term) +
				'&field='+ field +
				'&max=10&distance=Default&accuracy=0.5';
			request(url, function (error, response, body) {
				if (!error && response.statusCode === 200) {
					var result = JSON.parse(body);
					console.log(result);
					cb(result.Suggestions);
				}
				else {
					cb(error || response.statusCode, null);
				}
			});
		},
		save:function(id,doc,meta,cb){
			var operations = [
				{
					Method: "PUT",
					Document:doc,
					Metadata:meta,
					Key: id
				}
			];
			bulkPost(host,db,operations,cb);
		}
	}
};