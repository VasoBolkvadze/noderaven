var request = require('request');
var _ = require('underscore');

module.exports = function (host) {
	return {
		indexQuery: function (db, index, whereClause, start, limit, sortBys, fetchings, cb) {
			if (typeof whereClause === 'function') {
				cb = whereClause;
				whereClause = '';
				start = 0;
				limit = 12;
				sortBys = [];
				fetchings = [];
			}
			if (typeof start === 'function') {
				cb = start;
				start = 0;
				limit = 12;
				sortBys = [];
				fetchings = [];
			}
			if (typeof sortBys === 'function') {
				cb = sortBys;
				sortBys = [];
				fetchings = [];
			}
			if (typeof fetchings === 'function') {
				cb = fetchings;
				fetchings = [];
			}
			var url = host +
				'databases/' + db +
				'/indexes/' + index +
				'?query=' + encodeURIComponent(whereClause) +
				'&operator=AND' +
				'&pageSize=' + limit +
				'&start=' + start;
			var sorts = sortBys.map(function (prop) {
				return '&sort=' + prop;
			}).join('');
			var fetchs = fetchings.map(function (prop) {
				return '&fetch=' + prop;
			}).join('');
			url = url + sorts + fetchs;
			request(url, function (error, response, body) {
				if (!error && response.statusCode === 200) {
					var result = JSON.parse(body);
					var stats = _.reduce(result, function (memo, value, key) {
						if (key !== "Results") {
							memo[key] = value;
						}
						return memo;
					}, {});
					cb(null, {docs: result.Results, stats: stats});
				}
				else {
					cb(error || response.statusCode, null);
				}
			});
		},
		suggest: function (db, index, term, field, cb) {
			var url = host +
				'databases/' + db +
				'/suggest/' + index +
				'?term=' + encodeURIComponent(term) +
				'&field=' + field +
				'&max=10&distance=Default&accuracy=0.5';
			request(url, function (error, response, body) {
				if (!error && response.statusCode === 200) {
					var result = JSON.parse(body);
					cb(null, result);
				}
				else {
					cb(error || response.statusCode, null);
				}
			});
		},
		facets: function(db, indexName, facetDoc, query, cb){
			var url = host + "databases/" + db + "/facets/" + index + "?facetDoc=" + facetDoc + "&query=" + encodeURIComponent(query);
			request(url, function (error, response, body) {
				if (!error && response.statusCode === 200) {
					var result = JSON.parse(body);
					_.each(result.Results, function (v, k) {
						if (_.filter(v.Values, function (x) {
							return x.Hits > 0;
						}).length < 2) {
							delete result.Results[k];
							return;
						}
						v.Values = _.chain(v.Values)
							.map(function (x) {
								var val = JSON.stringify(x.Range)
											.replace(/^\"|\"$/gi, "")
											.replace(/\:/gi, "\\:")
											.replace(/\(/gi, "\\(")
											.replace(/\)/gi, "\\)");
								if (x.Range.indexOf(" TO ") <= 0 || x.Range.indexOf("[") !== 0) {
									val = val.replace(/\ /gi, "\\ ");
								}
								val = encodeURIComponent(val);
								x.q = k + ":" + val;
								x.Range = x.Range
										.replace(/^\[Dx/, "")
										.replace(/ Dx/, " ")
										.replace(/\]$/, "")
										.replace(/ TO /, "-");
								return x;
							}).filter(function (x) {
								return x.Hits > 0;
							}).sortBy(function (x) {
								return x.Range;
							}).value();
					});
					cb(null, result.Results);
				} else
					cb(error || response.statusCode, null);
			});
		},
		load: function (db, id, cb) {
			var url = host + 'databases/' + db + '/docs/' + id;
			request(url, function (error, response, body) {
				if (!error && response.statusCode === 200) {
					var doc = JSON.parse(body);
					var meta = _.reduce(response.headers
						, function (memo, val, key) {
							if (key.startsWith('raven'))
								memo[key] = val;
							return memo;
						}, {});
					meta['@id'] = response.headers['__document_id'];
					meta.etag = response.headers['etag'];
					doc['@metadata'] = meta;
					cb(null, doc);
				} else {
					cb(error || response.statusCode, null);
				}
			});
		},
		update: function (db, id, doc, meta, cb) {
			var operations = [
				{
					Method: "PUT",
					Document: doc,
					Metadata: meta,
					Key: id
				}
			];
			request.post({
				url: host + 'databases/' + db + '/bulk_docs',
				headers: {
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
		},
		/** Applies patch operations to given document.
		 * @param {string} db
		 * @param {string} id
		 * @param {Array<Object>} operations
		 * @param {Function} cb
		 */
		patch: function (db, id, operations, cb) {
			request.patch({
				url: host + 'databases/' + db + '/docs/' + id,
				headers: {
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
		},
		store: function (db, entityName, doc, cb) {
			request.post({
				url: host + 'databases/' + db + '/docs',
				headers: {
					'Content-Type': 'application/json; charset=utf-8',
					'Raven-Entity-Name': entityName
				},
				body: JSON.stringify(doc)
			}, function (error, response, resBody) {
				if (!error && response.statusCode === 201) {
					var result = JSON.parse(resBody);
					cb(null, result);
				} else {
					cb(error || new Error(response.statusCode), null);
				}
			});
		}
	}
};