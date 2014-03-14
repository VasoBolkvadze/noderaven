var request = require('request'),
	_ = require('underscore');

module.exports = function (host) {
	if (host.indexOf('/', host.length - 1) == -1)
		host += '/';
	return {
		/** Queries index and returns results in callback(err,result : object{docs,stats}).
		 * @param {string} db
		 * @param {string} index
		 * @param {string} whereClause
		 * @param {int} start
		 * @param {int} limit
		 * @param {Array<string>} sortBys
		 * @param {Array<string>} fetchings
		 * @param {Function} cb
		 */
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
					cb(error || new Error(response.statusCode), null);
				}
			});
		},
		/** Queries index and returns  all indexResults in callback(err,result : object{docs,stats}).
		 * @param {string} db
		 * @param {string} index
		 * @param {string} whereClause
		 * @param {Array<string>} sortBys
		 * @param {Function} cb
		 */
		indexQueryAll: function (db, index, whereClause, sortBys, cb) {
			var query = encodeURIComponent(whereClause)
			var url = host +
				'databases/' + db +
				'/indexes/' + index +
				'?query=' + query +
				'&operator=AND&pageSize=0&start=0';
			urlPartSort = sortBys.map(function (prop) {
									return '&sort=' + prop;
								}).join('');
			request(url, function (error, response, body) {
				if (!error && response.statusCode === 200) {
					var result = JSON.parse(body);
					var pageSize = 1024;
					var stats = _.reduce(result, function (memo, value, key) {
						if (key !== "Results") {
							memo[key] = value;
						}
						return memo;
					}, {});
					var totalResults = stats.TotalResults;
					var pageCount = parseInt(totalResults / pageSize);
					var delta = totalResults - pageCount * pageSize;
					if (delta > 0)
						pageCount++;
					var requests = [];
					for (var i = 0; i < pageCount; i++) {
						requests.push({
							Url: '/indexes/' + index,
							Query: 'start=' + i * pageSize + '&pageSize=' + pageSize + '&query=' + query + urlPartSort
						});
					}
					var multiGetBody = JSON.stringify(requests);
					var multiGetUrl = host + 'databases/' + db + '/multi_get';
					request.post({
							url: multiGetUrl,
							body: multiGetBody
						},
						function (err, resp, multiGetResponseBody) {
							if (!err && resp.statusCode === 200) {
								var multiGetResponse = JSON.parse(multiGetResponseBody);
								var results =
									_.flatten(_.reduce(multiGetResponse
										, function (memo, val) {
											if (val.Result && val.Result.Results)
												memo.push(val.Result.Results);
											return memo;
										}
										, []));
								cb(null, results);
							} else {
								cb(err || new Error(resp.statusCode), null);
							}
						});
				}
				else {
					cb(error || new Error(response.statusCode), null);
				}
			});
		},
		/** Returns Suggestions for given query and fieldName.
		 * @param {string} db
		 * @param {string} index
		 * @param {string} term
		 * @param {string} field
		 * @param {Function} cb
		 */
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
					cb(error || new Error(response.statusCode), null);
				}
			});
		},
		/** Returns Facet Results.
		 * @param {string} db
		 * @param {string} indexName
		 * @param {string} facetDoc
		 * @param {string} query
		 * @param {Function} cb
		 */
		facets: function (db, indexName, facetDoc, query, cb) {
			var url = host + "databases/" + db + "/facets/" + indexName + "?facetDoc=" + facetDoc + "&query=" + encodeURIComponent(query);
			request(url, function (error, response, body) {
				if (!error && response.statusCode === 200) {
					var result = JSON.parse(body);
					_.each(result.Results, function (v, k) {
						if (_.filter(v.Values,function (x) {
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
					cb(error || new Error(response.statusCode), null);
			});
		},
		/** Generates and returns Dynamic Report.
		 * @param {string} db
		 * @param {string} indexName
		 * @param {string} whereClause
		 * @param {string} groupBy
		 * @param {Array<string>} fieldsToSum
		 * @param {Function} cb
		 */
		dynamicAggregation: function (db, indexName, whereClause, groupBy, fieldsToSum, cb) {
			var url = host + 'databases/' + db + '/facets/' + indexName + '?';
			url += whereClause ? '&query=' + encodeURIComponent(whereClause) : '';
			url += '&facetStart=0&facetPageSize=1024';
			var facets = fieldsToSum
				.map(function (field) {
					return {
						"Mode": 0,
						"Aggregation": 16,
						"AggregationField": field,
						"Name": groupBy,
						"DisplayName": field,
						"Ranges": [],
						"MaxResults": null,
						"TermSortMode": 0,
						"IncludeRemainingTerms": false
					};
				});
			url += '&facets=' + JSON.stringify(facets);
			request(url, function (error, response, body) {
				if (!error && response.statusCode === 200) {
					var result = JSON.parse(body);
					cb(null, result);
				} else {
					cb(error || new Error(response.statusCode), null);
				}
			});
		},
		/** Loads document with given id.
		 * @param {string} db
		 * @param {string} id
		 * @param {Function} cb
		 */
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
					cb(error || new Error(response.statusCode), null);
				}
			});
		},
		/** Overwrites given document with given id.
		 * @param {string} db
		 * @param {string} id
		 * @param {Object} doc
		 * @param {Object} metadata
		 * @param {Function} cb
		 */
		update: function (db, id, doc, metadata, cb) {
			var operations = [
				{
					Method: "PUT",
					Document: doc,
					Metadata: metadata,
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
					cb(error || new Error(response.statusCode), null);
				}
			});
		},
		/** Applies patch operations to document with given id.
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
					cb(error || new Error(response.statusCode), null);
				}
			});
		},
		/** Stores given document, returns raven generated id in callback.
		 * @param {string} db
		 * @param {string} entityName
		 * @param {Object} doc
		 * @param {Function} cb
		 */
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