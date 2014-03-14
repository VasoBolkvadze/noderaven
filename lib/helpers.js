var _ = require('underscore');

module.exports.extractDataFromDynamicAggrResponse = function(response){
	return _.reduce(response.Results
		,function(memo,val,key){
			memo[key] = _.reduce(val.Values
				,function(m,v){
					m[v.Range] = v.Sum;
					return m;
				}
				,{});
			return memo;
		}
		,{});
};