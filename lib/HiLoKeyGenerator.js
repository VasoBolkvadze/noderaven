
// `var hilo = require('HiLoKeyGenerator.js');`
// Creating your instance
// ======================
// `var keygen = hilo(options);`
// ####Options:
//      {
//        client: Client instance
//        keySeparator: Separator character used in keys
//                      (default: /)
//        capacity: The number of keys to reserve in each batch
//                  (default: 32)
//      }

function HiLoKeyGenerator(options) {
	if (!options || !options.client) {
		throw new Error('You must specify the client when creating HiLoKeyGenerator');
	}

	for (var i in options) {
		if (options.hasOwnProperty(i)) {
			this[i] = options[i];
		}
	}

	if (!this.keySeparator) {
		this.keySeparator = '/';
	}
	if (!this.capacity) {
		this.capacity = 32;
	}

	this.documentPrefix = 'Raven/Hilo/';
	this.entityGenerators = {}; // Hold each entity type's generator
}

// Generate a new document key for the provided entity type
// ========================================================
// `keygen.generateDocumentKey(entityType, [callback]);`
// ####Parameters:
// `entityType` - The type of document
// ####Callback:
//      function (error, result)
//
//      result:
//      {
//        key: The generated key
//      }
HiLoKeyGenerator.prototype.generateDocumentKey = function(entityType, callback) {
	var self = this;
	self._nextId(entityType, function(error, id) {
		if (error) {
			return callback && callback(error, null);
		}
		return callback && callback(undefined, {
			key: entityType + self.keySeparator + id
		});
	});
};

// Internal functions
// ==================

// _nextId
// -------
// Find the next available id, if we have used our range up, then fetch a new range from the server
HiLoKeyGenerator.prototype._nextId = function(entityType, callback) {
	var self = this,
		generator;

	if (!self.entityGenerators[entityType]) {
		// First time generating this entity, create a placeholder document before fetching it from the server.
		self.entityGenerators[entityType] = {
			lastId: 0,
			currentMax: 0
		};
	}

	// Increment our current then validate against the max
	generator = self.entityGenerators[entityType];
	generator.lastId += 1;
	if (generator.lastId > generator.currentMax) {
		// We have exceeded the currently assigned limit, get a new limit
		self._getNextMax(entityType, function(error) {
			if (error) {
				return callback && callback(error);
			}

			generator.lastId += 1;
			return callback && callback(undefined, generator.lastId);
		});
	} else {
		// We are still in range, so just send it back
		return callback && callback(undefined, generator.lastId);
	}
};

// _getNextMax
// -------
// Query the server for the next reserved id range for these entities
HiLoKeyGenerator.prototype._getNextMax = function(entityName, callback) {
	var self = this;
	self.client
		.load(self.database
			,self.documentPrefix + entityName
			,function(error,result){
				var doc, lastMax;
				doc = result;
				if (doc) {
					if (doc.ServerHi) {
						// We have the older format ServerHi document, convert to the new Max which supports changing capacity
						doc.max = (doc.ServerHi - 1) * self.capacity;
						doc.ServerHi = undefined;
					}
					lastMax = doc.max;
					doc.max += self.capacity;
					self.client
						.update(self.database
								,self.documentPrefix + entityName
								,doc
								,{}
								,function(error,result){
									if (error || !result) {
										return callback && callback(error);
									}
									console.log('result',result);
									self.entityGenerators[entityName].currentMax = doc.max;
									self.entityGenerators[entityName].lastId = lastMax;
									return callback && callback(undefined);
								});
				}
				else {
					// Doc doesn't exist on server, so create a new one
					doc = {
						max: self.capacity
					};
					self.client
						.update(self.database
							, self.documentPrefix + entityName
							, doc
							, {'etag': '00000000-0000-0000-000000000000'}
							, function(error, result) {
								if (error || !result) {
									return callback && callback(error);
								}
								self.entityGenerators[entityName].currentMax = doc.max;
								self.entityGenerators[entityName].lastId = 0;
								return callback && callback(undefined);
							});
				}
			});
};



function generator(options) {
	return new HiLoKeyGenerator(options);
}

module.exports = generator;