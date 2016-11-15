'use strict'
var request = require("request");
var MongoClient = require('mongodb').MongoClient;
var URL = 'mongodb://localhost:27017/buubuuwang'

var hkd_to_usd;
exports.hkd_to_usd = hkd_to_usd = function (payload) {
	var from = payload['data']['from'] || null;
	var to = payload['data']['to'] || null;
	return new Promise(function(resolve, reject){
		request({
			url: "http://api.fixer.io/latest?base="+from,
			method: "GET"
		}, function(error, response, body) {
			if (error || !body) {
				return reject(payload);
			}
			var rate = JSON.parse(body)['rates'][to];
			//timestamp                            
			payload['data']['created_at'] = new Date(Date.now());
			//round off to 2 decmicals in STRING type
			payload['data']['rate'] = rate.toFixed((2));
			resolve(payload);
		});
	})
}

var save_to_mongodb;
exports.save_to_mongodb = save_to_mongodb = function (payload){
	return new Promise(function(resolve, reject){
		MongoClient.connect(URL, function(err, db) {
			if (err) reject(err);
			var collection = db.collection('hkd_to_usd')
			collection.insert(payload, function(err, result) {
				if(err) return reject(new Error(err));
				resolve(payload);
				return db.close()
			})
		})
	});
}
