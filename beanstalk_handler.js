/** 
 * payload
 * @property {string} [type] - point out what's the goal of payload
 * @property {string} [retries] - how many time did it reput to the tube
 * @property {number} [data] - the object of currency from HKD to USD
 * @property {number} [job_id] - the job_id will be added into payload after getting a job from beanstalk server
 */

/**
 * Create a new BeanstalkHandler
 * @constructor
 * @param {string} server - beanstalk server host and port
 * @constructor
 */

(function() {
	var client = require('beanstalk_client').Client;
	var BeanstalkHandler = (function() {
		function BeanstalkHandler(server) {
			this.server = server;
		}
		BeanstalkHandler.prototype.put_job = function(tubes, priority, delay, ttr, payload) {
			return new Promise(function(resolve, reject){
				return client.connect(this.server, function(err, conn) {
					if (err) {
						return reject('Error connecting: ' + err);
					} else {
						var job_data = payload?payload:{
							type: 'currency_exchagne_rate',
							retries:0,
							data: {
								"from": "HKD",
								"to": "USD"
							}
						};
						this.connection = conn;
						conn.put(priority, delay, ttr, JSON.stringify(job_data), function(err, job_id) {
							if (err) return reject(err);
							//console.log('BeanstalkHandler put the job successfully : ' + job_id);
							resolve(job_data);
						});
					};
				});
			})
		}
		BeanstalkHandler.prototype.get_job = function(tubes) {
			return new Promise(function(resolve, reject){
				return client.connect(this.server, function(err, conn) {
					if (err) {
						return reject('Error connecting: ' + err);
					} else {
						this.connection = conn;
						this.connection.watch(tubes, function(err) {
							this.connection.reserve(function(err, job_id, data) {
								if(err) return reject(err);
								//console.log('Consumer reserve a job: id ' + job_id);
								//console.log('job data: ' + data);
								var job_data = JSON.parse(data);
								job_data['job_id'] = job_id;
								resolve(job_data);
							});
						});
					};
				});
			})
		}
		BeanstalkHandler.prototype.destroy_job = function(job_id,payload) {
			return new Promise(function(resolve, reject){
				this.connection.destroy(job_id, function(err) {
					if(err) return reject(new Error(err));
					resolve("destroy the job successfully");
				});
			})
		}
		BeanstalkHandler.prototype.bury_job = function(job_id) {
			return new Promise(function(resolve, reject){
				this.connection.bury(job_id,client.LOWEST_PRIORITY, function(err) {
					if(err) return reject(new Error(err));
					resolve('bury the job successfully');
				});
			})
		}
		BeanstalkHandler.prototype.release_job = function(job_id) {
			return new Promise(function(resolve, reject){
				this.connection.release(job_id,client.LOWEST_PRIORITY,function(err) {
					if(err) return reject(new Error(err));
					resolve(job_data);
				});
			})
		}
		BeanstalkHandler.prototype.stop = function(job_id) {
			return new Promise(function(resolve, reject){
				return this.connection.end();
			})
		}
		return BeanstalkHandler;
	})();
	exports.BeanstalkHandler = BeanstalkHandler;
}).call(this);

