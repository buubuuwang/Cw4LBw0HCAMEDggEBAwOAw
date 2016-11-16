/**
 * @property {number} [delay = 0] - If request is succeed, reput to the tube and delay with 60s or 3s for failed request.
 * @property {number} [tries_limit = 10] - Stop the task if you tried 10 succeed attempts
 * @property {number} [tries = 0] - The number of succeed attempts
 * @property {number} [retries_limit = 3] - If failed more than 3 times in total (not consecutive), bury the job. 
 * @property {object} [failed_payload = payload] - if request failed , the retries of attributes of a job payload will be added one time.
 *
 */

/**
 * Create a new woker
 * @constructor
 * @param {object} arguments
 * @constructor
 */

(function(){
	var Promise = require("bluebird"); 
	var co = require('co'); 
	
	var BeanstalkHandler = require('./beanstalk_handler').BeanstalkHandler;
	var w = new BeanstalkHandler('challenge.aftership.net:11300');
	var tube_name = 'buubuuwang';
	
	var hkd_to_usd = require('./data_handler').hkd_to_usd;
	var save_to_mongodb = require('./data_handler').save_to_mongodb;
	
	var delay = 0;
	var tries_limit = 10;
	var tries = 0;
	var retries_limit = 2;
	var failed_payload = null

	var worker = function (){
		co(function* (){
			return w.get_job(tube_name);
		}).then(function(payload){
			console.log("get a job...OK");
			failed_payload = payload;
			return hkd_to_usd(payload);
		}).then(function(payload){
			console.log("get the exchange rate...OK");
			return save_to_mongodb(payload);
		}).then(function(payload){
			console.log("save it to mongodb...OK");
			var job_id = payload['job_id'];
			return w.destroy_job(job_id,payload);
		}).then(function(payload){
			console.log("request is succeed...("+(tries+1)+")");
			++tries; 
			delay = 60;
			//If request is succeed, reput to the tube and delay with 60s.
			if(tries < tries_limit){
				return w.put_job(tube_name,0,delay,0,failed_payload).then(function(payload){
			        console.log('reput job to tube and delay with '+ delay+'s...OK');
					console.log('waiting for 60s ........');
					console.log('--------------------- end -----------------------');
					return worker();
				})
			} else {
				//Stop the task if you tried 10 succeed attempts
				console.log('Stop the task ... OK');
				return w.stop();
			}
		}).catch(function (err) {
			if (err.message ===  "get_rates_failed") {
			    console.log("failed to get the exchange rate ...");
				var retries = failed_payload['retries'];
				var job_id = failed_payload['job_id'];
				//If request is failed, reput to the tube and delay with 3s.
				if(retries < retries_limit){
					failed_payload['retries']++;
					delay = 3;
					return w.destroy_job(job_id,failed_payload).then(function(payload){
						return w.put_job(tube_name,0,delay,0,failed_payload);
					}).then(function(payload){
						console.log('reput job to tube and delay with '+ delay+'s...OK');
						console.log('waiting for 3s ........');
						console.log('--------------------- end -----------------------');
						return worker();
					});
				}else{
					//If failed more than 3 times in total (not consecutive), bury the job.
					failed_payload = null;
					console.log('task failed... bury the job');
					return w.bury_job(job_id);
				}
			} else {
				console.log('Error :',err);
				return w.stop();
			}
		});
	}
	return worker();
})();
