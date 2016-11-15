(function(){
	var Promise = require("bluebird"); 
	var co = require('co'); 
	
	var BeanstalkHandler = require('./beanstalk_handler').BeanstalkHandler;
	var w = new BeanstalkHandler('127.0.0.1:11300');
	var tube_name = 'buubuuwang';
	
	var hkd_to_usd = require('./data_handler').hkd_to_usd;
	var save_to_mongodb = require('./data_handler').save_to_mongodb;
	
	var delay = 0;
	var tries_limit = 10;
	var tries = 0;
	var retries_limit = 3;
	var failed_payload = null

	var worker = function (){
		co(function* (){
			return w.get_job(tube_name);
		}).then(function(payload){
			console.log("get a job...OK");
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
					console.log('--------------------- end -----------------------');
					return worker();
				})
			} else {
				//Stop the task if you tried 10 succeed attempts
				console.log('Stop the task ... OK');
				return w.stop();
			}
		}).catch(function (err) {
			if(err instanceof Error) {
				console.log('Error :',err);
				return w.stop();
			} else {
			    console.log("failed to get the exchange rate ...");
				var retries = err['retries'];
				var job_id = err['job_id'];
				//If request is failed, reput to the tube and delay with 3s.
				if(retries < retries_limit){
					err['retries']++;
					failed_payload = err;
					delay = 3;
					return w.destroy_job(job_id,failed_payload).then(function(payload){
						return w.put_job(tube_name,0,delay,0,failed_payload);
					}).then(function(payload){
						console.log('reput job to tube and delay with '+ delay+'s...OK');
						console.log('--------------------- end -----------------------');
						return worker();
					});
				}else{
					//If failed more than 3 times in total (not consecutive), bury the job.
					failed_payload = null;
					console.log('task failed... bury the job');
					return w.bury_job(job_id);
				}
			}
		});
	}
	return worker();
})();
