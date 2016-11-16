var MongoClient = require('mongodb').MongoClient;
var request = require('supertest');
var URL = 'mongodb://localhost:27017/buubuuwang'
var chai = require('chai');
var expect = require('chai').expect;
var hkd_to_usd = require('../data_handler').hkd_to_usd;
var save_to_mongodb = require('../data_handler').save_to_mongodb;

var BeanstalkHandler = require('../beanstalk_handler').BeanstalkHandler;                       
var w = new BeanstalkHandler('127.0.0.1:11300');                                              
var tube_name = 'buubuuwang';

describe('Unit Testing', function(){
	var db = null;
	var test_data = null;
	var request_failed = false;

	before(function(){
		MongoClient.connect(URL, function(err, test_database) {
			if (err) reject(err);
			db = test_database;
		})
	});

	describe('Successful Case', function(){
		it('put a job', function(done) {
			w.put_job(tube_name,0,0,0,null).then(function(payload){
				expect(payload).to.have.property('type')
				expect(payload).to.have.property('retries')
				expect(payload).to.have.property('data')
				expect(payload).to.have.deep.property('data.from')
				expect(payload).to.have.deep.property('data.to')
				expect(payload).to.be.an('object');
				test_data = payload;
				done();
			}).catch(done);;
		});
		
		it('reserve and get a job', function(done) {
			w.get_job(tube_name).then(function(payload){
				expect(payload).to.have.property('type')
				expect(payload).to.have.property('retries')
				expect(payload).to.have.property('data')
				expect(payload).to.have.property('job_id')
				expect(payload).to.have.deep.property('data.from')
				expect(payload).to.have.deep.property('data.to')
				expect(payload).to.be.an('object');
				test_data = payload;
				done();
			}).catch(done);;
		});
		
		it('get the exchange rate', function(done) {
			hkd_to_usd(test_data).then(function(payload){
				expect(test_data).to.have.deep.property('data.rate')
				expect(test_data).to.have.deep.property('data.created_at')
				done();						
			}).catch(function(err){
				request_failed = true;
				test_data['retries'] = 2;
				expect(err.message).to.equal('get_rates_failed');	
				done(err);
			})
		});
		
		it('save it to mongodb', function(done) {
			expect(request_failed).equal(false)
			save_to_mongodb(test_data).then(function(payload){
				expect(payload).to.have.property('type')
				expect(payload).to.have.property('retries')
				expect(payload).to.have.property('data')
				expect(payload).to.have.property('job_id')
				expect(payload).to.have.deep.property('data.from')
				expect(payload).to.have.deep.property('data.to')
				expect(payload).to.be.an('object');
				test_data = payload;
				done();
			}).catch(done);
		});

		it('destroy a job', function(done) {
			var job_id = test_data['job_id'];
			w.destroy_job(job_id,test_data).then(function(val){
				expect(val).equal('destroy the job successfully')
				done();
			}).catch(done);
		});
	});
	
	describe('Failed Case', function(){
		it('reput a job', function(done) {
			test_data['retries'] = 2;
			w.put_job(tube_name,0,0,0,test_data).then(function(payload){
				expect(payload).to.have.property('type')
				expect(payload).to.have.property('retries')
				expect(payload).to.have.property('data')
				expect(payload).to.have.deep.property('data.from')
				expect(payload).to.have.deep.property('data.to')
				expect(payload).to.be.an('object');
				test_data = payload;
				done();
			}).catch(done);;
		});
		
		it('reserve and get a job', function(done) {
			w.get_job(tube_name).then(function(payload){
				expect(payload).to.have.property('type')
				expect(payload).to.have.property('retries',2)
				expect(payload).to.have.property('data')
				expect(payload).to.have.property('job_id')
				expect(payload).to.have.deep.property('data.from')
				expect(payload).to.have.deep.property('data.to')
				expect(payload).to.be.an('object');
				test_data = payload;
				done();
			}).catch(done);;
		});
		
		it('bury a job', function(done) {
			var job_id = test_data['job_id'];
			w.bury_job(job_id).then(function(val){
				expect(val).equal('bury the job successfully')
				done();
			}).catch(done);;
		});
	});
	
	after(function(){
		var collection = db.collection('hkd_to_usd').remove();
		db.close();
	});
});
