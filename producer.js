var BeanstalkHandler = require('./beanstalk_handler').BeanstalkHandler;
var w = new BeanstalkHandler('challenge.aftership.net:11300');
var tube_name = 'buubuuwang';

w.put_job(tube_name,0,0,0,null)

