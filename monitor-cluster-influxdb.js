#!/usr/bin/env node

var AWS_REGION = process.env.AWS_REGION ? process.env.AWS_REGION : 'us-east-1';
var INFLUX_DB = process.env.INFLUX_DB ? process.env.INFLUX_DB : 'http://127.0.0.1:8086/hyperflow_influxdb';
var CLUSTER_NAME = process.env.CLUSTER_NAME ? process.env.CLUSTER_NAME :  'ecs_test_cluster_hyperflow';

var AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID ? process.env.AWS_ACCESS_KEY_ID : "";
var AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY ? process.env.AWS_SECRET_ACCESS_KEY : "";

//var AMQP_URL  = process.env.AMQP_URL ? process.env.AMQP_URL : "amqp://localhost:5672";

var AWS = require('aws-sdk');
//var amqp = require('amqplib/callback_api');

var config={accessKeyId: AWS_ACCESS_KEY_ID, secretAccessKey: AWS_SECRET_ACCESS_KEY,region: AWS_REGION};


var ecs = new AWS.ECS(config);
var cloudwatch = new AWS.CloudWatch(config);

const Influx = require('influxdb-nodejs');
const client = new Influx(INFLUX_DB);

var express = require('express');
var prometheus = require('prom-client');

var prometheusMetrics = {};

prometheus.collectDefaultMetrics();

// simple countdown 
function CDL(countdown, completion) {
  this.signal = function() { 
      if(--countdown < 1) completion(); 
  };
}

// i --> integer
// s --> string
// f --> float
// b --> boolean
const fieldSchema = {
  ec2_count: 'i',
  worker_count: 'i',
  QueueLength: 'i',
  url: 's',
};
const tagSchema = {
  region: ['us-east1', 'us-east2'],
  AlarmHight : ["Alarm", "Ok", "INSUFFICIENT_DATA"],
  AlarmLow : ["Alarm", "Ok", "INSUFFICIENT_DATA"],
};

client.schema('hyperflow_monitor', fieldSchema, tagSchema, {
  // default is false
  stripUnknown: true,
});

var params = {
  cluster: CLUSTER_NAME
 };

var paramsAlarm = {
  AlarmNames : ["ecs_test_cluster_hyperflow_queue_lenth_low","ecs_test_cluster_hyperflow_queue_lenth_high"],
};

setInterval(function(){
  var containerCount = 0;
  var taskCount = 0;
  var countRespons=3;
  var AlarmHightValue="";
  var AlarmLowValue="";

  var latch = new CDL(countRespons, function() {
    client.write('hyperflow_monitor')
    .tag({
      region: AWS_REGION,
      AlarmHight : AlarmHightValue,
      AlarmLow: AlarmLowValue,
    })
    .field({
      ec2_count: containerCount,
      worker_count: taskCount,
      url: 'https://github.com/vicanso/influxdb-nodejs',
    })
    .then(() => console.info('write point success'))
    .catch(console.error);

    prometheusMetrics.hyperflow_monitor_worker_count = prometheusMetrics.hyperflow_monitor_worker_count ||
        new prometheus.Gauge({
          name: 'hyperflow_monitor_worker_count',
          help: 'Alarm monitor worker count',
          labelNames: ['region', 'alarmHigh', 'alarmLow']
        });
    prometheusMetrics.hyperflow_monitor_worker_count.set({region: AWS_REGION, alarmHigh : AlarmHightValue, alarmLow: AlarmLowValue}, taskCount);

    prometheusMetrics.hyperflow_monitor_ec2_count = prometheusMetrics.hyperflow_monitor_ec2_count ||
        new prometheus.Gauge({
          name: 'hyperflow_monitor_ec2_count',
          help: 'Alarm monitor ec2 count',
          labelNames: ['region', 'alarmHigh', 'alarmLow']
        });
    prometheusMetrics.hyperflow_monitor_ec2_count.set({region: AWS_REGION, alarmHigh : AlarmHightValue, alarmLow: AlarmLowValue}, containerCount);

  });

  ecs.listContainerInstances(params, function(err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else
    {
      //console.log(data);           // successful response
      containerCount=data.containerInstanceArns.length;
      latch.signal();
    }
    /*
    data = {
     containerInstanceArns: [
        "arn:aws:ecs:us-east-1:<aws_account_id>:container-instance/f6bbb147-5370-4ace-8c73-c7181ded911f", 
        "arn:aws:ecs:us-east-1:<aws_account_id>:container-instance/ffe3d344-77e2-476c-a4d0-bf560ad50acb"
     ]
    }
    */
  });

  ecs.listTasks(params, function(err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else{
      taskCount=data.taskArns.length;
      latch.signal();
    }
    /*
    data = {
     taskArns: [
        "arn:aws:ecs:us-east-1:012345678910:task/0cc43cdb-3bee-4407-9c26-c0e6ea5bee84", 
        "arn:aws:ecs:us-east-1:012345678910:task/6b809ef6-c67e-4467-921f-ee261c15a0a1"
     ]
    }
    */
  });

  cloudwatch.waitFor('alarmExists', paramsAlarm, function(err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else{
      console.log((new Date())+"Alarm:"+data.MetricAlarms[0].StateValue);
      AlarmLowValue = data.MetricAlarms[0].StateValue;
      AlarmHightValue = data.MetricAlarms[1].StateValue;
      latch.signal();
    }
  });

  
}, 1000);
  

var app = express();

app.get('/metrics', (req, res) => {
  res.set('Content-Type', prometheus.register.contentType);
  res.send(prometheus.register.metrics());
});

app.listen(3003, () => console.log(`Example app listening on port 3003!`))
