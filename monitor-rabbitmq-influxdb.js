#!/usr/bin/env node

var AMQP_URL  = process.env.AMQP_URL ? process.env.AMQP_URL : "amqp://localhost:5672";
var QUEUE_NAME = process.env.QUEUE_NAME ? process.env.QUEUE_NAME : 'hyperflow.jobs';

var INFLUX_DB = process.env.INFLUX_DB ? process.env.INFLUX_DB : 'http://127.0.0.1:8086/hyperflow_influxdb';


var HYPERFLOW_METRIC_NAME = process.env.HYPERFLOW_METRIC_NAME ? process.env.HYPERFLOW_METRIC_NAME : "QueueLength";
var HYPERFLOW_METRIC_NAMESPACE = process.env.HYPERFLOW_METRIC_NAMESPACE ? process.env.HYPERFLOW_METRIC_NAMESPACE : 'hyperflow';
var CLUSET_NAME = process.env.CLUSET_NAME ? process.env.CLUSET_NAME : 'ecs_test_cluster_hyperflow';

var AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID ? process.env.AWS_ACCESS_KEY_ID : "";
var AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY ? process.env.AWS_SECRET_ACCESS_KEY : "";
var AWS_REGION = process.env.AWS_REGION ? process.env.AWS_REGION : 'us-east-1';

var AWS = require('aws-sdk');

var config={accessKeyId: AWS_ACCESS_KEY_ID, secretAccessKey: AWS_SECRET_ACCESS_KEY,region: AWS_REGION};
var cloudwatch = new AWS.CloudWatch(config);

var amqp = require('amqplib/callback_api');
const Influx = require('influxdb-nodejs');

const client = new Influx(INFLUX_DB);

var express = require('express');
var prometheus = require('prom-client');

var prometheusMetrics = {};

prometheus.collectDefaultMetrics();

const fieldSchema = {
    QueueLength: 'i',
    consumerCount: 'i',
    queue: 's',
  };

  const tagSchema = {

  };
  
  client.schema('hyperflow_rabbitmq_monitor', fieldSchema, tagSchema, {
    // default is false
    stripUnknown: true,
  });

console.log(AMQP_URL);

var tryAgain = true;

function notifyCloudWatchMetric(value)
{
  console.log("value %d",value);
  var params = {
    MetricData: [ 
      {
        MetricName: HYPERFLOW_METRIC_NAME, 
        Value: value,
        Dimensions: [
          {
            Name: 'ClusterName', 
            Value: CLUSET_NAME
          }]
      }
    ],
    Namespace: HYPERFLOW_METRIC_NAMESPACE 
    
  };

  cloudwatch.putMetricData(params, function(err, data) {
    if (err) console.log(err, err.stack); 
    else     console.log(data);           
  });
}



amqp.connect(AMQP_URL, function(err, conn) {

  console.log("ok after connect");
    conn.createChannel(function(err, ch) {
        console.log("createch err: %j", err);
        tryAgain = false;

        timeout = null;
      setInterval(function(){
        console.log("setInterval");
        ch.assertQueue(QUEUE_NAME, {durable: true});
          var mcount=0;
            ch.checkQueue(QUEUE_NAME, function(err, ok) {
              if(ok)
              {
                console.log("Session: %j", ok);
                mcount =ok.messageCount;

                if(mcount == 0 && timeout==null)
                {
                  if(ok.consumerCount > 1)
                  {
                    console.log("START TIMER");
                    timeout=setTimeout(function(){
                    console.log("TIMEOUT");
                    notifyCloudWatchMetric(-1)
                    timeout=null;
                  },200000)
                  }
                }
                if(mcount > 0 && timeout!=null)
                {
                  console.log("clear timer");
                  clearTimeout(timeout);
                  timeout =null;
                }

                client.write('hyperflow_rabbitmq_monitor')
                .field({
                  QueueLength: ok.messageCount,
                  consumerCount: ok.consumerCount,
                  queue: ok.queue,
                })
                .then(() => console.info('write point success'))
                .catch(console.error);

                prometheusMetrics.hyperflow_rabbitmq_monitor_queue_length = prometheusMetrics.hyperflow_rabbitmq_monitor_queue_length ||
                    new prometheus.Gauge({
                      name: 'hyperflow_rabbitmq_monitor_queue_length',
                      help: 'RabbitMQ queue length',
                      labelNames: ['queue'],
                    });
                prometheusMetrics.hyperflow_rabbitmq_monitor_queue_length.set({queue: ok.queue}, ok.messageCount);

                prometheusMetrics.hyperflow_rabbitmq_monitor_consumer_count = prometheusMetrics.hyperflow_rabbitmq_monitor_consumer_count ||
                    new prometheus.Gauge({
                      name: 'hyperflow_rabbitmq_monitor_consumer_count',
                      help: 'RabbitMQ consumer count',
                      labelNames: ['queue'],
                    });
                prometheusMetrics.hyperflow_rabbitmq_monitor_consumer_count.set({queue: ok.queue}, ok.consumerCount);

                notifyCloudWatchMetric(mcount);
              }
            });
      }, 1000);
     });

  });

var app = express();

app.get('/metrics', (req, res) => {
  res.set('Content-Type', prometheus.register.contentType);
  res.send(prometheus.register.metrics());
});

app.listen(3004, () => console.log(`Example app listening on port 3004!`))
  