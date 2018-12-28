FROM ubuntu:16.04

# Install Ruby and Rails dependencies
RUN apt-get update && apt-get install -y \
  nodejs \
  nodejs-legacy \
  nodejs-dev \
  npm \
  redis-server \
  rabbitmq-server

#enable guest user to connect from remote host
RUN echo "[{rabbit, [{loopback_users, []}]}]." > /etc/rabbitmq/rabbitmq.config && \
    rabbitmq-plugins enable rabbitmq_management

COPY . /hyperflow-master-monitor
WORKDIR /hyperflow-master-monitor

RUN npm install amqplib influxdb-nodejs aws-sdk express prom-client@11.0.0

CMD service redis-server start && \
    service rabbitmq-server start && \
    sleep 5s && \
    ./monitor-rabbitmq-influxdb.js & \
    ./monitor-cluster-influxdb.js & \
    ./monitor-alarms.js  & \
    tail -F /var/log/rabbitmq/startup_err

EXPOSE 5672
