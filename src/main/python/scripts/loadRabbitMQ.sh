#!/bin/bash
export nowDate=`date +"%Y%m%d%H%M%S"`
echo `date +"%Y-%m-%d %H:%M:%S"` Starting loadRabbitMQ job.... >../logs/"$nowDate"loadRabbitMQ.log
python ../src/loadRabbitMQ.py -c ../config/loadRabbitMQ.conf
echo `date +"%Y-%m-%d %H:%M:%S"` loadRabbitMQ job complete! >>../logs/"$nowDate"loadRabbitMQ.log
