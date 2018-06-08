#!/usr/bin/env bash

echo 'You are executing' $1

script_path=`readlink -f $0`
script_dir=`dirname ${script_path}`
ATHENA_HOME=`cd ${script_dir}/../;pwd`
echo ATHENA_HOME: ${ATHENA_HOME}
cd ${ATHENA_HOME}

HADOOP_CLASSPATH=`hadoop classpath`
SPARK_CLASSPATH="$SPARK_HOME/lib/*"

cp=".:${ATHENA_HOME}/lib/*:${ATHENA_HOME}/conf:${HADOOP_CLASSPATH}:$SPARK_CLASSPATH"
echo CLASSPATH: ${cp}

function athena_init() {
    echo Initing ...
    echo Athena inited
}

function athena_start() {
    echo Starting athena ...
    if [ -e athena.pid ];
    then
      if [ -e /proc/`cat athena.pid` ]; then
        athena_stop
      fi
      nohup java -cp ${cp} com.timeyang.athena.Athena >${ATHENA_HOME}/athena.out 2>&1 &
    fi
    echo Athena started
}

function athena_stop() {
    echo Stoping athena ...
    #  ps -ef | grep com.timeyang.athena.Athena | grep -v grep | awk '{print $2}' | xargs kill -s TERM
    kill -TERM `cat athena.pid`
    # 检查进程是否已经结束，如果没有，则继续等待
    while kill -0 `cat athena.pid` 2> /dev/null; do sleep 1; done;

    echo Athena stopped
}

function check_up() {
    if [ -e athena.pid ];
    then
      if [ ! -e /proc/`cat athena.pid` ]; then
        echo `date` start athena ... >> check_up.log
        athena_start
      fi
    fi
}

case $1 in
  init) athena_init
  ;;
  start) athena_start
  ;;
  stop) athena_stop
  ;;
  up) check_up
  ;;
  *) echo 'wrong cmd args, please input init/start/stop/up'
  exit 0
  ;;
esac