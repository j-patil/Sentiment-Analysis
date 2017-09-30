#!/bin/bash
echo "Running Script"
echo qwerty | sudo -S /opt/lampp/lampp start 
/usr/local/hadoop/sbin/start-all.sh
unset HADOOP_CLASSPATH
export HADOOP_ClASSPATH=/home/rutesh/workspace/Beta/lib/opennlp-tools-1.5.3.jar
export LIBJARS=/home/rutesh/workspace/Beta/lib/opennlp-tools-1.5.3.jar
echo "Done Successfully"
