#! /bin/bash

#javac src/label-upgrade/*.java -classpath /home/zhaozhao/hadoop-pecos/hadoop-0.18.3-core.jar -d class/label-upgrade
#jar cvf bin/label-upgrade.jar -C ./class/label-upgrade . 

#javac src/label-naive/*.java -classpath /home/zhaozhao/hadoop-pecos/hadoop-0.18.3-core.jar -d class/label-naive
#jar cvf bin/label-naive.jar -C ./class/label-naive . 

#javac src/label/*.java -classpath /home/zhaozhao/hadoop-pecos/hadoop-0.18.3-core.jar -d class/label
#jar cvf bin/label.jar -C ./class/label . 

#javac src/graphlet/*.java -classpath /home/zhaozhao/hadoop-pecos/hadoop-0.18.3-core.jar -d class/graphlet
#jar cvf bin/graphlet.jar -C ./class/graphlet . 

#javac src/nosample/*.java -classpath /home/zhaozhao/hadoop-pecos/hadoop-0.18.3-core.jar -d class/nosample
#jar cvf bin/nosample.jar -C ./class/nosample . 

#javac src/sample/*.java -classpath /home/zhaozhao/hadoop-pecos/hadoop-0.18.3-core.jar -d class/sample
#jar cvf bin/sample.jar -C ./class/sample .

echo $HADOOP_HOME
cp=$HADOOP_HOME/lib/commons-logging-1.1.1.jar
for jar in $HADOOP_HOME/hadoop*core*jar
do
  #echo $cp
  cp=$cp:$jar
  #echo $cp
done
echo $cp 

javac src/basic/*.java -classpath $cp -d class/basic
jar cvf bin/mwp.jar -C ./class/basic .

