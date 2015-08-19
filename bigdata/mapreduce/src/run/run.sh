#!/bin/bash

#remove old ones
rm -rf lib
rm $1.jar

#unzip
unzip $1-dep.zip

#form libs
libjars=
first=1
for f in lib/*.jar; 
do
  if [ $first -eq 1 ]; then
    first=0;
  else
	libjars=$libjars','
  fi
  libjars=$libjars$f;
done

#remove old output
if hadoop fs -test -d $4; then
  echo 'remove '$4;
  hadoop fs -rm -r $4;
fi

#run hadoop
hadoop jar $1.jar $2 -libjars $libjars $3 $4;