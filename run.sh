#!/bin/bash

#cd $sesdir
#java -jar $sesjar &
#cd ..
#sleep 10
#cd $scsdir
#java -Dspring.profiles.active=docker -jar $scsjar $* &
#cd ..
#sleep 10
#cd $sgwdir
#java -Dspring.profiles.active=docker-aaio -jar $sgwjar $* &
#cd ..
#sleep 10
cd $scrdir
java -Dspring.profiles.active=docker -jar $scrjar $*
