#!/bin/bash

cd $sesdir
java -jar $sesjar &
cd ..
sleep 10
cd $scsdir
java -jar $scsjar &
cd ..
sleep 10
cd $sgwdir
java -jar $sgwjar &
cd ..
sleep 10
cd $scrdir
java -jar $scrjar $*
