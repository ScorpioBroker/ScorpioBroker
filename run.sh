#!/bin/bash

cd $sesdir
java -jar $sesjar &
cd ..
cd $scsdir
java -jar $scsjar &
cd ..
cd $sgwdir
java -jar $sgwjar &
cd ..
cd $scrdir
java -jar $scrjar 
