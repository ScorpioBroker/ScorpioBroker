#!/bin/bash

cd $sesdir
java -jar $sesjar &
cd ..
cd $sgwdir
java -jar $sgwjar &
cd ..
cd $scrdir
java -jar $scrjar $*
