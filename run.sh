#!/bin/bash

cd $sesdir
java -jar $sesjar &
cd..
cd $scsdir
java -jar $scsjar &
cd..
cd $sgwdir
java -jar $sgwjar &
cd..
cd $hmgdir
java -jar $hmgjar &
cd..
cd $qmgdir
java -jar $qmgjar &
cd..
cd $rmgdir
java -jar $rmgjar &
cd..
cd $strmgdir
java -jar $strmgjar &
cd..
cd $submgdir
java -jar $submgjar &
cd.. 
cd $acsdir
java -jar $acsjar 


