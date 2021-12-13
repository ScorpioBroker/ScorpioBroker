#!/bin/bash
cd $sesdir
java -Dlog4j2.formatMsgNoLookups=true -jar $sesjar &
cd ..
sleep 10
cd $scsdir
java -Dlog4j2.formatMsgNoLookups=true -jar $scsjar $* &
cd ..
sleep 10
cd $sgwdir
java -Dlog4j2.formatMsgNoLookups=true -jar $sgwjar $* &
cd ..
sleep 10
cd $scrdir
java -Dlog4j2.formatMsgNoLookups=true -jar $scrjar $*
