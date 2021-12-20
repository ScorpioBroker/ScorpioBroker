#!/bin/bash
cd $scrdir
java -Dspring.profiles.active=docker-no-eureka -jar $scrjar $*
