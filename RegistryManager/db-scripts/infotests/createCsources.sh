#!/bin/bash
set -o xtrace
for file in ./*.jsonld; do
  echo ${file##*/}
  curl -X POST -H 'Accept: application/ld+json'  -H 'Content-Type: application/json' --data "@${file##*/}" http://localhost:9090/ngsi-ld/v1/csourceRegistrations/ 

done


