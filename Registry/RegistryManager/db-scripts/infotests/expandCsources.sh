#!/bin/bash
set -o xtrace

jsonExpander="python /media/sf_ngb/jsonld-expand.py"

cat /dev/null > output.txt
for file in ./*.jsonld; do
  echo ${file##*/}
  ${jsonExpander} ${file##*/} >> output.txt
  echo "" >> output.txt
done


