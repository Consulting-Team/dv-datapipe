#/bin/bash

BASE=$(dirname $0)
TODAY=$(date +%Y-%m-%d)

cd $BASE/..
# python impala_test.py

echo $TODAY
python iceberg_test.py --hull H2521