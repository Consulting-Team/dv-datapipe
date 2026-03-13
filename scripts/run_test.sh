#/bin/bash

BASE=$(dirname $0)

cd $BASE/..
# python impala_test.py
python iceberg_test.py --hull H2521