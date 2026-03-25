#/bin/bash

BASE=$(dirname $0)
TARGET_DATE=$(date -d "1 day ago" +%Y-%m-%d)

echo $TARGET_DATE

cd $BASE/..
# python iceberg_test.py --hull H2521 --date $TARGET_DATE
python iceberg_test.py --hull H2521 --date "2026-03-24"