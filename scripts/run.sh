#!/bin/bash

BASE=$(dirname $0)
TARGET_DATE=$(date -d "1 day ago" +%Y-%m-%d)

# echo $TARGET_DATE

cd $BASE/..

# H2521
# python dv_datapipe.py --hull H2521 --date $TARGET_DATE
# python dv_datapipe.py --hull H2521 --date 2026-03-26 -c

# H2508
# python dv_datapipe.py --hull H2508 --date 2025-06-01

# H2532
# python dv_datapipe.py --hull H2532 --date 2026-03-26
python dv_datapipe.py --hull H2532 --start 2026-04-01 --end 2026-04-02 -c