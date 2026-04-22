#!/bin/bash

BASE=$(dirname $0)
TARGET_DATE=$(date -d "1 day ago" +%Y-%m-%d)

# echo $TARGET_DATE

cd $BASE/..

# H2521
# python dv_datapipe.py --hull H2521 --date $TARGET_DATE
python dv_datapipe.py --hull H2521 --start 2026-03-01 --end 2026-03-02 -c
# python dv_datapipe.py --hull H2521 --start 2026-03-27 --end 2026-03-27

# H2508
# python dv_datapipe.py --hull H2508 --start 2025-05-01 --end 2025-06-02 -c
# python dv_datapipe.py --hull H2508 --start 2025-04-01 --end 2025-04-30

# H2532
# python dv_datapipe.py --hull H2532 --start 2026-03-01 --end 2026-04-02 -c
# python dv_datapipe.py --hull H2532 --start 2026-04-03 --end 2026-04-30