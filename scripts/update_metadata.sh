#!/bin/bash

BASE=$(dirname $0)

cd $BASE/..
python update_metadata.py --path resources/csv/H2521_metadata.csv