#!/bin/bash

echo "Creating DB"
echo "PGPASSWORD=password psql -h localhost -p " $1 "-U postgres -c \"create database cs223w2020_low_concurrency;\""
PGPASSWORD=password psql -h localhost -p $1 -U postgres -c "create database cs223w2020_low_concurrency;"

echo "Initiate Schema"
PGPASSWORD=password psql -h localhost -p $1 -U postgres -d cs223w2020_low_concurrency -q -f ./inputs/schema/create.sql

echo "Inserting Metadata"
PGPASSWORD=password psql -h localhost -p $1 -U postgres -d cs223w2020_low_concurrency -q -f ./inputs/data/low_concurrency/metadata.sql