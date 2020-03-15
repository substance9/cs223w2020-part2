#!/bin/bash

echo "Creating DB"
PGPASSWORD=password psql -h localhost -p $1 -U postgres -c "create database cs223w2020_high_concurrency;"

echo "Initiate Schema"
PGPASSWORD=password psql -h localhost -p $1 -U postgres -d cs223w2020_high_concurrency -q -f ./inputs/schema/create.sql

echo "Inserting Metadata"
PGPASSWORD=password psql -h localhost -p $1 -U postgres -d cs223w2020_high_concurrency -q -f ./inputs/data/high_concurrency/metadata.sql