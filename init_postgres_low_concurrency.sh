#!/bin/bash

PGPASSWORD=password psql -h localhost -p 10020 -U postgres -d cs223w2020_low_concurrency -q -f ./inputs/schema/drop.sql
PGPASSWORD=password psql -h localhost -p 10020 -U postgres -c "drop database cs223w2020_low_concurrency"
PGPASSWORD=password psql -h localhost -p 10020 -U postgres -c "create database cs223w2020_low_concurrency"
PGPASSWORD=password psql -h localhost -p 10020 -U postgres -d cs223w2020_low_concurrency -q -f ./inputs/schema/create.sql
PGPASSWORD=password psql -h localhost -p 10020 -U postgres -d cs223w2020_low_concurrency -q -f ./inputs/data/low_concurrency/metadata.sql