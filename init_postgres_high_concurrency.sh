#!/bin/bash

PGPASSWORD=password psql -h localhost -p 10020 -U postgres -d cs223w2020_high_concurrency -q -f ./inputs/schema/drop.sql
PGPASSWORD=password psql -h localhost -p 10020 -U postgres -c "drop database cs223w2020_high_concurrency"
PGPASSWORD=password psql -h localhost -p 10020 -U postgres -c "create database cs223w2020_high_concurrency"
PGPASSWORD=password psql -h localhost -p 10020 -U postgres -d cs223w2020_high_concurrency -q -f ./inputs/schema/create.sql
PGPASSWORD=password psql -h localhost -p 10020 -U postgres -d cs223w2020_high_concurrency -q -f ./inputs/data/high_concurrency/metadata.sql