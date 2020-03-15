#!/bin/bash

echo "Creating Log DB"
PGPASSWORD=password psql -h localhost -p $1 -U postgres -c "create database cs223w2020_cohort_log;"

echo "Initiate Log Schema"
PGPASSWORD=password psql -h localhost -p $1 -U postgres -d cs223w2020_cohort_log -q -f ./cohort_log_table_create.sql
