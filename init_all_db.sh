#!/bin/bash

echo "Executing init_mysql_low_concurrency.sh"
source ./init_mysql_low_concurrency.sh

echo "Executing init_mysql_high_concurrency.sh"
source ./init_mysql_high_concurrency.sh

echo "Executing init_postgres_low_concurrency.sh"
source ./init_postgres_low_concurrency.sh

echo "Executing init_postgres_high_concurrency.sh"
source ./init_postgres_high_concurrency.sh