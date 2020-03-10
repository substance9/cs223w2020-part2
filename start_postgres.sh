#!/bin/bash

docker container stop cs223_postgres
docker run --name cs223_postgres --rm --volume=$(pwd)/postgres_data:/var/lib/postgresql/data -p 10020:5432 --shm-size=8G -e POSTGRES_PASSWORD=password -d postgres:12.1 -N 1000

 