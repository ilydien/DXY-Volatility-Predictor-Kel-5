#!/bin/sh
cd /dbt
dbt deps
dbt docs generate
dbt docs serve --port 8080 --host 0.0.0.0
