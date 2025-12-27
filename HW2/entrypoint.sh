#!/bin/bash
set -e

#echo "Running jdbc_test.py"
#spark-submit /opt/spark/apps/jdbc_test.py

echo "Running etl_to_star.py"
spark-submit /opt/spark/apps/etl_to_star.py

echo "Running etl_to_marts.py"
spark-submit /opt/spark/apps/etl_to_marts.py

echo "All Spark jobs finished"