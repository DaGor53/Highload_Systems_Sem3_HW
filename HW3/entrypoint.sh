#!/bin/bash
set -e

echo "Starting Flink..."
start-cluster.sh

echo "Waiting 200 seconds for Flink to be ready..."
sleep 200

echo "Submitting Flink job..."
flink run -py /opt/flink/jobs/kafka_to_star.py

tail -f /dev/null