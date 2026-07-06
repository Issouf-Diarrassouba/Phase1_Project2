#!/usr/bin/env bash
# init-redpanda-topics.sh
#
# Idempotently creates the "iot-telemetry" topic on the local Redpanda
# broker. Intended to run as a one-shot init container/service in
# docker-compose.yml after Redpanda has become healthy, but can also be
# run manually:
#
#   ./scripts/init-redpanda-topics.sh
#
# Requires `rpk` (Redpanda's CLI), which is bundled inside the official
# redpanda container image.

set -euo pipefail

BROKER="${BROKER_ADDRESS:-redpanda:9092}"
TOPIC="${TOPIC_NAME:-iot-telemetry}"
PARTITIONS="${TOPIC_PARTITIONS:-3}"
REPLICATION_FACTOR="${TOPIC_REPLICATION_FACTOR:-1}"

echo "[init-redpanda-topics] waiting for broker at ${BROKER}..."
until rpk cluster health --brokers "${BROKER}" >/dev/null 2>&1; do
  echo "[init-redpanda-topics] broker not ready yet, retrying in 2s..."
  sleep 2
done

echo "[init-redpanda-topics] broker is healthy."

if rpk topic list --brokers "${BROKER}" | grep -qx "${TOPIC}"; then
  echo "[init-redpanda-topics] topic '${TOPIC}' already exists, skipping creation."
else
  echo "[init-redpanda-topics] creating topic '${TOPIC}' (partitions=${PARTITIONS}, rf=${REPLICATION_FACTOR})..."
  rpk topic create "${TOPIC}" \
    --brokers "${BROKER}" \
    --partitions "${PARTITIONS}" \
    --replicas "${REPLICATION_FACTOR}"
  echo "[init-redpanda-topics] topic '${TOPIC}' created."
fi

echo "[init-redpanda-topics] done."