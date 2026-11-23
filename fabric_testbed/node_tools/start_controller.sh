#!/usr/bin/env bash
# Start (or restart) native quantnet_controller and qn-api tmux sessions on the controller VM.
set -euo pipefail

QNCP_ROOT="/home/rocky/qncp"
mkdir -p "${QNCP_ROOT}/logs"
sudo loginctl enable-linger rocky 2>/dev/null || true

tmux kill-session -t qncp-controller 2>/dev/null || true
tmux new-session -d -s qncp-controller \
    "QUANTNET_LINK_ADJACENCY=true ${QNCP_ROOT}/.venv/bin/quantnet_controller \
    -c ${QNCP_ROOT}/conf/quantnet.cfg \
    --mq-broker-host 127.0.0.1 \
    --mq-mongo-host 127.0.0.1 \
    --plugin-path ${QNCP_ROOT}/src/qn-plugins/plugins \
    --schema-path ${QNCP_ROOT}/src/qn-plugins/plugins/schema \
    2>&1 | tee ${QNCP_ROOT}/logs/controller.console.log"

tmux kill-session -t qncp-api 2>/dev/null || true
tmux new-session -d -s qncp-api \
    "cd ${QNCP_ROOT}/src/qn-api && \
    QUANTNET_HOME=${QNCP_ROOT} ${QNCP_ROOT}/.venv/bin/uvicorn main:app \
    --host :: --port 8081 \
    2>&1 | tee ${QNCP_ROOT}/logs/api.console.log"
