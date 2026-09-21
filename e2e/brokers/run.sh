#!/usr/bin/env bash
# Cross-host e2e for @simple-discovery/redis, nats and amqp.
#
#   BROKER_HOST=admin@flygo-02 BROKER_IP=192.168.2.157 REMOTES="admin@flygo-02 admin@flygo-04" ./e2e/brokers/run.sh
#
# Starts Redis, NATS and RabbitMQ in Docker on BROKER_HOST (own names and ports, removed at the end),
# runs one agent here and one on each remote, started a second apart and never heartbeating, then
# restarts the broker. Every agent must see every other one before the restart (hello replies) and
# again after it (reconnect + resync). Remotes need only SSH; bun is copied into a temp dir.
set -euo pipefail

: "${BROKER_HOST:?set BROKER_HOST, e.g. admin@flygo-02}"
: "${BROKER_IP:?set BROKER_IP, the broker host address the agents connect to}"
: "${REMOTES:?set REMOTES, space-separated ssh targets}"
TRANSPORTS=${TRANSPORTS:-redis nats amqp}
BUN_VERSION=${BUN_VERSION:-1.4.2}
SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=10 ${SSH_KEY:+-i "$SSH_KEY"})
HERE=$(cd "$(dirname "$0")" && pwd)
LOCAL_TMP=$(mktemp -d -t simple-discovery-brokers)
TAG="sd-e2e-$$"
REMOTE_DIRS=()   # "target:dir" pairs

cleanup() {
    ssh "${SSH_OPTS[@]}" "$BROKER_HOST" "docker rm -f $TAG-redis $TAG-nats $TAG-rabbitmq >/dev/null 2>&1 || true"
    for pair in ${REMOTE_DIRS[@]+"${REMOTE_DIRS[@]}"}; do ssh "${SSH_OPTS[@]}" "${pair%%:*}" "rm -rf '${pair#*:}'" || true; done
    rm -rf "$LOCAL_TMP"
}
trap cleanup EXIT

echo "==> starting brokers on $BROKER_HOST"
ssh "${SSH_OPTS[@]}" "$BROKER_HOST" "docker run -d --name $TAG-redis -p 16379:6379 redis:7-alpine >/dev/null \
    && docker run -d --name $TAG-nats -p 14222:4222 nats:2-alpine >/dev/null \
    && docker run -d --name $TAG-rabbitmq -p 15672:5672 rabbitmq:4-alpine >/dev/null"
url_for() { case $1 in redis) echo "redis://$BROKER_IP:16379" ;; nats) echo "nats://$BROKER_IP:14222" ;; amqp) echo "amqp://$BROKER_IP:15672" ;; esac; }
container_for() { case $1 in redis) echo "$TAG-redis" ;; nats) echo "$TAG-nats" ;; amqp) echo "$TAG-rabbitmq" ;; esac; }
dir_for() { for pair in "${REMOTE_DIRS[@]}"; do [ "${pair%%:*}" = "$1" ] && echo "${pair#*:}"; done; }

echo "==> bundling agent"
(cd "$HERE" && bun build agent.ts --target=bun --outfile="$LOCAL_TMP/agent.js" >/dev/null)
(cd "$LOCAL_TMP" && npm pack "@oven/bun-linux-x64@$BUN_VERSION" --silent >/dev/null && tar xzf ./*.tgz)
for remote in $REMOTES; do
    dir=$(ssh "${SSH_OPTS[@]}" "$remote" "mktemp -d /tmp/simple-discovery-brokers.XXXXXX")
    REMOTE_DIRS+=("$remote:$dir")
    scp -q "${SSH_OPTS[@]}" "$LOCAL_TMP/package/bin/bun" "$LOCAL_TMP/agent.js" "$remote:$dir/"
    ssh "${SSH_OPTS[@]}" "$remote" "chmod +x '$dir/bun'"
done

echo "==> waiting for RabbitMQ to accept connections"
for _ in $(seq 60); do
    ssh "${SSH_OPTS[@]}" "$BROKER_HOST" "docker exec $TAG-rabbitmq rabbitmq-diagnostics -q check_port_connectivity" >/dev/null 2>&1 && break
    sleep 1
done

FAILED=0
for transport in $TRANSPORTS; do
    echo "==> $transport"
    start=$(( $(date +%s) * 1000 ))
    restart_at=$(( start + 6000 ))
    common="AGENT_TRANSPORT=$transport AGENT_URL=$(url_for "$transport") AGENT_NAMESPACE=e2e-$transport-$RANDOM AGENT_KEY=k$RANDOM$RANDOM AGENT_RESTART_AT=$restart_at"
    : > "$LOCAL_TMP/$transport.log"
    env $common AGENT_NAME=local AGENT_DURATION_MS=16000 bun "$LOCAL_TMP/agent.js" >> "$LOCAL_TMP/$transport.log" 2>&1 &
    pids=($!)
    i=0
    for remote in $REMOTES; do
        i=$((i + 1)); sleep 1
        ssh "${SSH_OPTS[@]}" "$remote" "cd '$(dir_for "$remote")' && env $common AGENT_NAME=remote-$i AGENT_DURATION_MS=$((15000 - i * 1000)) ./bun agent.js" >> "$LOCAL_TMP/$transport.log" 2>&1 &
        pids+=($!)
    done
    sleep $(( (restart_at - $(date +%s) * 1000) / 1000 + 1 ))
    echo "    restarting $(container_for "$transport")"
    ssh "${SSH_OPTS[@]}" "$BROKER_HOST" "docker restart $(container_for "$transport") >/dev/null"
    wait "${pids[@]}" || true

    names="local $(seq -f 'remote-%g' 1 "$i" | tr '\n' ' ')"
    grep '^{' "$LOCAL_TMP/$transport.log" | python3 "$HERE/check.py" "$names" || { FAILED=1; sed 's/^/      /' "$LOCAL_TMP/$transport.log" | grep -v '"ev":"seen"' | tail -20; }
done

if [ "$FAILED" = 0 ]; then echo "==> PASS"; else echo "==> FAIL"; fi
exit "$FAILED"
