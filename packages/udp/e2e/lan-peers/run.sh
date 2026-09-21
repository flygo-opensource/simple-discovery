#!/usr/bin/env bash
# E2E chế độ peers của @simple-discovery/udp giữa máy này và MỘT máy Linux cùng LAN vật lý.
#
#   REMOTE=user@linux-box \
#   REMOTE_LAN_IP=192.168.1.20 LOCAL_LAN_IP=192.168.1.10 \
#   ./e2e/lan-peers/run.sh
#
# SSH chỉ dùng để điều khiển máy kia; gói discovery đi qua IP LAN (REMOTE_LAN_IP/LOCAL_LAN_IP).
# Qua VPN không chuyển multicast (NetBird, WireGuard): đặt AGENT_MULTICAST=off và truyền địa chỉ
# VPN, IP hoặc hostname, vào REMOTE_LAN_IP/LOCAL_LAN_IP.
# Máy local chạy 1 agent, máy remote chạy 2 agent cùng máy. Hai máy dùng hai multicast group
# khác nhau, nên multicast không thể nối chúng: thấy nhau được là nhờ peers.
#
# Ca 1 (đối chứng, không peers): hai máy KHÔNG được thấy nhau; 2 agent cùng máy vẫn thấy nhau.
# Ca 2 (peers): mọi agent thấy nhau, kể cả agent thứ hai trên máy remote. Trên Linux gói unicast
# rơi vào socket bind cuối cùng, nên agent kia chỉ thấy máy local nếu gói được phát lại.
#
# Mọi thứ trên máy remote nằm trong một thư mục mktemp và bị xoá khi script kết thúc (kể cả lỗi).
# Không cài gì global: bun được mang theo trong thư mục tạm.
set -euo pipefail

: "${REMOTE:?set REMOTE, e.g. user@linux-box}"
: "${REMOTE_LAN_IP:?set REMOTE_LAN_IP (the remote host's physical LAN address)}"
: "${LOCAL_LAN_IP:?set LOCAL_LAN_IP (this host's address on the same LAN)}"
BUN_VERSION=${BUN_VERSION:-1.4.2}

HERE=$(cd "$(dirname "$0")" && pwd)
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10 "$REMOTE")
LOCAL_TMP=$(mktemp -d -t simple-discovery-lan-peers)
REMOTE_DIR=""

cleanup() {
    if [ -n "$REMOTE_DIR" ]; then
        # Không pkill theo đường dẫn: mẫu đó khớp luôn chính shell SSH đang chạy lệnh dọn.
        "${SSH[@]}" "rm -rf '$REMOTE_DIR'" || echo "WARN: could not remove $REMOTE:$REMOTE_DIR" >&2
    fi
    rm -rf "$LOCAL_TMP"
}
trap cleanup EXIT

echo "==> bundling agent from current source"
(cd "$HERE" && bun build agent.ts --target=bun --outfile="$LOCAL_TMP/agent.js" >/dev/null)

case "$("${SSH[@]}" uname -m)" in
    x86_64) BUN_PKG=bun-linux-x64 ;;
    aarch64) BUN_PKG=bun-linux-aarch64 ;;
    *) echo "unsupported remote arch" >&2; exit 1 ;;
esac
echo "==> fetching @oven/$BUN_PKG@$BUN_VERSION for the remote"
(cd "$LOCAL_TMP" && npm pack "@oven/$BUN_PKG@$BUN_VERSION" --silent >/dev/null && tar xzf ./*.tgz)

REMOTE_DIR=$("${SSH[@]}" "mktemp -d /tmp/simple-discovery-lan-peers.XXXXXX")
echo "==> staging into $REMOTE:$REMOTE_DIR"
scp -q -o BatchMode=yes "$LOCAL_TMP/package/bin/bun" "$LOCAL_TMP/agent.js" "$REMOTE:$REMOTE_DIR/"
"${SSH[@]}" "chmod +x '$REMOTE_DIR/bun'"

# run_scenario <label> <on|off>  -> in ra ma trận "ai thấy ai" dạng JSON vào $LOCAL_TMP/<label>.json
run_scenario() {
    local label=$1 peers=$2
    local port=$((42000 + RANDOM % 5000)) ns="lan-peers-$label-$RANDOM" key="k-$RANDOM$RANDOM"
    local local_peers="" remote_peers=""
    if [ "$peers" = on ]; then local_peers=$REMOTE_LAN_IP; remote_peers=$LOCAL_LAN_IP; fi
    local common="AGENT_NAMESPACE=$ns AGENT_KEY=$key AGENT_PORT=$port AGENT_MULTICAST=${AGENT_MULTICAST:-on}"

    # remote-1 bind trước, remote-2 bind sau.
    "${SSH[@]}" "cd '$REMOTE_DIR' && \
        env $common AGENT_GROUP=239.77.0.2 AGENT_PEERS=$remote_peers AGENT_DURATION_MS=9000 AGENT_NAME=remote-1 ./bun agent.js & \
        sleep 0.4; cd '$REMOTE_DIR' && \
        env $common AGENT_GROUP=239.77.0.2 AGENT_PEERS=$remote_peers AGENT_DURATION_MS=8600 AGENT_NAME=remote-2 ./bun agent.js & \
        wait" > "$LOCAL_TMP/$label-remote.log" 2>&1 &
    local ssh_pid=$!
    sleep 2
    env $common AGENT_GROUP=239.77.0.1 AGENT_PEERS=$local_peers AGENT_DURATION_MS=6000 AGENT_NAME=local \
        bun "$LOCAL_TMP/agent.js" > "$LOCAL_TMP/$label-local.log" 2>&1
    wait "$ssh_pid"

    cat "$LOCAL_TMP/$label-local.log" "$LOCAL_TMP/$label-remote.log" | grep '^{' | python3 -c '
import sys, json
seen, ready = {}, set()
for line in sys.stdin:
    e = json.loads(line)
    if e["ev"] == "ready": ready.add(e["agent"])
    if e["ev"] == "seen": seen.setdefault(e["agent"] + "<-" + e["from"], []).append(e.get("remote_host"))
json.dump({"ready": sorted(ready), "seen": seen}, sys.stdout)
' > "$LOCAL_TMP/$label.json"
}

# check <label> <pair> <expect: seen|not-seen>
FAILED=0
check() {
    local label=$1 pair=$2 expect=$3
    local actual
    actual=$(python3 -c "import json,sys; d=json.load(open('$LOCAL_TMP/$label.json')); print('seen' if '$pair' in d['seen'] else 'not-seen')")
    if [ "$actual" = "$expect" ]; then echo "  ok    $label: $pair $expect"
    else echo "  FAIL  $label: $pair expected $expect, got $actual"; FAILED=1; fi
}

check_ready() {
    local label=$1 n
    n=$(python3 -c "import json; print(len(json.load(open('$LOCAL_TMP/$label.json'))['ready']))")
    if [ "$n" = 3 ]; then echo "  ok    $label: all 3 agents ready"
    else echo "  FAIL  $label: only $n/3 agents became ready"; FAILED=1; fi
}

echo "==> scenario 1: control, no peers"
run_scenario control off
check_ready control
check control 'local<-remote-1' not-seen
check control 'local<-remote-2' not-seen
check control 'remote-1<-local' not-seen
check control 'remote-2<-local' not-seen
check control 'remote-1<-remote-2' seen
check control 'remote-2<-remote-1' seen

echo "==> scenario 2: explicit peers"
run_scenario peers on
check_ready peers
for pair in 'local<-remote-1' 'local<-remote-2' 'remote-1<-local' 'remote-2<-local' 'remote-1<-remote-2' 'remote-2<-remote-1'; do
    check peers "$pair" seen
done
python3 -c "
import json
d = json.load(open('$LOCAL_TMP/peers.json'))['seen']
for agent in ('remote-1', 'remote-2'):
    direct = sum(1 for h in d.get(agent + '<-local', []) if h == '$LOCAL_LAN_IP')
    print(f'  info  {agent} got {direct} copies of local straight from $LOCAL_LAN_IP (the rest were relayed on-host)')
"

if [ "$FAILED" = 0 ]; then echo "==> PASS"; else echo "==> FAIL"; fi
exit "$FAILED"
