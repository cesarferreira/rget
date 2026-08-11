#!/usr/bin/env bash
# Add artificial RTT / loss to loopback so the bench harness can model a real
# path: TCP slow start, congestion response, and per-flow fairness. These are
# the effects rangeserver.py's token bucket CANNOT model, and the most likely
# home of the unexplained live-CDN regression in issue #1.
#
#   sudo scripts/bench/netem.sh on 20ms 0%     # 20 ms RTT, no loss
#   sudo scripts/bench/netem.sh on 100ms 0.1%  # high BDP, light loss
#   sudo scripts/bench/netem.sh off
#   scripts/bench/netem.sh status
#
# WARNING: this affects ALL loopback traffic on the host, not just the benchmark.
# Anything talking to 127.0.0.1 (databases, dev servers, IDE language servers)
# gets the same delay. Turn it off when you are done.
set -euo pipefail

DEV=lo
ACTION=${1:-status}

case "$ACTION" in
  on)
    DELAY=${2:-20ms}
    LOSS=${3:-0%}
    # netem delay applies per direction, so half the RTT each way.
    HALF=$(python3 -c "
import re,sys
v=sys.argv[1]
n=float(re.sub(r'[a-z]+$','',v))
print(f'{n/2}ms')" "$DELAY")
    tc qdisc del dev "$DEV" root 2>/dev/null || true
    if [ "$LOSS" = "0%" ]; then
      tc qdisc add dev "$DEV" root netem delay "$HALF"
    else
      tc qdisc add dev "$DEV" root netem delay "$HALF" loss "$LOSS"
    fi
    echo "netem on $DEV: ${DELAY} RTT (${HALF} each way), loss ${LOSS}"
    echo "verify with: ping -c3 127.0.0.1"
    ;;
  off)
    tc qdisc del dev "$DEV" root 2>/dev/null || true
    echo "netem removed from $DEV"
    ;;
  status)
    tc qdisc show dev "$DEV"
    ;;
  *)
    echo "usage: $0 {on <rtt> <loss>|off|status}" >&2
    exit 2
    ;;
esac
