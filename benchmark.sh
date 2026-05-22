#!/usr/bin/env bash

set -euo pipefail

CPU=10
SIBLING=$(cat /sys/devices/system/cpu/cpu${CPU}/topology/thread_siblings_list \
    | tr ',' '\n' \
    | awk -F'-' 'NF==2{for(i=$1;i<=$2;i++) print i} NF==1{print $1}' \
    | grep -v "^${CPU}$" | head -1)

cargo build -r --example ping
cargo build -r --example pong

pong_measure() {
    local pong_cpu=$1
    local pong_spin=$2
    local label=$3
    printf "  CPU %2d %-30s" "$pong_cpu" "$label"
    taskset -c "$pong_cpu" ./target/release/examples/pong "$pong_spin" 2>&1 \
        | grep "Average" | sed 's/Average round-trip time: //'
}

run_pass() {
    local ping_spin=$1
    local pong_spin=$2
    local label=$3
    local cpus_to_test=${4:-all}  # "focused" = same+sibling only, "all" = 0..19

    echo ""
    echo "=== Pass: $label (spin=$pong_spin) ==="

    rm -rf /dev/shm/pingpong
    taskset -c $CPU ./target/release/examples/ping $ping_spin 2>/dev/null &
    local ping_pid=$!
    sleep 0.5

    if [ "$cpus_to_test" = "focused" ]; then
        pong_measure $CPU          "$pong_spin" "(same CPU)"
        if [ -n "$SIBLING" ]; then
            pong_measure $SIBLING "$pong_spin" "(hyperthread sibling)"
        fi
    else
        for i in {0..19}; do
            if [ "$i" -eq "$CPU" ]; then
                pong_measure $i "$pong_spin" "(same CPU)"
            elif [ -n "$SIBLING" ] && [ "$i" -eq "$SIBLING" ]; then
                pong_measure $i "$pong_spin" "(hyperthread sibling)"
            else
                pong_measure $i "$pong_spin" ""
            fi
        done
    fi

    kill $ping_pid 2>/dev/null
    wait $ping_pid 2>/dev/null || true
}

# Pass 1: sleep-only — spin=0 skips all spinning and yields to the OS immediately.
# Only tested on same-CPU and hyperthread sibling to avoid the 270s × 20 CPU blowup
# (10k rounds × ~27µs futex RTT per round × 20 CPUs = 90 minutes).
# Cross-CPU with spin=0 is always slow (~27 µs/round), nothing interesting there.
run_pass 0 0 "sleep-only (same-CPU optimal)" focused

# Pass 2: PAUSE spin — optimal for cross-CPU.
run_pass 1000 1000 "PAUSE spin" all

