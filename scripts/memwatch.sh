#!/usr/bin/env bash

# Memory watchdog for bench and test runs: executes a command while sampling
# the summed RSS of its whole process tree every 2 seconds; if the sum ever
# exceeds the cap, the tree is killed and the run FAILS (exit 97).
#
# Motivation (D2 M5): the walk-time edge checks briefly carried a
# registration-multiplication defect that made a single compare() call on a
# width-64 antichain exceed 11 GB RSS, and `cargo bench` OOMed a 128 GB
# machine before any number was reported. Criterion happily times an
# allocation explosion right up to the jetsam kill, so bench evidence runs
# ride under this wrapper: any regression of that class becomes a clean,
# attributable failure instead of a dead machine.
#
# Usage: scripts/memwatch.sh CAP_MB command [args...]
# Example: scripts/memwatch.sh 8192 cargo bench -p ankurah-core \
#              --features bench-internals --bench event_dag
# Exit: the command's exit status, or 97 on a cap breach.

set -u

if [ "$#" -lt 2 ]; then
    echo "usage: $0 CAP_MB command [args...]" >&2
    exit 2
fi

CAP_MB="$1"
shift

"$@" &
ROOT=$!

# All live descendants of $ROOT (inclusive), by walking ppid links.
tree_pids() {
    ps -axo pid=,ppid= | awk -v root="$ROOT" '
        { pid[NR] = $1; ppid[NR] = $2 }
        END {
            want[root] = 1
            changed = 1
            while (changed) {
                changed = 0
                for (i = 1; i <= NR; i++)
                    if ((ppid[i] in want) && !(pid[i] in want)) { want[pid[i]] = 1; changed = 1 }
            }
            for (p in want) print p
        }'
}

while :; do
    STATE=$(ps -o state= -p "$ROOT" 2>/dev/null)
    case "$STATE" in
        "" | *Z*) break ;; # exited (or zombie awaiting our wait below)
    esac
    PIDS=$(tree_pids)
    if [ -n "$PIDS" ]; then
        RSS_KB=$(ps -o rss= -p "$(echo "$PIDS" | tr '\n' ',' | sed 's/,$//')" 2>/dev/null | awk '{ s += $1 } END { print s + 0 }')
        RSS_MB=$((RSS_KB / 1024))
        if [ "$RSS_MB" -gt "$CAP_MB" ]; then
            echo "MEMWATCH BREACH: process tree RSS ${RSS_MB} MB exceeds cap ${CAP_MB} MB; killing the tree" >&2
            # shellcheck disable=SC2086 # word-splitting the pid list is the point
            kill -KILL $PIDS 2>/dev/null
            wait "$ROOT" 2>/dev/null
            exit 97
        fi
    fi
    sleep 2
done

wait "$ROOT"
exit $?
