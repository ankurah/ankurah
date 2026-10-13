#!/usr/bin/env python3
"""Compare identical public-API cold and retained-query fixtures, one process per sample."""

import argparse
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import signal
import statistics
import subprocess
import sys
import time


def run_sample(binary, env, log, timeout):
    """Reap directly so CPU and peak RSS belong to this child, not previous samples."""
    start = time.monotonic()
    child = subprocess.Popen([str(binary)], env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    timed_out = False
    while True:
        pid, status, usage = os.wait4(child.pid, os.WNOHANG)
        if pid:
            child.returncode = os.waitstatus_to_exitcode(status)
            break
        if time.monotonic() - start >= timeout:
            timed_out = True
            os.killpg(child.pid, signal.SIGKILL)
            _, status, usage = os.wait4(child.pid, 0)
            child.returncode = os.waitstatus_to_exitcode(status)
            break
        time.sleep(0.005)
    return ("timeout" if timed_out else child.returncode), {
        "process_wall_seconds": time.monotonic() - start,
        "process_user_cpu_seconds": usage.ru_utime,
        "process_system_cpu_seconds": usage.ru_stime,
        "process_peak_rss_bytes": usage.ru_maxrss if sys.platform == "darwin" else usage.ru_maxrss * 1024,
        "process_minor_faults": usage.ru_minflt,
        "process_major_faults": usage.ru_majflt,
        "scope": "whole fresh process: setup, prefetch, warmup, measurement, verification, teardown",
    }


def distribution(values):
    median = statistics.median(values)
    return {
        "samples": len(values), "median": median, "min": min(values), "max": max(values),
        "mean": statistics.mean(values),
        "stdev": statistics.stdev(values) if len(values) > 1 else None,
        "median_absolute_deviation": statistics.median(abs(x - median) for x in values),
    }


def summarize(data):
    if not data:
        return None
    times = distribution([x["elapsed_us"] / 1000 for x in data])
    result = {"samples": len(data), "median_ms": times.pop("median"), "min_ms": times.pop("min"),
              "max_ms": times.pop("max"), "latency_distribution_ms": times,
              "settled_distribution_ms": distribution([x["settled_us"] / 1000 for x in data]),
              "initial_drained_distribution_ms": distribution([x["initial_drained_us"] / 1000 for x in data]),
              "transport_settled_distribution_ms": distribution([x["transport_settled_us"] / 1000 for x in data]),
              "verification_distribution_ms": distribution([x["verification_us"] / 1000 for x in data]),
              "median_requester_messages": statistics.median(x["traffic"]["requester_messages"] for x in data),
              "median_bytes": statistics.median(x["traffic"]["requester_bytes"] + x["traffic"]["responder_bytes"] for x in data),
              "largest_message_bytes": max(x["traffic"]["max_message_bytes"] for x in data)}
    if all("local_result_installed_us" in x for x in data):
        installed = [x["local_result_installed_us"] for x in data]
        result["local_result_installed_distribution_ms"] = distribution([value / 1000 for value in installed]) if all(value is not None for value in installed) else None
    for name in ("process_user_cpu_seconds", "process_system_cpu_seconds", "process_peak_rss_bytes"):
        result[name] = distribution([x["resources"][name] for x in data])
    if all("phase_process_cpu" in x for x in data):
        result["phase_process_cpu_us"] = {}
        for phase in ("setup", "through_ready", "through_initial_drain", "through_local_result_installed", "verification", "through_verified", "after_verification_drain", "through_final_drain"):
            values = [x["phase_process_cpu"].get(phase) for x in data]
            result["phase_process_cpu_us"][phase] = {
                name: distribution([value[name] for value in values]) for name in ("user_us", "system_us")
            } if all(value is not None for value in values) else None
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("baseline", "candidate", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--baseline-revision", required=True)
    parser.add_argument("--candidate-revision", required=True)
    parser.add_argument("--sizes", default="0,8,32,256,4096")
    parser.add_argument("--rtts", default="0,50")
    parser.add_argument("--bandwidths", default="0", help="bytes/second per direction; zero is unlimited")
    parser.add_argument("--modes", default="cold,warm_refresh,warm_equal,warm_edit,warm_membership")
    parser.add_argument("--cache", default="empty,half,all")
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--timeout", type=float, default=240)
    args = parser.parse_args()
    sizes, rtts, bandwidths = ([int(n) for n in text.split(",")] for text in (args.sizes, args.rtts, args.bandwidths))
    caches, modes = args.cache.split(","), args.modes.split(",")
    if args.repeats < 1 or args.timeout <= 0 or any(n < 0 for n in sizes + rtts + bandwidths):
        parser.error("positive repeats/timeout and nonnegative sizes/RTTs/bandwidths required")
    if any(cache not in ("empty", "half", "all", "half_blocks", "half_interleaved") for cache in caches):
        parser.error("cache modes: empty,half,all,half_blocks,half_interleaved")
    if any(mode not in ("cold", "warm_equal", "warm_edit", "warm_membership", "warm_refresh") for mode in modes):
        parser.error("modes: cold,warm_refresh,warm_equal,warm_edit,warm_membership")
    cases = [(n, rtt, bps, mode, cache) for n, rtt, bps, mode, cache in itertools.product(sizes, rtts, bandwidths, modes, caches)
             if not ((n == 0 and cache != "empty") or (mode != "cold" and n == 0) or (mode not in ("cold", "warm_refresh") and cache != "all"))]
    if not cases:
        parser.error("matrix contains no cases")
    binaries = {label: getattr(args, label).resolve(strict=True) for label in ("baseline", "candidate")}
    args.output.mkdir(parents=True, exist_ok=False)
    metadata = {
        "platform": platform.platform(), "compiler": subprocess.check_output(["rustc", "--version"], text=True).strip(),
        "baseline_revision": args.baseline_revision, "candidate_revision": args.candidate_revision,
        "binaries": {label: {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                     for label, path in binaries.items()},
        "harness_sha256": hashlib.sha256(Path(__file__).with_name("live_query_startup.rs").read_bytes()).hexdigest(),
        "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "sizes": sizes, "rtt_ms": rtts, "bandwidth_bytes_per_second": bandwidths, "cache": caches, "modes": modes,
        "repeats": args.repeats, "sample_timeout_seconds": args.timeout,
        "order": "AB then BA, alternating per repetition within each case",
        "scope": "cold no-query-tree durable answer or retained-query reconnect fully settled; setup/prefetch excluded from latency",
        "transport": "serialized real-node in-process full-duplex link; per-direction shared bandwidth serialization plus one-way RTT/2 propagation; no transport framing or socket cost",
        "resource_scope": "CPU and peak RSS cover whole isolated fixture process; storage counters and traffic cover timed transfer plus settle tail",
        "warm_endpoint": "same-selection refresh uses versioned durable ready; reconnect resets public resultset loading metadata before arm/timing, observes first fresh loaded=true local result installation via O(1) Signal.listen, requires its own new QuerySubscribed, and immediately verifies exact membership/state/clock; installation precedes durable bookkeeping, ready_us is null, final drain tracked separately",
    }
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    results, failures, summary = [], [], []
    for n, rtt, bps, mode, cache in cases:
        samples = {label: [] for label in binaries}
        for repeat in range(args.repeats):
            order = ("baseline", "candidate") if repeat % 2 == 0 else ("candidate", "baseline")
            for label in order:
                name = f"{mode}-{n}-{cache}-{rtt}ms-{bps}bps-{repeat}-{label}.log"
                env = dict(os.environ, STARTUP_LABEL=label, STARTUP_SIZES=str(n), STARTUP_RTT_MS=str(rtt),
                           STARTUP_BPS=str(bps), STARTUP_MODES=mode, STARTUP_CACHE=cache, STARTUP_REPEATS="1")
                with (args.output / name).open("w") as log:
                    status, resources = run_sample(binaries[label], env, log, args.timeout)
                lines = (args.output / name).read_text().splitlines()
                records = [json.loads(line[8:]) for line in lines if line.startswith("STARTUP ")]
                if status != 0 or len(records) != 1:
                    failure = {"file": name, "status": status, "records": len(records), "resources": resources,
                               "last_phase": next((line[6:] for line in reversed(lines) if line.startswith("PHASE ")), None)}
                    failures.append(failure)
                    (args.output / "results.json").write_text(json.dumps({"samples": results, "summary": summary, "failures": failures}, indent=2) + "\n")
                    print(json.dumps({"failure": failure}), flush=True)
                    continue
                record = records[0]
                if (record["entities"], record["cache"], record["mode"], record["rtt_ms"], record["bandwidth_bytes_per_second"], record["label"], record["verified"]) != (n, cache, mode, rtt, bps, label, True):
                    raise RuntimeError(f"unexpected or unverified workload in {name}")
                record.update(repeat=repeat, resources=resources, raw_log=name)
                samples[label].append(record)
                results.append(record)
        row = {"entities": n, "cache": cache, "mode": mode, "rtt_ms": rtt, "bandwidth_bytes_per_second": bps,
               "complete": all(len(data) == args.repeats for data in samples.values())}
        row.update({label: summarize(data) for label, data in samples.items()})
        if row["baseline"] and row["candidate"]:
            row["median_ratio"] = row["candidate"]["median_ms"] / row["baseline"]["median_ms"]
            row["median_change_ms"] = row["candidate"]["median_ms"] - row["baseline"]["median_ms"]
            paired = {label: {x["repeat"]: x["elapsed_us"] / 1000 for x in data} for label, data in samples.items()}
            common = paired["baseline"].keys() & paired["candidate"].keys()
            row["paired_change_ms"] = distribution([paired["candidate"][i] - paired["baseline"][i] for i in common]) if common else None
        summary.append(row)
        (args.output / "results.json").write_text(json.dumps({"samples": results, "summary": summary, "failures": failures}, indent=2) + "\n")
        print(json.dumps(row), flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
