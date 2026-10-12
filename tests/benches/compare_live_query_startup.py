#!/usr/bin/env python3
"""Run identical cold-query fixtures on two compiled revisions, one process per sample."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline-revision", required=True)
    parser.add_argument("--candidate-revision", required=True)
    parser.add_argument("--sizes", default="0,8,32,256,4096")
    parser.add_argument("--rtts", default="0,50")
    parser.add_argument("--cache", default="empty,half,all")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=240)
    args = parser.parse_args()
    sizes = [int(n) for n in args.sizes.split(",")]
    rtts = [int(n) for n in args.rtts.split(",")]
    caches = args.cache.split(",")
    if args.repeats < 1 or args.timeout <= 0 or any(n < 0 for n in sizes + rtts):
        parser.error("positive repeats/timeout and nonnegative sizes/RTTs required")
    if any(cache not in ("empty", "half", "all") for cache in caches):
        parser.error("cache modes: empty,half,all")
    if not any(n > 0 or "empty" in caches for n in sizes):
        parser.error("matrix contains no cases: a zero-row result uses cache=empty")
    binaries = {label: getattr(args, label).resolve(strict=True) for label in ("baseline", "candidate")}
    # Refuse to overwrite evidence from an earlier run.
    args.output.mkdir(parents=True, exist_ok=False)
    metadata = {
        "platform": platform.platform(),
        "compiler": subprocess.check_output(["rustc", "--version"], text=True).strip(),
        "baseline_revision": args.baseline_revision,
        "candidate_revision": args.candidate_revision,
        "binaries": {label: {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                     for label, path in binaries.items()},
        "harness_sha256": hashlib.sha256(Path(__file__).with_name("live_query_startup.rs").read_bytes()).hexdigest(),
        "sizes": sizes, "rtt_ms": rtts, "cache": caches, "repeats": args.repeats,
        "sample_timeout_seconds": args.timeout,
        "order": "AB then BA, alternating per repetition within each case",
        "scope": "cold query tree; catalog/setup/prefetch excluded; serialized in-process transport with latency only",
    }
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    results, failures, summary = [], [], []
    for n in sizes:
        for rtt in rtts:
            for cache in caches:
                if n == 0 and cache != "empty":
                    continue
                samples = {label: [] for label in binaries}
                for repeat in range(args.repeats):
                    order = ("baseline", "candidate") if repeat % 2 == 0 else ("candidate", "baseline")
                    for label in order:
                        name = f"{n}-{cache}-{rtt}ms-{repeat}-{label}.log"
                        env = dict(os.environ, STARTUP_LABEL=label, STARTUP_SIZES=str(n), STARTUP_RTT_MS=str(rtt),
                                   STARTUP_CACHE=cache, STARTUP_REPEATS="1")
                        with (args.output / name).open("w") as log:
                            try:
                                process = subprocess.run([str(binaries[label])], env=env, stdout=log,
                                                         stderr=subprocess.STDOUT, timeout=args.timeout)
                                status = process.returncode
                            except subprocess.TimeoutExpired:
                                status = "timeout"
                        records = [json.loads(line[8:]) for line in (args.output / name).read_text().splitlines()
                                   if line.startswith("STARTUP ")]
                        if status != 0 or len(records) != 1:
                            failure = {"file": name, "status": status, "records": len(records)}
                            failures.append(failure)
                            print(json.dumps({"failure": failure}), flush=True)
                            continue
                        record = records[0]
                        if (record["entities"], record["cache"], record["rtt_ms"], record["label"]) != (n, cache, rtt, label):
                            raise RuntimeError(f"unexpected workload in {name}")
                        record["repeat"] = repeat
                        samples[label].append(record)
                        results.append(record)
                row = {"entities": n, "cache": cache, "rtt_ms": rtt,
                       "complete": all(len(data) == args.repeats for data in samples.values())}
                for label, data in samples.items():
                    times = [x["elapsed_us"] / 1000 for x in data]
                    row[label] = None if not times else {
                        "samples": len(times), "median_ms": statistics.median(times),
                        "min_ms": min(times), "max_ms": max(times),
                        "median_requester_messages": statistics.median(x["traffic"]["requester_messages"] for x in data),
                        "median_bytes": statistics.median(x["traffic"]["requester_bytes"] + x["traffic"]["responder_bytes"] for x in data),
                        "largest_message_bytes": max(x["traffic"]["max_message_bytes"] for x in data),
                    }
                if row["baseline"] and row["candidate"]:
                    row["median_ratio"] = row["candidate"]["median_ms"] / row["baseline"]["median_ms"]
                    row["median_change_ms"] = row["candidate"]["median_ms"] - row["baseline"]["median_ms"]
                summary.append(row)
                (args.output / "results.json").write_text(json.dumps({"samples": results, "summary": summary, "failures": failures}, indent=2) + "\n")
                print(json.dumps(row), flush=True)
    # Timing changes are reported, not silently accepted by an arbitrary threshold.
    # A failed/incomplete measurement is a failed run even if other samples pass.
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
