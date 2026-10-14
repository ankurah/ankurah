"""Versioned benchmark records. Standard library only; never executes input data."""

import copy
import hashlib
import json
import math
import re
from pathlib import Path


REPOSITORY = "ankurah/ankurah"
RAW_BASE = f"https://raw.githubusercontent.com/{REPOSITORY}/benchmark-data"
SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,99}\Z")


def encoded(value):
    return (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def load(path):
    path = Path(path)
    if path.is_symlink() or path.stat().st_size > 20_000_000:
        raise ValueError("JSON must be a regular file under 20 MB")
    def reject_constant(value):
        raise ValueError(f"Non-finite JSON value: {value}")
    return json.loads(path.read_text(), parse_constant=reject_constant)


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(encoded(value))


def empty_history():
    return {"schema_version": 1, "runs": [], "attempts": [], "coverage": []}


def compatibility(run, workload=None):
    # Source SHA is deliberately excluded: equal configurations form a trend.
    fields = {key: run[key] for key in (
        "kind", "runner", "compiler", "profile", "parameters", "profile_sha256"
    )}
    if "series_generation" in run["harness"]:
        fields["series_generation"] = run["harness"]["series_generation"]
    semantic = run["harness"].get("workload_semantics", {}).get(workload)
    # Only an explicitly audited workload contract can bridge API-only adapters.
    # Exact instrument bytes remain part of run deduplication and provenance.
    fields["harness"] = {"workload_semantic_version": semantic} if semantic else {
        key: run["harness"][key] for key in ("id", "sha256")}

    return digest(fields)


def measurement_series(run, workload):
    semantic = run["harness"].get("workload_semantics", {}).get(workload)
    return f"{semantic or run['harness']['id']}:{workload}:{compatibility(run, workload)}"


def comparison_key(run):
    return digest({"sha": run["source"]["sha"], "patch": run["source"].get("patch_sha256"),
                   "lockfile": run["lockfile_sha256"], "build": run["build_sha256"], "configuration": compatibility(run)})


def extract(criterion, run, config):
    """Use Criterion's median estimate and its own bootstrap interval, never slope."""
    expected = {name for suite in config["suites"] for name in suite["workloads"]}
    names = {original: name for suite in config["suites"]
             for name, original in suite.get("workload_names", {}).items()}
    measurements, raw = [], {}
    for path in sorted(Path(criterion).glob("**/new/benchmark.json")):
        benchmark = load(path)
        original = benchmark["full_id"]
        name = names.get(original, original)
        if name not in expected:
            continue
        if name in raw:
            raise ValueError(f"Duplicate Criterion workload: {name}")
        estimates, sample = load(path.with_name("estimates.json")), load(path.with_name("sample.json"))
        median = estimates["median"]
        interval = median["confidence_interval"]
        times, iterations = sample["times"], sample["iters"]
        if len(times) != len(iterations) or not times or any(not finite(x) or x <= 0 for x in iterations):
            raise ValueError(f"Invalid samples: {name}")
        if any(not finite(x) or x < 0 for x in times):
            raise ValueError(f"Invalid sample times: {name}")
        parameters = dict(config["parameters"])
        parameters["sample_size"] = len(times)  # Criterion groups may override the requested default.
        measurements.append({"workload": name, "unit": "ns", "median": median["point_estimate"],
                             "lower": interval["lower_bound"], "upper": interval["upper_bound"],
                             "confidence_level": interval["confidence_level"], "samples": len(times),
                             "parameters": parameters,
                             "series": measurement_series(run, name)})
        if name != original:
            measurements[-1]["original_workload"] = original
        raw[name] = {"benchmark": benchmark, "estimates": estimates, "sample": sample}
    missing = expected - raw.keys()
    if missing:
        raise ValueError(f"Missing Criterion workloads: {', '.join(sorted(missing))}")
    for measurement in measurements:
        validate_measurement(measurement)
    return measurements, raw


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def validate_measurement(value):
    if value["unit"] not in ("ns", "reads") or not isinstance(value["series"], str) or not value["series"]:
        raise ValueError("Invalid measurement unit/series")
    if any(not finite(value[key]) for key in ("median", "lower", "upper", "confidence_level")):
        raise ValueError("Non-finite measurement")
    if not 0 <= value["lower"] <= value["median"] <= value["upper"] or not 0 < value["confidence_level"] < 1:
        raise ValueError("Invalid median confidence interval")


def targets_complete(records):
    successful = [record for record in records if record["status"] == "ok"]
    if any(not record.get("target_results") for record in successful):
        return True  # Existing fixed-suite schema-v1 records.
    wanted = {target["id"] for record in records for target in record.get("target_results", [])}
    completed = {target["id"] for record in successful for target in record.get("target_results", []) if target["status"] == "ok"}
    return bool(wanted) and wanted <= completed


def validate_bundle(bundle, plan):
    if bundle["schema_version"] != 1 or bundle["id"] != plan["id"] or not SAFE_ID.fullmatch(bundle["id"]):
        raise ValueError("Invalid request identity")
    if bundle["source"] != plan["source"] or bundle["kind"] != plan["kind"] or bundle["markers"] != plan["markers"]:
        raise ValueError("Result does not match the trusted request")
    registered = {target["id"]: target for target in plan.get("benchmarks", [])}
    for target in bundle.get("benchmarks", []):
        trusted = registered.get(target["id"])
        if not trusted or any(target[key] != trusted[key] for key in ("package", "target")):
            raise ValueError("Catalog changed a trusted target identity")
        if target["status"] not in ("registered", "discovered", "unsupported", "failed"):
            raise ValueError("Unknown catalog target status")
    if "reuse_id" in bundle:
        if not SAFE_ID.fullmatch(bundle["reuse_id"]) or not re.fullmatch(r"[a-f0-9]{64}", bundle["comparison_key"]):
            raise ValueError("Invalid reused run identity")
        return
    record = bundle["record"]
    for key in ("id", "source", "kind", "markers", "run_url"):
        if record[key] != plan[key]:
            raise ValueError(f"Result changed trusted {key}")
    if record["status"] in ("failed", "unsupported"):
        if record.get("measurements") or not record.get("reason"):
            raise ValueError("Failed attempt cannot contain measurements")
        return
    if record["status"] != "ok" or not record["measurements"]:
        raise ValueError("Unknown or empty result")
    if record["raw_url"] != f"{RAW_BASE}/runs/{record['id']}/raw.json":
        raise ValueError("Unexpected raw evidence URL")
    if record["harness"] != plan["harness"] or record["parameters"] != plan["config"]["parameters"]:
        raise ValueError("Result changed the harness configuration")
    if record["comparison_key"] != comparison_key(record):
        raise ValueError("Invalid comparison key")
    if plan["config"].get("auto_discover"):
        from catalog import workload_id
        suites = {suite["id"]: suite for suite in plan["config"]["suites"]}
        expected = set()
        for target in record["target_results"]:
            if target["id"] not in suites:
                raise ValueError("Unknown benchmark target")
            if target["status"] == "ok":
                for original in target["original_workloads"]:
                    expected.add(workload_id(suites[target["id"]], original))
    else:
        expected = {name for suite in plan["config"]["suites"] for name in suite["workloads"]}
    if set(bundle["raw"]) != expected or {m["workload"] for m in record["measurements"]} != expected:
        raise ValueError("Unexpected/missing raw workloads")
    if len(record["measurements"]) != len(expected):
        raise ValueError("Duplicate workload")
    for measurement in record["measurements"]:
        validate_measurement(measurement)
        name = measurement["workload"]
        if measurement["series"] != measurement_series(record, name):
            raise ValueError("Invalid compatibility series")
        estimate = bundle["raw"][name]["estimates"]["median"]
        interval = estimate["confidence_interval"]
        if measurement["confidence_level"] != plan["config"]["parameters"]["confidence_level"]:
            raise ValueError("Unexpected confidence level")
        if [measurement[k] for k in ("median", "lower", "upper", "confidence_level")] != [
            estimate["point_estimate"], interval["lower_bound"], interval["upper_bound"], interval["confidence_level"]
        ]:
            raise ValueError("Summary does not match raw median")


def union_markers(left, right):
    return sorted({json.dumps(marker, sort_keys=True): marker for marker in left + right}.values(),
                  key=lambda marker: (marker["kind"], marker["label"]))


def merge(history, bundle, repeat=False):
    """Idempotent projection; explicit repeats remain separate plotted points."""
    history = copy.deepcopy(history)
    if history["schema_version"] != 1:
        raise ValueError("Unsupported history schema")
    runs = {run["id"]: run for run in history["runs"]}
    if "reuse_id" in bundle:
        run = runs[bundle["reuse_id"]]
        if (run["source"]["sha"] != bundle["source"]["sha"] or run["kind"] != bundle["kind"]
                or run["comparison_key"] != bundle["comparison_key"]):
            raise ValueError("Reused run is from another source/lane")
        for measured in history["runs"]:
            if measured["source"]["sha"] == bundle["source"]["sha"]:
                measured["markers"] = union_markers(measured["markers"], bundle["markers"])
        return history
    record = copy.deepcopy(bundle["record"])
    collection = history["runs"] if record["status"] == "ok" else history["attempts"]
    comparable = [run for run in history["runs"] if record["status"] == "ok"
                  and run["comparison_key"] == record["comparison_key"] and run["id"] != record["id"]]
    if comparable and not repeat:
        seen = {(m["workload"], m["series"]) for run in comparable for m in run["measurements"]}
        # Target retries can add previously missing workloads without duplicating
        # the successful target. All unfiltered records/raw stay immutable on disk.
        record["measurements"] = [m for m in record["measurements"] if (m["workload"], m["series"]) not in seen]
    existing = next((entry for entry in collection if entry["id"] == record["id"]), None)
    if existing:
        original = copy.deepcopy(existing)
        original["markers"] = record["markers"]
        if original != record:
            raise ValueError("Attempt ID already has different immutable data")
    elif record["status"] == "ok" and not record["measurements"]:
        receipt = {"id": record["id"], "reuses": comparable[0]["id"], "reason": "Concurrent identical source/configuration"}
        if receipt not in history.setdefault("duplicates", []):
            history["duplicates"].append(receipt)
    else:
        collection.append(record)
    # Main/release/PR observations apply to all measurements of that exact commit.
    for run in history["runs"]:
        if run["source"]["sha"] == bundle["source"]["sha"]:
            run["markers"] = union_markers(run["markers"], bundle["markers"])
    return history


def write_immutable(path, value):
    path = Path(path)
    if path.exists():
        if path.is_symlink() or path.read_bytes() != encoded(value):
            raise ValueError(f"Refusing to replace immutable record: {path}")
    else:
        save(path, value)


def publish_files(root, bundle, plan):
    root = Path(root)
    validate_bundle(bundle, plan)
    # The data branch is not executable input. Reject symlinks before any writes.
    if any(path.is_symlink() for path in root.rglob("*") if ".git" not in path.parts):
        raise ValueError("Data branch contains a symlink")
    history_path = root / "history.json"
    history = load(history_path) if history_path.exists() else empty_history()
    if "reuse_id" in bundle:
        reused = next(run for run in history["runs"] if run["id"] == bundle["reuse_id"])
        if reused["harness"]["sha256"] != plan["harness"]["sha256"] or reused["parameters"] != plan["config"]["parameters"]:
            raise ValueError("Reused run has another harness/configuration")
    if "record" in bundle:
        for target in bundle["record"].get("target_results", []):
            if target["status"] == "reused":
                original = next((record for record in history["runs"] if record["id"] == target["run_id"]), None)
                if not original or original["comparison_key"] != bundle["record"]["comparison_key"] or not any(
                    prior["id"] == target["id"] and prior["status"] == "ok" and prior["original_workloads"] == target["original_workloads"]
                    for prior in original.get("target_results", [])):
                    raise ValueError("Reused target has no matching completed source/configuration")
    updated = merge(history, bundle, repeat=plan.get("repeat", False))
    if "coverage" in plan:
        write_immutable(root / "catalogs" / f"{bundle['id']}.json", plan["coverage"])
        coverage = {(entry["source"]["sha"] or entry["source"]["label"], entry["kind"]): entry for entry in history.get("coverage", [])}
        coverage.update({(entry["source"]["sha"] or entry["source"]["label"], entry["kind"]): entry for entry in plan["coverage"]})
        updated["coverage"] = list(coverage.values())
    for entry in updated.get("coverage", []):
        if entry["source"]["sha"] is None:
            continue
        matches = [record for record in updated["runs"] + updated["attempts"]
                   if record["source"]["sha"] == entry["source"]["sha"] and record["kind"] == entry["kind"] and
                   (not entry.get("harness_sha256") or record.get("harness", {}).get("sha256") == entry["harness_sha256"])]
        matches.sort(key=lambda record: record.get("recorded_at", ""))
        success = [record for record in matches if record["status"] == "ok"]
        if targets_complete(matches):
            entry.update(status="measured", run_ids=[record["id"] for record in success])
            entry.pop("reason", None)
        elif matches and not entry.get("retry_after"):
            last = matches[-1]
            entry.update(status="pending" if last["status"] == "ok" else last["status"],
                         reason=last.get("reason", "Some benchmark targets remain incomplete; successful targets are retained"))
    receipt = {key: bundle[key] for key in ("schema_version", "id", "source", "kind", "markers")}
    receipt["repeat"] = plan.get("repeat", False)
    if "reuse_id" in bundle:
        receipt.update(reuse_id=bundle["reuse_id"], comparison_key=bundle["comparison_key"])
    else:
        receipt.update(run_id=bundle["record"]["id"], status=bundle["record"]["status"])
        if "comparison_key" in bundle["record"]:
            receipt["comparison_key"] = bundle["record"]["comparison_key"]
    targets = {entry["id"]: entry for entry in history.get("benchmarks", [])}
    for entry in plan.get("benchmarks", []) + bundle.get("benchmarks", []):
        prior = targets.get(entry["id"], {})
        workloads = {value["id"]: value for value in prior.get("workloads", [])}
        workloads.update({value["id"]: value for value in entry.get("workloads", [])})
        merged = dict(entry, workloads=list(workloads.values()))
        if entry["status"] == "registered" and prior.get("status") == "discovered":
            merged["status"] = "discovered"
        targets[entry["id"]] = merged
    updated["benchmarks"] = list(targets.values())
    if plan.get("benchmarks") or bundle.get("benchmarks"):
        write_immutable(root / "catalogs" / f"{bundle['id']}-benchmarks.json",
                        {"registered": plan.get("benchmarks", []), "observed": bundle.get("benchmarks", [])})
    write_immutable(root / "requests" / f"{bundle['id']}.json", receipt)
    if "record" in bundle:
        record = bundle["record"]
        write_immutable(root / "runs" / record["id"] / "run.json", record)
        write_immutable(root / "runs" / record["id"] / "raw.json", bundle.get("raw", {}))
        if bundle.get("adapter"):
            if bundle["record"].get("adapter", {}).get("sha256") != digest(bundle["adapter"]):
                raise ValueError("Adapter provenance hash mismatch")
            write_immutable(root / "runs" / record["id"] / "adapter.json", bundle["adapter"])
        if bundle.get("diagnostics"):
            write_immutable(root / "runs" / record["id"] / "diagnostics.json", bundle["diagnostics"])
    save(history_path, updated)
