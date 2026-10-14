#!/usr/bin/env python3
"""Trusted controller for benchmark planning, unprivileged measurement and publication."""

import argparse
import datetime
import hashlib
import json
import os
import platform
import re
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

from catalog import registered, enumerate_criterion
from historical import AdapterUnavailable, apply_surface, assert_lock_preserved, file_at, select_harness
from semantics import audited_workloads
from dataset import RAW_BASE, REPOSITORY, comparison_key, digest, empty_history, extract, load, publish_files, save


ROOT = Path(__file__).resolve().parents[2]
RELEASE_TAG = re.compile(r"(?:[A-Za-z0-9_-]+[-@])?v?\d+\.\d+\.\d+(?:[-+][A-Za-z0-9.-]+)?\Z")


def command(args, cwd=ROOT, **kwargs):
    return subprocess.run(args, cwd=cwd, check=True, text=True, stdout=subprocess.PIPE, **kwargs).stdout.strip()


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def get_json(url, token=None):
    import json
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "ankurah-benchmarks"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=30) as response:
        return json.load(response)


def source_sha(ref):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,199}", ref) or ref.startswith("refs/pull/"):
        raise ValueError("Ref must be a commit, main-history branch or tag; pull request refs are unsupported")
    return command(["git", "rev-parse", "--verify", f"{ref}^{{commit}}"])


def ancestor(sha):
    return subprocess.run(["git", "merge-base", "--is-ancestor", sha, "HEAD"], cwd=ROOT).returncode == 0


def harness_configuration(harness_ref=None):
    config = load(ROOT / "tools/benchmarks/config.json")
    # Operators bump this when the stable runner/OS/profile policy changes.
    config["series_generation"] = os.environ.get("BENCHMARK_SERIES_GENERATION", "1")
    revision = None
    if harness_ref:
        revision = source_sha(harness_ref)
        if not ancestor(revision):
            raise ValueError("Historical harness must be reachable from reviewed main")
        # Original historical DAG harness only. Do not guess adaptations to old lane APIs.
        config["suites"] = config["suites"][:1]
        source = command(["git", "show", f"{revision}:core/benches/event_dag.rs"])
        if 'BenchmarkId::new("diamond_chain_drain"' in source:
            config["harness_id"] = "dag-cloned-drain-v1"
            config["suites"][0]["workloads"] = [name.replace("diamond_chain_prepare", "diamond_chain_drain")
                                                  for name in config["suites"][0]["workloads"]]
        elif 'BenchmarkId::new("diamond_chain_prepare"' in source:
            config["harness_id"] = "dag-prepare-v1"
        else:
            raise ValueError("Historical DAG timing boundary is not recognized; an explicit adapter is required")
    if not harness_ref:
        metadata = json.loads(command(["cargo", "metadata", "--no-deps", "--format-version=1"]))
        config["suites"], config["benchmarks"] = registered(metadata, ROOT, config["suites"])
        config["auto_discover"] = True
    files = {}
    for suite in config["suites"]:
        files[suite["file"]] = subprocess.check_output(["git", "show", f"{revision}:{suite['file']}"], cwd=ROOT) if revision else (ROOT / suite["file"]).read_bytes()
    if "core/benches/event_dag.rs" in files:
        files["core/benches/event_dag.rs"] = files["core/benches/event_dag.rs"].replace(b"debug_assert!", b"assert!")
    if revision:
        files["core/src/bench_support.rs"] = subprocess.check_output(["git", "show", f"{revision}:core/src/bench_support.rs"], cwd=ROOT)
        config["surface_adapter_sha256"] = file_hash(ROOT / "tools/benchmarks/historical.py")
    hashes = {name: hashlib.sha256(content).hexdigest() for name, content in files.items()}
    config["workload_semantics"] = audited_workloads(files.get("core/benches/event_dag.rs", b""),
        [name for suite in config["suites"] for name in suite.get("legacy_workloads", suite["workloads"])])
    hashes["config"] = digest(config)
    return config, revision, files, digest(hashes)


def plan(args):
    config, harness_sha, files, harness_hash = harness_configuration(args.harness_ref)
    control_sha = command(["git", "rev-parse", "HEAD"])
    request = {"schema_version": 1, "id": os.environ["BENCHMARK_REQUEST_ID"], "recorded_at": now(),
               "kind": args.kind, "run_url": os.environ["BENCHMARK_RUN_URL"], "markers": [],
               "source": {"repository": REPOSITORY, "ref": args.ref, "sha": None, "label": args.ref},
               "control_sha": control_sha, "config": config, "repeat": args.repeat, "harness_ref": harness_sha,
               "harness": {"id": config["harness_id"], "sha256": harness_hash, "workload_semantics": config.get("workload_semantics", {}), "series_generation": config.get("series_generation", "1"),
                           "description": "Historical DAG harness; end-to-end lanes excluded." if harness_sha else
                                          "Borrowed in-memory DAG comparison and layer preparation; real Sled/local transport lanes.",
                           "source_url": f"https://github.com/{REPOSITORY}/tree/{harness_sha or control_sha}/core/benches"}}
    try:
        sha = source_sha(args.ref)
        request["source"].update(sha=sha, committed_at=command(["git", "show", "-s", "--format=%cI", sha]))
        if not ancestor(sha):
            from backfill import LEGACY_HARNESS, authorize_release
            request["kind"] = "hosted"
            authorization = authorize_release(sha)
            if not authorization:
                raise ValueError("Non-main source requires a verified official crate publication, matching tag/manifest and clean published VCS SHA")
            request["source"]["publication_verification"] = authorization
            if not args.harness_ref:
                config, harness_sha, files, harness_hash = harness_configuration(select_harness(ROOT, sha))
                request.update(config=config, harness_ref=harness_sha)
                request["harness"].update(id=config["harness_id"], sha256=harness_hash, workload_semantics=config.get("workload_semantics", {}), series_generation=config.get("series_generation", "1"),
                    description="Historical DAG harness; end-to-end lanes excluded.",
                    source_url=f"https://github.com/{REPOSITORY}/tree/{harness_sha}/core/benches")
        else:
            request["markers"].append({"kind": "main", "label": sha[:8], "url": f"https://github.com/{REPOSITORY}/commit/{sha}"})
        for tag in command(["git", "tag", "--points-at", sha]).splitlines():
            if RELEASE_TAG.fullmatch(tag):
                request["markers"].append({"kind": "release", "label": tag,
                                           "url": f"https://github.com/{REPOSITORY}/releases/tag/{tag}"})
        page = 1
        while True:
            pulls = get_json(f"https://api.github.com/repos/{REPOSITORY}/commits/{sha}/pulls?per_page=100&page={page}",
                             os.environ.get("GH_TOKEN"))
            for pull in pulls:
                if pull.get("merged_at") and pull["base"]["ref"] == "main" and pull["merge_commit_sha"] == sha:
                    request["markers"].append({"kind": "pr", "label": f"#{pull['number']}", "url": pull["html_url"]})
            if len(pulls) < 100:
                break
            page += 1
    except (ValueError, subprocess.CalledProcessError) as error:
        request["unsupported"] = str(error)
    request["benchmarks"] = config.get("benchmarks", [{"id": f"{suite['package']}/{suite['bench']}",
        "package": suite["package"], "target": suite["bench"], "status": "registered", "workloads": []} for suite in config["suites"]])
    if args.catalog:
        from backfill import discover
        request["coverage"] = discover(request["kind"])["coverage"]
    save(args.output, request)
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write(f"control_sha={control_sha}\n")
        output.write(f"toolchain={config['toolchain']}\n")
        output.write(f"lane={request['kind']}\n")


def history():
    try:
        return get_json(f"{RAW_BASE}/history.json")
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return empty_history()
        raise


def file_hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def incompatible_harness(output, filename, features=()):
    """Only classify concrete benchmark API errors; transport/build failures can retry."""
    for feature in features:
        missing_feature = rf"^error: the package '[^']+' does not contain this feature: {re.escape(feature)}$"
        if re.search(missing_feature, output, re.MULTILINE):
            return True
    errors = []
    for line in output.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("reason") == "compiler-message" and event["message"]["level"] == "error":
            errors.append(event["message"])
    api_codes = {"E0308", "E0432", "E0433", "E0599", "E0603", "E0061", "E0277", "E0053", "E0046", "E0412", "E0425"}
    return bool(errors) and all(
        (error.get("code") or {}).get("code") in api_codes and
        any(span["is_primary"] and span["file_name"].endswith(filename) for span in error["spans"])
        for error in errors)


def measure(args):
    request = load(args.plan)
    bundle = {key: request[key] for key in ("schema_version", "id", "source", "kind", "markers")}
    run = {key: request[key] for key in ("id", "source", "kind", "markers", "run_url", "harness")}
    run.update(recorded_at=now(), status="failed", raw_url=f"{RAW_BASE}/runs/{request['id']}/raw.json")
    bundle.update(record=run, raw={}, benchmarks=request.get("benchmarks", []), diagnostics={})
    # Write a failure placeholder before doing work so interrupted jobs can still publish a status.
    run["reason"] = "Measurement interrupted before completion; see the workflow log"
    save(args.output, bundle)
    try:
        if request.get("unsupported"):
            run.update(status="unsupported", reason=request["unsupported"])
            return
        config = request["config"]
        toolchain = config["toolchain"]
        rustc = command(["rustc", f"+{toolchain}", "-vV"])
        cpu = platform.processor()
        if Path("/proc/cpuinfo").exists():
            cpu = next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
                        if line.startswith("model name")), cpu)
        hardware = os.environ.get("BENCHMARK_HARDWARE") or f"{cpu}; {os.cpu_count()} logical CPUs"
        runner_id = os.environ.get("BENCHMARK_RUNNER_ID") or "github-ubuntu-24.04"
        if request["kind"] == "stable" and (not os.environ.get("BENCHMARK_HARDWARE") or not os.environ.get("BENCHMARK_RUNNER_ID")):
            raise ValueError("Stable lane requires a configured hardware description and permanent machine ID")
        run.update(runner={"id": runner_id, "label": runner_id, "os": platform.platform(), "hardware": hardware},
                   compiler={"version": rustc.splitlines()[0], "target": next(line[6:] for line in rustc.splitlines() if line.startswith("host: "))},
                   profile=config["profile"], parameters=config["parameters"])
        with tempfile.TemporaryDirectory(prefix="ankurah-measure-") as temporary:
            source = Path(temporary) / "source"
            command(["git", "clone", "--no-hardlinks", "--no-checkout", str(ROOT), str(source)])
            command(["git", "checkout", "--detach", request["source"]["sha"]], cwd=source)
            _, _, harness_files, actual_hash = harness_configuration(request.get("harness_ref"))
            if actual_hash != request["harness"]["sha256"]:
                raise ValueError("Harness changed since planning")
            env = {key: value for key, value in os.environ.items() if not any(word in key.upper() for word in ("TOKEN", "SECRET", "PASSWORD"))}
            env.update(CARGO_TARGET_DIR=str(Path(temporary) / "target"), CARGO_INCREMENTAL="0", RUSTFLAGS="")
            for suite in config["suites"]:
                destination = source / suite["file"]
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(harness_files[suite["file"]])
            if request.get("harness_ref"):
                original_lock = (source / "Cargo.lock").read_bytes()
                adapter = apply_surface(source, harness_files["core/src/bench_support.rs"])
                if adapter:
                    # Resolve only added instrument dependencies, then verify that
                    # every original locked package/version/source is preserved.
                    command(["cargo", f"+{toolchain}", "metadata", "--format-version=1"], cwd=source, env=env)
                    assert_lock_preserved(original_lock, (source / "Cargo.lock").read_bytes())
                    adapter["tracked_patch"] = command(["git", "diff", "--binary"], cwd=source)
                    adapter["overlays"]["core/benches/event_dag.rs"] = (source / "core/benches/event_dag.rs").read_text()
                    adapter["effective_lockfile"] = (source / "Cargo.lock").read_text()
                    bundle["adapter"] = adapter
                    run["adapter"] = {"sha256": digest(adapter), "url": f"{RAW_BASE}/runs/{request['id']}/adapter.json",
                                      "original_lockfile_sha256": adapter["original_lockfile_sha256"]}
            run["lockfile_sha256"] = file_hash(source / "Cargo.lock")
            build_files = [source / "Cargo.toml"] + sorted((source / ".cargo").glob("*"))
            run["build_sha256"] = digest({str(path.relative_to(source)): file_hash(path) for path in build_files if path.is_file()})
            import tomllib
            run["profile_sha256"] = digest({"profile": tomllib.loads((source / "Cargo.toml").read_text()).get("profile", {}).get("release", {}),
                                            "cargo_config": {path.name: file_hash(path) for path in build_files[1:] if path.is_file()},
                                            "RUSTFLAGS": "", "CARGO_INCREMENTAL": "0"})
            run["comparison_key"] = comparison_key(run)
            completed_targets = {}
            if not request["repeat"]:
                matching = [entry for entry in history()["runs"] if entry.get("comparison_key") == run["comparison_key"]]
                for entry in matching:
                    for target in entry.get("target_results", []):
                        if target["status"] == "ok":
                            completed_targets[target["id"]] = (entry["id"], target)
                wanted = {suite.get("id", f"{suite['package']}/{suite['bench']}") for suite in config["suites"]}
                previous = next((entry for entry in reversed(matching) if not entry.get("target_results")), None)
                if previous or (matching and wanted <= completed_targets.keys()):
                    bundle.pop("record")
                    bundle.pop("raw")
                    bundle["reuse_id"] = (previous or matching[-1])["id"]
                    bundle["comparison_key"] = run["comparison_key"]
                    return
            commands = []
            for suite in config["suites"]:
                cargo = ["cargo", f"+{toolchain}", "bench", "--locked", "--profile", config["profile"],
                         "-p", suite["package"], "--bench", suite["bench"]]
                if suite["features"]:
                    cargo += ["--features", ",".join(suite["features"])]
                commands.append((suite, cargo))
            # Compile/list each target before timed work. A target failure never
            # discards another target's complete, validated measurements.
            ready = []
            run["target_results"] = []
            run["measurements"] = []
            targets = {entry["id"]: dict(entry) for entry in request.get("benchmarks", [])}
            for suite, cargo in commands:
                identifier = suite.get("id", f"{suite['package']}/{suite['bench']}")
                target = targets.setdefault(identifier, {"id": identifier, "package": suite["package"],
                                                       "target": suite["bench"], "workloads": []})
                if identifier in completed_targets:
                    reused_id, prior = completed_targets[identifier]
                    run["target_results"].append(dict(prior, status="reused", run_id=reused_id))
                    continue
                result_record = {"id": identifier, "status": "failed", "reason": "Target interrupted before completion"}
                run["target_results"].append(result_record)
                result = subprocess.run(cargo + ["--no-run", "--message-format=json"], cwd=source, env=env,
                                        text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                if result.returncode:
                    historical = request["source"]["sha"] != request["control_sha"] or request.get("harness_ref")
                    incompatible = historical and incompatible_harness(result.stdout, suite["file"], suite["features"])
                    reason = f"Cannot compile {suite['bench']} with this instrument/compiler; inspect durable diagnostics"
                    result_record.update(status="unsupported" if incompatible else "failed", reason=reason)
                    target.update(status=result_record["status"], reason=reason)
                    bundle["diagnostics"][identifier] = {"build_log": result.stdout[-150000:], "log_truncated": len(result.stdout) > 150000}
                    continue
                executables = set()
                for line in result.stdout.splitlines():
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if event.get("reason") == "compiler-artifact" and event.get("target", {}).get("name") == suite["bench"] and event.get("executable"):
                        executables.add(event["executable"])
                try:
                    if len(executables) != 1:
                        raise ValueError("Cargo did not identify exactly one benchmark executable")
                    executable = executables.pop()
                    selected = dict(suite)
                    if config.get("auto_discover"):
                        listing = enumerate_criterion(executable, suite, source, env)
                        if listing["status"] != "discovered":
                            raise ValueError(listing["reason"])
                        selected["workload_names"] = listing["workload_names"]
                        selected["workloads"] = list(selected["workload_names"])
                        target.update(status="discovered", workloads=[{"id": name, "original_workload": original, "unit": "ns"}
                            for name, original in selected["workload_names"].items()])
                    result_record["original_workloads"] = list(selected.get("workload_names", {name: name for name in selected["workloads"]}).values())
                    ready.append((selected, executable, target, result_record))
                except (ValueError, subprocess.TimeoutExpired) as error:
                    # Listing errors/timeouts are retryable; only manifest-proven
                    # non-Criterion/custom targets are registered as unsupported.
                    target.update(status="failed", reason=str(error))
                    result_record.update(status="failed", reason=str(error))
            bundle["benchmarks"] = list(targets.values())
            # Persist the catalog before measurements begin, including failed targets.
            save(args.output, bundle)
            for suite, executable, target, result_record in ready:
                settings = config["parameters"]
                criterion_directory = Path(temporary) / "samples" / digest(suite["id"] if "id" in suite else suite["bench"])
                target_env = dict(env, CRITERION_HOME=str(criterion_directory))
                arguments = ["--bench", "--warm-up-time", str(settings["warm_up_seconds"]),
                             "--measurement-time", str(settings["measurement_seconds"]), "--sample-size", str(settings["sample_size"]),
                             "--confidence-level", str(settings["confidence_level"]), "--noplot"]
                if suite["filter"]:
                    arguments.append(suite["filter"])
                try:
                    result = subprocess.run([executable] + arguments, cwd=source, env=target_env, text=True,
                                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=1800)
                    if result.returncode:
                        bundle["diagnostics"][target["id"]] = {"measurement_log": result.stdout[-150000:], "log_truncated": len(result.stdout) > 150000}
                        raise ValueError("Measurement/assertion failure; no partial target measurements retained")
                    measurements, raw = extract(criterion_directory, run, dict(config, suites=[suite]))
                    if set(raw) & bundle["raw"].keys():
                        raise ValueError("Workload namespace collision between targets")
                    run["measurements"].extend(measurements)
                    bundle["raw"].update(raw)
                    result_record["status"] = "ok"
                    result_record.pop("reason", None)
                    run["status"] = "ok"
                    run.pop("reason", None)
                except (ValueError, subprocess.TimeoutExpired) as error:
                    target.update(status="failed", reason=str(error))
                    result_record.update(status="failed", reason=str(error))
                save(args.output, bundle)
            if not run["measurements"]:
                run.pop("measurements")
                run.update(status="failed", reason="No benchmark target completed; inspect target results and durable diagnostics")
                return
            run["status"] = "ok"
            run.pop("reason", None)
    except AdapterUnavailable as error:
        run.update(status="unsupported", reason=str(error))
    except Exception as error:
        run.update(status="failed", reason=str(error))
    finally:
        save(args.output, bundle)


def publish(args):
    request = load(args.plan)
    if Path(args.bundle).exists():
        bundle = load(args.bundle)
    else:
        bundle = {key: request[key] for key in ("schema_version", "id", "source", "kind", "markers")}
        record = {key: request[key] for key in ("id", "source", "kind", "markers", "run_url")}
        record.update(status="failed", recorded_at=now(), reason="Measurement job did not deliver a result; see workflow log",
                      raw_url=f"{RAW_BASE}/runs/{request['id']}/raw.json")
        bundle.update(record=record, raw={})
    if request["id"] != os.environ["BENCHMARK_REQUEST_ID"] or request["control_sha"] != command(["git", "rev-parse", "HEAD"]):
        raise ValueError("Publisher request is not from this workflow/control revision")
    # Credential is scoped to this fresh hosted job. No benchmark or historical code runs here.
    import base64
    credential = base64.b64encode(f"x-access-token:{os.environ['GH_TOKEN']}".encode()).decode()
    env = dict(os.environ, GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="http.https://github.com/.extraheader",
               GIT_CONFIG_VALUE_0=f"AUTHORIZATION: basic {credential}")
    for attempt in range(8):
        with tempfile.TemporaryDirectory(prefix="ankurah-publish-") as temporary:
            data = Path(temporary)
            command(["git", "init", "--initial-branch=benchmark-data"], cwd=data)
            command(["git", "remote", "add", "origin", f"https://github.com/{REPOSITORY}.git"], cwd=data)
            exists = command(["git", "ls-remote", "--heads", "origin", "benchmark-data"], cwd=data, env=env)
            if exists:
                command(["git", "fetch", "--depth=1", "origin", "benchmark-data"], cwd=data, env=env)
                command(["git", "checkout", "-B", "benchmark-data", "FETCH_HEAD"], cwd=data)
            publish_files(data, bundle, request)
            paths = [name for name in ("history.json", "requests", "runs", "catalogs") if (data / name).exists()]
            command(["git", "add", "--"] + paths, cwd=data)
            if exists and not command(["git", "diff", "--cached", "--name-only"], cwd=data):
                return
            command(["git", "-c", "user.name=Ankurah benchmarks", "-c", "user.email=benchmarks@users.noreply.github.com",
                     "-c", "commit.gpgsign=false", "commit", "-s", "-m", f"Record benchmark request {request['id']}"], cwd=data)
            result = subprocess.run(["git", "push", "origin", "HEAD:refs/heads/benchmark-data"], cwd=data, env=env)
            if result.returncode == 0:
                return
        time.sleep(min(attempt + 1, 5))
    raise RuntimeError("Data branch changed repeatedly or push was denied; retry this workflow (publication is idempotent)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    planner = actions.add_parser("plan")
    planner.add_argument("--ref", required=True)
    planner.add_argument("--kind", choices=("hosted", "stable"), required=True)
    planner.add_argument("--repeat", action="store_true")
    planner.add_argument("--harness-ref")
    planner.add_argument("--catalog", action="store_true")
    planner.add_argument("--output", required=True)
    runner = actions.add_parser("measure")
    runner.add_argument("--plan", required=True)
    runner.add_argument("--output", required=True)
    publisher = actions.add_parser("publish")
    publisher.add_argument("--plan", required=True)
    publisher.add_argument("--bundle", required=True)
    args = parser.parse_args()
    {"plan": plan, "measure": measure, "publish": publish}[args.action](args)
    if args.action == "measure" and load(args.output).get("record", {}).get("status") == "failed":
        raise SystemExit("Measurement failed; result.json retains the reason for publication")


if __name__ == "__main__":
    main()
