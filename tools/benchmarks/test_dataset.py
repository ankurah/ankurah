import copy
import argparse
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import backfill
import catalog
import historical
import dataset as data
import pipeline


def fixture():
    config = {"harness_id": "test-v1", "parameters": {"sample_size": 10, "confidence_level": .95},
              "sample_size_overrides": {}, "suites": [{"package": "ankurah-core", "bench": "event_dag", "workloads": ["compare/diamond_chain/4"]}]}
    run = {"id": "42-1", "status": "ok", "recorded_at": "2026-10-13T12:00:00+00:00", "kind": "stable",
           "source": {"sha": "a" * 40, "repository": "ankurah/ankurah", "committed_at": "2026-10-12T12:00:00+00:00"},
           "markers": [{"kind": "main", "label": "aaaaaaaa"}], "run_url": "https://github.com/ankurah/ankurah/actions/runs/42",
           "runner": {"id": "bench-1", "label": "bench-1", "os": "Linux", "hardware": "Example CPU"},
           "compiler": {"version": "rustc test", "target": "x86_64-unknown-linux-gnu"}, "profile": "release",
           "harness": {"id": "test-v1", "sha256": "b" * 64, "source_url": "https://github.com/ankurah/ankurah/tree/main"},
           "parameters": config["parameters"], "lockfile_sha256": "c" * 64, "build_sha256": "d" * 64, "profile_sha256": "e" * 64,
           "raw_url": f"{data.RAW_BASE}/runs/42-1/raw.json"}
    run["comparison_key"] = data.comparison_key(run)
    plan = {key: run[key] for key in ("id", "source", "kind", "markers", "run_url", "harness")}
    plan.update(schema_version=1, config=config)
    return run, plan, config


def criterion_fixture(root, config, median=20):
    folder = Path(root) / "compare/diamond_chain/4/new"
    folder.mkdir(parents=True)
    data.save(folder / "benchmark.json", {"full_id": config["suites"][0]["workloads"][0]})
    data.save(folder / "estimates.json", {"median": {"point_estimate": median, "confidence_interval": {
        "lower_bound": 18, "upper_bound": 24, "confidence_level": .95}}, "slope": {"point_estimate": 9000}})
    data.save(folder / "sample.json", {"iters": [1, 2, 3], "times": [20, 42, 57]})
    return folder


def complete_fixture():
    run, plan, config = fixture()
    with tempfile.TemporaryDirectory() as directory:
        criterion_fixture(directory, config)
        run["measurements"], raw = data.extract(directory, run, config)
    bundle = {key: plan[key] for key in ("schema_version", "id", "source", "kind", "markers")}
    bundle.update(record=run, raw=raw)
    return bundle, plan


class DatasetTests(unittest.TestCase):
    def test_actual_median_not_slope_and_raw_samples_preserved(self):
        bundle, plan = complete_fixture()
        data.validate_bundle(bundle, plan)
        point = bundle["record"]["measurements"][0]
        self.assertEqual((point["median"], point["lower"], point["upper"], point["samples"]), (20, 18, 24, 3))
        self.assertEqual(bundle["raw"][point["workload"]]["sample"]["times"], [20, 42, 57])

    def test_missing_samples_and_nonfinite_data_fail(self):
        run, _, config = fixture()
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "Missing Criterion"):
                data.extract(directory, run, config)
            folder = criterion_fixture(directory, config)
            data.save(folder / "sample.json", {"iters": [0], "times": [20]})
            with self.assertRaisesRegex(ValueError, "Invalid samples"):
                data.extract(directory, run, config)
            (folder / "sample.json").write_text('{"iters":[1], "times":[NaN]}')
            with self.assertRaises(ValueError):
                data.extract(directory, run, config)

    def test_releases_keep_series_but_dedup_checks_source_inputs(self):
        run, _, _ = fixture()
        other = copy.deepcopy(run)
        other.update(lockfile_sha256="f" * 64, build_sha256="0" * 64)
        other["harness"]["source_url"] = "https://github.com/ankurah/ankurah/tree/another-commit"
        self.assertEqual(data.compatibility(run), data.compatibility(other))
        self.assertNotEqual(data.comparison_key(run), data.comparison_key(other))
        for key, value in (("kind", "hosted"), ("profile_sha256", "9" * 64), ("compiler", {"version": "different"})):
            changed = dict(run, **{key: value})
            self.assertNotEqual(data.compatibility(run), data.compatibility(changed))
        other["harness"]["sha256"] = "9" * 64
        self.assertNotEqual(data.compatibility(run), data.compatibility(other))

    def test_audited_workload_contract_bridges_only_api_adapters(self):
        run, _, _ = fixture()
        name = "compare/diamond_chain/4"
        run["harness"]["workload_semantics"] = {name: "cloned-diamond-compare-v1"}
        other = copy.deepcopy(run)
        other["harness"].update(id="other-api", sha256="9" * 64)
        self.assertEqual(data.measurement_series(run, name), data.measurement_series(other, name))
        self.assertNotEqual(data.comparison_key(run), data.comparison_key(other))
        self.assertNotEqual(data.measurement_series(run, "layers/diamond_chain_drain/4"),
                            data.measurement_series(other, "layers/diamond_chain_drain/4"))
        other["harness"]["workload_semantics"][name] = "different-timed-boundary-v2"
        self.assertNotEqual(data.measurement_series(run, name), data.measurement_series(other, name))

    def test_publication_retry_and_repeated_measurement(self):
        bundle, plan = complete_fixture()
        history = data.merge(data.empty_history(), bundle)
        self.assertEqual(data.merge(history, bundle), history)
        repeat = copy.deepcopy(bundle)
        repeat["id"] = repeat["record"]["id"] = "42-2"
        repeat["record"]["raw_url"] = f"{data.RAW_BASE}/runs/42-2/raw.json"
        self.assertEqual(len(data.merge(history, repeat, repeat=True)["runs"]), 2)
        corrupted = copy.deepcopy(bundle)
        corrupted["record"]["measurements"][0]["median"] = 21
        with self.assertRaises(ValueError):
            data.merge(history, corrupted)

    def test_concurrent_same_configuration_projects_once_but_keeps_raw(self):
        bundle, plan = complete_fixture()
        concurrent, concurrent_plan = copy.deepcopy(bundle), copy.deepcopy(plan)
        concurrent["id"] = concurrent["record"]["id"] = concurrent_plan["id"] = "43-1"
        concurrent["record"]["raw_url"] = f"{data.RAW_BASE}/runs/43-1/raw.json"
        concurrent["record"]["recorded_at"] = "2026-10-13T12:01:00+00:00"
        concurrent["markers"] = concurrent["record"]["markers"] = concurrent_plan["markers"] = [{"kind": "release", "label": "v1.0.0"}]
        with tempfile.TemporaryDirectory() as directory:
            data.publish_files(directory, bundle, plan)
            data.publish_files(directory, concurrent, concurrent_plan)
            data.publish_files(directory, concurrent, concurrent_plan)
            history = data.load(Path(directory) / "history.json")
            self.assertEqual(len(history["runs"]), 1)
            self.assertEqual(history["duplicates"][0]["reuses"], "42-1")
            self.assertEqual(len(history["runs"][0]["markers"]), 2)
            self.assertTrue((Path(directory) / "runs/43-1/raw.json").is_file())
            receipt = data.load(Path(directory) / "requests/43-1.json")
            self.assertEqual(receipt["run_id"], "43-1")
            self.assertNotIn("record", receipt)
            self.assertNotIn("raw", receipt)

    def test_new_tag_reuses_exact_run_and_preserves_immutable_raw(self):
        bundle, plan = complete_fixture()
        with tempfile.TemporaryDirectory() as directory:
            data.publish_files(directory, bundle, plan)
            original = (Path(directory) / "runs/42-1/run.json").read_bytes()
            tag_plan = copy.deepcopy(plan)
            tag_plan.update(id="43-1", markers=[{"kind": "release", "label": "v1.0.0"}])
            reuse = {key: tag_plan[key] for key in ("schema_version", "id", "source", "kind", "markers")}
            reuse.update(reuse_id="42-1", comparison_key=bundle["record"]["comparison_key"])
            data.publish_files(directory, reuse, tag_plan)
            data.publish_files(directory, reuse, tag_plan)
            history = data.load(Path(directory) / "history.json")
            self.assertEqual(len(history["runs"]), 1)
            self.assertEqual(len(history["runs"][0]["markers"]), 2)
            self.assertEqual((Path(directory) / "runs/42-1/run.json").read_bytes(), original)
            reuse["comparison_key"] = "0" * 64
            with self.assertRaises(ValueError):
                data.publish_files(directory, reuse, tag_plan)

    def test_security_rejects_paths_source_changes_and_raw_disagreement(self):
        bundle, plan = complete_fixture()
        for mutate in (lambda b: b.update(id="../escape"), lambda b: b["source"].update(sha="f" * 40),
                       lambda b: b["record"]["measurements"][0].update(median=21)):
            corrupt = copy.deepcopy(bundle)
            mutate(corrupt)
            with self.assertRaises(ValueError):
                data.validate_bundle(corrupt, plan)
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as outside:
            (Path(directory) / "runs").symlink_to(outside)
            with self.assertRaisesRegex(ValueError, "symlink"):
                data.publish_files(directory, bundle, plan)
            self.assertFalse(list(Path(outside).iterdir()))

    def test_unsupported_is_durable_and_coverage_contains_gaps(self):
        bundle, plan = complete_fixture()
        bundle["record"].update(status="unsupported", reason="Historical API requires adapter")
        bundle["record"].pop("measurements")
        plan["coverage"] = [{"source": plan["source"], "kind": "stable", "markers": [], "status": "pending"},
                            {"source": {"sha": None, "label": "crate@0.1.0"}, "kind": "stable", "markers": [],
                             "status": "unsupported", "reason": "No source tag"}]
        with tempfile.TemporaryDirectory() as directory:
            data.publish_files(directory, bundle, plan)
            history = data.load(Path(directory) / "history.json")
            self.assertEqual(history["runs"], [])
            self.assertEqual(history["attempts"][0]["status"], "unsupported")
            self.assertEqual(len(history["coverage"]), 2)
            self.assertTrue(all(item["status"] == "unsupported" for item in history["coverage"]))

    def test_historical_surface_adapter_preserves_algorithm_and_lock_versions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            contents = {
                "core/Cargo.toml": '[package]\nname="ankurah-core"\nversion="0.9.3"\n[features]\n[dev-dependencies]\n',
                "core/src/lib.rs": "pub(crate) mod event_dag;\n",
                "core/src/event_dag/relation.rs": "pub(crate) enum AbstractCausalRelation {}\n",
                "core/src/event_dag/layers.rs": "pub(crate) struct EventLayers {}\n",
                "core/src/event_dag/comparison.rs": "// original comparison algorithm\n",
                "Cargo.lock": 'version=4\n[[package]]\nname="original"\nversion="1.0.0"\n',
            }
            for path, text in contents.items():
                (root / path).parent.mkdir(parents=True, exist_ok=True)
                (root / path).write_text(text)
            support = b'use crate::event_dag::accumulator::{ComparisonResult, EventLayers};\npub use crate::event_dag::DEFAULT_BUDGET;'
            adapter = historical.apply_surface(root, support)
            self.assertIn('bench-internals = []', (root / "core/Cargo.toml").read_text())
            self.assertIn('layers::EventLayers', (root / "core/src/bench_support.rs").read_text())
            self.assertEqual((root / "core/src/event_dag/comparison.rs").read_text(), contents["core/src/event_dag/comparison.rs"])
            self.assertEqual((root / "Cargo.lock").read_text(), contents["Cargo.lock"])
            original = contents["Cargo.lock"].encode()
            historical.assert_lock_preserved(original, original + b'[[package]]\nname="criterion"\nversion="0.5.1"\n')
            with self.assertRaisesRegex(ValueError, "original locked"):
                historical.assert_lock_preserved(original, original.replace(b'1.0.0', b'2.0.0'))
            self.assertEqual(adapter["version"], historical.VERSION)

    def test_target_retry_adds_only_missing_workloads(self):
        first, _ = complete_fixture()
        first["record"]["target_results"] = [{"id": "a", "status": "ok"}, {"id": "b", "status": "failed"}]
        history = data.merge(data.empty_history(), first)
        self.assertFalse(data.targets_complete(history["runs"]))
        second = copy.deepcopy(first)
        second["id"] = second["record"]["id"] = "44-1"
        second["record"]["target_results"] = [{"id": "a", "status": "reused"}, {"id": "b", "status": "ok"}]
        point = copy.deepcopy(second["record"]["measurements"][0])
        point.update(workload="new/target/workload", series="another-series")
        second["record"]["measurements"].append(point)
        history = data.merge(history, second)
        self.assertEqual([len(record["measurements"]) for record in history["runs"]], [1, 1])
        self.assertTrue(data.targets_complete(history["runs"]))
        self.assertEqual(data.merge(history, second), history)

    def test_target_registration_and_criterion_names_need_no_manual_list(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "Cargo.toml"
            manifest.write_text('[package]\nname="new-package"\nversion="0.1.0"\n[[bench]]\nname="fresh"\nharness=false\n[[bench]]\nname="custom"\nharness=true\n')
            metadata = {"workspace_members": ["p"], "packages": [{"id": "p", "name": "new-package", "manifest_path": str(manifest),
                "dependencies": [{"name": "criterion"}], "targets": [
                    {"kind": ["bench"], "name": "fresh", "src_path": str(root / "benches/fresh.rs"), "required-features": ["bench-api"]},
                    {"kind": ["bench"], "name": "custom", "src_path": str(root / "benches/custom.rs")},
                ]}]}
            suites, targets = catalog.registered(metadata, root, [])
            self.assertEqual(len(suites), 1)
            self.assertEqual(suites[0]["features"], ["bench-api"])
            self.assertEqual({t["id"]: t["status"] for t in targets}, {"new-package/custom": "unsupported", "new-package/fresh": "registered"})
            names = catalog.criterion_workloads("new/group/9: benchmark\n", suites[0])
            self.assertEqual(names, {"new-package/fresh/new/group/9": "new/group/9"})
            other = dict(suites[0], id="second/fresh")
            self.assertNotEqual(set(names), set(catalog.criterion_workloads("new/group/9: benchmark\n", other)))
            with self.assertRaises(ValueError):
                catalog.criterion_workloads("custom harness with no Criterion listing", suites[0])

    def test_listing_timeout_remains_retryable(self):
        with patch.object(catalog.subprocess, "run", side_effect=subprocess.TimeoutExpired("benchmark --list", 60)):
            result = catalog.enumerate_criterion("benchmark", {"id": "new/target"}, ".", {})
        self.assertEqual(result["status"], "failed")
        self.assertIn("timed out", result["reason"])

    def test_partial_targets_publish_complete_samples_and_keep_empty_catalog_entries(self):
        bundle, plan = complete_fixture()
        suite = plan["config"]["suites"][0]
        suite.update(id="ankurah-core/event_dag", legacy_workloads=suite["workloads"])
        plan["config"]["auto_discover"] = True
        bundle["record"]["target_results"] = [{"id": suite["id"], "status": "ok", "original_workloads": suite["workloads"]}]
        plan["config"]["suites"].append({"id": "new/special", "workloads": [], "legacy_workloads": []})
        bundle["record"]["target_results"].append({"id": "new/special", "status": "failed", "reason": "platform service unavailable"})
        plan["benchmarks"] = [{"id": suite["id"], "package": "ankurah-core", "target": "event_dag", "status": "registered", "workloads": []},
                              {"id": "new/special", "package": "new", "target": "special", "status": "registered", "workloads": []}]
        bundle["benchmarks"] = [dict(plan["benchmarks"][0], status="discovered", workloads=[{"id": suite["workloads"][0], "unit": "ns"}]),
                                dict(plan["benchmarks"][1], status="failed", reason="platform service unavailable")]
        with tempfile.TemporaryDirectory() as directory:
            data.publish_files(directory, bundle, plan)
            history = data.load(Path(directory) / "history.json")
            self.assertEqual(len(history["runs"][0]["measurements"]), 1)
            self.assertEqual(len(history["benchmarks"]), 2)
            self.assertEqual(history["benchmarks"][1]["status"], "failed")
            self.assertEqual(history["benchmarks"][1]["workloads"], [])
            self.assertTrue((Path(directory) / "catalogs/42-1-benchmarks.json").is_file())

    def test_historical_harness_retains_drain_boundary(self):
        config, revision, files, _ = pipeline.harness_configuration(backfill.LEGACY_HARNESS)
        self.assertEqual(revision, backfill.LEGACY_HARNESS)
        self.assertEqual(len(config["suites"]), 1)
        self.assertIn("layers/diamond_chain_drain/4", config["suites"][0]["workloads"])
        self.assertNotIn("layers/diamond_chain_prepare/4", config["suites"][0]["workloads"])
        self.assertIn(b'diamond_chain_drain', files["core/benches/event_dag.rs"])

    def test_historical_ref_injection_is_not_a_git_option(self):
        for ref in ("--upload-pack=evil", "refs/pull/123/head", "main;echo owned", "$(env)", "main\nfoo"):
            with self.assertRaises(ValueError):
                pipeline.source_sha(ref)

    def test_crate_publications_and_pr_markers_share_sha(self):
        sha = backfill.LEGACY_HARNESS
        def git(args, **_):
            if args[1] == "show-ref":
                return f"{sha} refs/tags/ankurah-v0.1.0"
            if args[1] == "show":
                return "2026-10-01T00:00:00Z"
            return sha
        pulls = [{"merged_at": "2026-10-01", "merge_commit_sha": sha, "number": 1, "html_url": "https://github.com/ankurah/ankurah/pull/1"}]
        crates = {"ankurah": [{"num": "0.1.0", "created_at": "2026-10-01"}, {"num": "0.0.9", "created_at": "2026-09-01"}]}
        with patch.object(backfill, "command", side_effect=git), patch.object(backfill, "ancestor", return_value=True), \
                patch.object(backfill, "manifest_versions", return_value={"ankurah": "0.1.0"}):
            catalog = backfill.discover("stable", pulls=pulls, crates=crates, previous=data.empty_history())
        self.assertEqual(len(catalog["coverage"]), 2)
        mapped = next(entry for entry in catalog["coverage"] if entry["source"]["sha"])
        self.assertEqual({marker["kind"] for marker in mapped["markers"]}, {"release", "pr"})
        self.assertEqual(len(mapped["markers"]), 3)
        self.assertEqual(next(entry for entry in catalog["coverage"] if not entry["source"]["sha"])["status"], "unsupported")

    def test_umbrella_tag_with_wrong_crate_version_is_unmapped(self):
        sha = backfill.LEGACY_HARNESS
        def git(args, **_):
            return f"{sha} refs/tags/v0.1.0" if args[1] == "show-ref" else sha
        with patch.object(backfill, "command", side_effect=git), patch.object(backfill, "ancestor", return_value=True), \
                patch.object(backfill, "manifest_versions", return_value={"ankurah": "0.2.0"}):
            catalog = backfill.discover("stable", pulls=[], crates={"ankurah": [{"num": "0.1.0"}]}, previous=data.empty_history())
        unmapped = next(entry for entry in catalog["coverage"] if entry["source"]["sha"] is None)
        self.assertEqual(unmapped["source"]["label"], "ankurah@0.1.0")

    def test_published_release_authorization_requires_clean_matching_vcs(self):
        sha = "a" * 40
        publications = {"ankurah": [{"num": "0.9.3", "created_at": "2026-09-01"}]}
        with patch.object(backfill, "workspace_crates", return_value=["ankurah"]), \
                patch.object(backfill, "manifest_versions", return_value={"ankurah": "0.9.3"}), \
                patch.object(backfill, "command", return_value="ankurah-v0.9.3"):
            for vcs in ({"git": {"sha1": "b" * 40}}, {"git": {"sha1": sha, "dirty": True}}):
                self.assertIsNone(backfill.authorize_release(sha, publications, {"ankurah@0.9.3": vcs}))
            proof = backfill.authorize_release(sha, publications, {"ankurah@0.9.3": {"git": {"sha1": sha}}})
            self.assertEqual(proof["sha"], sha)
            self.assertEqual(proof["tag"], "ankurah-v0.9.3")

    def test_nonmain_official_release_is_forced_onto_hosted_runner(self):
        _, _, config = fixture()
        sha = "a" * 40
        proof = {"sha": sha, "crate": "ankurah", "version": "0.9.3"}
        config["toolchain"] = "nightly-test"
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(pipeline, "harness_configuration", return_value=(config, backfill.LEGACY_HARNESS, {}, "b" * 64)), \
                patch.object(pipeline, "source_sha", return_value=sha), patch.object(pipeline, "ancestor", return_value=False), \
                patch.object(backfill, "authorize_release", return_value=proof), \
                patch.object(pipeline, "get_json", return_value=[]), patch.object(pipeline, "command", return_value=sha), \
                patch.dict(pipeline.os.environ, {"BENCHMARK_REQUEST_ID": "42-1", "BENCHMARK_RUN_URL": "https://example.test/run", "GITHUB_OUTPUT": directory + "/output"}):
            args = argparse.Namespace(harness_ref=backfill.LEGACY_HARNESS, ref=sha, kind="stable", repeat=False,
                                      catalog=False, output=directory + "/request.json")
            pipeline.plan(args)
            request = data.load(args.output)
            self.assertEqual(request["kind"], "hosted")
            self.assertEqual(request["source"]["publication_verification"], proof)
            self.assertIn("lane=hosted", Path(directory + "/output").read_text())

    def test_only_primary_benchmark_api_errors_are_unsupported(self):
        message = {"reason": "compiler-message", "message": {"level": "error", "code": {"code": "E0432"},
                   "spans": [{"is_primary": True, "file_name": "core/benches/event_dag.rs"}]}}
        self.assertTrue(pipeline.incompatible_harness(json.dumps(message), "core/benches/event_dag.rs"))
        message["message"]["spans"][0]["file_name"] = "core/src/node.rs"
        self.assertFalse(pipeline.incompatible_harness(json.dumps(message), "core/benches/event_dag.rs"))
        self.assertFalse(pipeline.incompatible_harness("error: dependency download timed out", "core/benches/event_dag.rs"))

    def test_missing_declared_harness_feature_is_not_a_transient_download_failure(self):
        diagnostic = "error: the package 'ankurah-core' does not contain this feature: bench-internals\n"
        self.assertTrue(pipeline.incompatible_harness(diagnostic, "core/benches/event_dag.rs", ["bench-internals"]))
        self.assertFalse(pipeline.incompatible_harness(diagnostic, "tests/benches/lane_perf.rs", []))
        self.assertFalse(pipeline.incompatible_harness("error: failed to download bench-internals", "core/benches/event_dag.rs", ["bench-internals"]))

    def test_transient_failed_attempt_is_pending_with_backoff(self):
        sha = backfill.LEGACY_HARNESS
        _, _, _, harness_hash = pipeline.harness_configuration(sha)
        previous = {"runs": [], "attempts": [{"id": "42-1", "source": {"sha": sha}, "kind": "stable",
                    "harness": {"sha256": harness_hash}, "status": "failed", "reason": "Download timeout",
                    "recorded_at": "2026-10-13T12:00:00+00:00"}]}
        def git(args, **_):
            return f"{sha} refs/tags/ankurah-v0.1.0" if args[1] == "show-ref" else sha
        with patch.object(backfill, "command", side_effect=git), patch.object(backfill, "ancestor", return_value=True):
            catalog = backfill.discover("stable", pulls=[], crates={}, previous=previous)
        self.assertEqual(catalog["coverage"][0]["status"], "pending")
        self.assertEqual(catalog["coverage"][0]["retry_after"], "2026-10-13T14:00:00+00:00")


if __name__ == "__main__":
    unittest.main()
