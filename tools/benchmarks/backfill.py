#!/usr/bin/env python3
"""Inventory published crate versions and merged PRs; optionally dispatch missing runs."""

import argparse
import datetime
import functools
import io
import json
import os
import subprocess
import tarfile
import urllib.error
import urllib.request
from pathlib import Path

from historical import select_harness
from dataset import targets_complete, REPOSITORY, load, save, union_markers
from pipeline import ROOT, RELEASE_TAG, ancestor, command, get_json, harness_configuration, history, now


# Audited pre-LWW main harness: cloned fixtures and diamond_chain_drain semantics.
LEGACY_HARNESS = "ebdb1727254c675a4d37236ecf23c16c06c4df12"


@functools.lru_cache(maxsize=None)
def workspace_crates():
    import tomllib
    names = []
    for relative in command(["git", "ls-files", "*Cargo.toml"]).splitlines():
        package = tomllib.loads((ROOT / relative).read_text()).get("package", {})
        name = package.get("name", "")
        if name.startswith(("ankurah", "ankql")) and package.get("publish") is not False:
            names.append(name)
    return sorted(set(names))


@functools.lru_cache(maxsize=None)
def published_vcs(crate, version):
    url = f"https://static.crates.io/crates/{crate}/{crate}-{version}.crate"
    request = urllib.request.Request(url, headers={"User-Agent": "ankurah-benchmark-history (https://github.com/ankurah/ankurah)"})
    with urllib.request.urlopen(request, timeout=30) as response:
        archive = response.read(20_000_001)
    if len(archive) > 20_000_000:
        raise ValueError("Published crate archive exceeds 20 MB")
    # Read one bounded metadata member; never extract files or execute crate code.
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        name = f"{crate}-{version}/.cargo_vcs_info.json"
        try:
            member = tar.getmember(name)
        except KeyError:
            return None
        if not member.isfile() or member.size > 10000:
            raise ValueError("Invalid published VCS metadata")
        return json.load(tar.extractfile(member))


def authorize_release(sha, publications=None, vcs_cache=None):
    """Non-main execution is hosted-only and requires corroborated published source."""
    versions = manifest_versions(sha)
    tags = set(command(["git", "tag", "--points-at", sha]).splitlines())
    for crate in workspace_crates():
        version = versions.get(crate)
        if not isinstance(version, str):
            continue
        matching = tags.intersection((f"{crate}-v{version}", f"{crate}@{version}", f"v{version}"))
        if not matching:
            continue
        if publications is None:
            try:
                published = get_json(f"https://crates.io/api/v1/crates/{crate}/{version}")["version"]
            except urllib.error.HTTPError as error:
                if error.code == 404:
                    continue
                raise
        else:
            published = next((item for item in publications.get(crate, []) if item["num"] == version), None)
            if not published:
                continue
        key = f"{crate}@{version}"
        if vcs_cache is None:
            vcs = published_vcs(crate, version)
        else:
            vcs = vcs_cache.get(key)
        if vcs and vcs.get("git", {}).get("sha1") == sha and not vcs["git"].get("dirty", False):
            return {"crate": crate, "version": version, "tag": sorted(matching)[0], "sha": sha,
                    "published_at": published["created_at"], "url": f"https://crates.io/crates/{crate}/{version}",
                    "vcs": vcs}
    return None


@functools.lru_cache(maxsize=None)
def manifest_versions(sha):
    """Check the source itself before associating a crate publication with a tag."""
    import tomllib
    versions = {}
    files = command(["git", "ls-tree", "-r", "--name-only", sha]).splitlines()
    root = tomllib.loads(command(["git", "show", f"{sha}:Cargo.toml"]))
    workspace_version = root.get("workspace", {}).get("package", {}).get("version")
    for path in (path for path in files if path == "Cargo.toml" or path.endswith("/Cargo.toml")):
        package = tomllib.loads(command(["git", "show", f"{sha}:{path}"])).get("package", {})
        version = package.get("version")
        if isinstance(version, dict) and version.get("workspace"):
            version = workspace_version
        if package.get("name"):
            versions[package["name"]] = version
    return versions


def merged_pulls():
    pulls, page = [], 1
    while True:
        batch = get_json(f"https://api.github.com/repos/{REPOSITORY}/pulls?state=closed&base=main&per_page=100&page={page}",
                         os.environ.get("GH_TOKEN"))
        pulls.extend(pull for pull in batch if pull.get("merged_at"))
        if len(batch) < 100:
            return pulls
        page += 1


def published_crates():
    crates = {}
    for name in workspace_crates():
        try:
            crates[name] = get_json(f"https://crates.io/api/v1/crates/{name}/versions")["versions"]
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
    return crates


def discover(kind, pulls=None, crates=None, previous=None, vcs_cache=None):
    pulls = merged_pulls() if pulls is None else pulls
    crates = published_crates() if crates is None else crates
    previous = history() if previous is None else previous
    tags = {}
    for line in command(["git", "show-ref", "--tags", "-d"]).splitlines():
        sha, ref = line.split(" ", 1)
        tag = ref.removeprefix("refs/tags/").removesuffix("^{}")
        if RELEASE_TAG.fullmatch(tag):
            # Peeled annotated entries follow their tag object and overwrite it.
            tags[tag] = sha
    coverage = {}

    def add(sha, marker, label, released_at=None):
        key = sha or label
        if key not in coverage:
            source = {"sha": sha, "repository": REPOSITORY, "label": label}
            entry = {"source": source, "kind": kind, "markers": [], "status": "pending"}
            if sha:
                try:
                    source["committed_at"] = command(["git", "show", "-s", "--format=%cI", sha])
                    if not ancestor(sha):
                        entry.update(status="unsupported", reason="Outside main: published-source verification is required for hosted execution")
                except subprocess.CalledProcessError:
                    entry.update(status="unsupported", reason="Published source commit is unavailable in the repository")
            else:
                entry.update(status="unsupported", reason="Crates.io publication has no unambiguous matching source tag; locate its .cargo_vcs_info.json before measuring")
            coverage[key] = entry
        entry = coverage[key]
        entry["markers"] = union_markers(entry["markers"], [marker])
        if released_at:
            entry.setdefault("publications", []).append({"label": label, "published_at": released_at})

    for tag, sha in tags.items():
        add(sha, {"kind": "release", "label": tag, "url": f"https://github.com/{REPOSITORY}/releases/tag/{tag}"}, tag)
    for crate, versions in crates.items():
        for version in versions:
            number = version["num"]
            candidates = {tags[tag] for tag in (f"{crate}-v{number}", f"{crate}@{number}", f"v{number}")
                          if tag in tags and manifest_versions(tags[tag]).get(crate) == number}
            sha = next(iter(candidates)) if len(candidates) == 1 else None
            label = f"{crate}@{number}"
            add(sha, {"kind": "release", "label": label, "url": f"https://crates.io/crates/{crate}/{number}"},
                label, version.get("created_at"))
    for pull in pulls:
        if pull.get("merged_at") and pull.get("base", {}).get("ref", "main") == "main":
            sha = pull["merge_commit_sha"]
            add(sha, {"kind": "pr", "label": f"#{pull['number']}", "url": pull["html_url"]}, f"PR #{pull['number']}")
    _, _, _, current_hash = harness_configuration()
    historical_hashes = {}
    for entry in coverage.values():
        sha = entry["source"]["sha"]
        if entry.get("reason", "").startswith("Outside main:"):
            authorization = authorize_release(sha, publications=crates, vcs_cache=vcs_cache)
            if authorization:
                entry.update(kind="hosted", requested_kind=kind, status="pending", publication_verification=authorization)
                entry.pop("reason")
        if entry["status"] != "pending":
            continue
        legacy = bool(entry.get("publication_verification")) or subprocess.run(["git", "merge-base", "--is-ancestor", sha, LEGACY_HARNESS], cwd=ROOT).returncode == 0
        entry["harness_ref"] = select_harness(ROOT, sha) if legacy else None
        if entry["harness_ref"] and entry["harness_ref"] not in historical_hashes:
            historical_hashes[entry["harness_ref"]] = harness_configuration(entry["harness_ref"])[3]
        expected_hash = historical_hashes[entry["harness_ref"]] if legacy else current_hash
        entry["harness_sha256"] = expected_hash
        matches = [run for run in previous["runs"] + previous["attempts"] if run["source"]["sha"] == sha
                   and run["kind"] == entry["kind"] and run.get("harness", {}).get("sha256") == expected_hash]
        matches.sort(key=lambda record: record.get("recorded_at", ""))
        successful = [run for run in matches if run["status"] == "ok"]
        if targets_complete(matches):
            entry.update(status="measured", run_ids=[run["id"] for run in successful])
        elif matches:
            last = matches[-1]
            if last["status"] == "unsupported":
                entry.update(status="unsupported", reason=last["reason"])
            else:
                delay = datetime.timedelta(hours=min(24, 2 ** min(len(matches), 5)))
                retry_after = datetime.datetime.fromisoformat(last["recorded_at"]) + delay
                entry.update(status="pending", retry_after=retry_after.isoformat(),
                             last_failure=dict(id=last["id"], recorded_at=last["recorded_at"], reason=last.get("reason", "Some benchmark targets remain incomplete")))
    return {"schema_version": 1, "repository": REPOSITORY, "discovered_at": now(),
            "control_sha": command(["git", "rev-parse", "HEAD"]),
            "coverage": sorted(coverage.values(), key=lambda item: (item["source"].get("committed_at", ""), item["source"]["label"]))}


def dispatch(catalog, output, limit):
    # GitHub dispatch is at-least-once: if interrupted after acceptance, rediscovery
    # may submit again. The measurement's exact-SHA/configuration lookup deduplicates it.
    active_titles = []
    for status in ("queued", "in_progress", "waiting", "pending", "requested"):
        page = 1
        while True:
            result = get_json(f"https://api.github.com/repos/{REPOSITORY}/actions/workflows/benchmark-history.yml/runs?status={status}&per_page=100&page={page}",
                              os.environ.get("GH_TOKEN"))
            active_titles.extend(run["display_title"] for run in result["workflow_runs"])
            if len(result["workflow_runs"]) < 100:
                break
            page += 1
    for entry in catalog["coverage"]:
        if entry["status"] == "pending" and any(title.startswith(f"Benchmark {entry['source']['sha']} (") for title in active_titles):
            entry.update(status="requested", reason="A measurement workflow for this source is already queued/running")
    save(output, catalog)
    ready = [item for item in catalog["coverage"] if item["status"] == "pending" and
             (not item.get("retry_after") or datetime.datetime.fromisoformat(item["retry_after"]) <= datetime.datetime.now(datetime.timezone.utc))]
    for entry in ready[:limit]:
        inputs = {"ref": entry["source"]["sha"], "lane": entry["kind"], "harness_ref": entry.get("harness_ref") or ""}
        body = json.dumps({"ref": "main", "inputs": inputs}).encode()
        request = urllib.request.Request(f"https://api.github.com/repos/{REPOSITORY}/actions/workflows/benchmark-history.yml/dispatches",
                                         data=body, headers={"Authorization": f"Bearer {os.environ['GH_TOKEN']}",
                                                             "Accept": "application/vnd.github+json", "User-Agent": "ankurah-benchmarks"})
        with urllib.request.urlopen(request, timeout=30):
            pass
        entry.update(status="requested", requested_at=now())
        save(output, catalog)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--kind", choices=("hosted", "stable"), default="stable")
    parser.add_argument("--pulls-json", help="Saved GitHub REST pull responses for reproducible/offline discovery")
    parser.add_argument("--crates-json", help="Saved {crate: versions} response for reproducible/offline discovery")
    parser.add_argument("--history-json")
    parser.add_argument("--vcs-json", help="Saved {crate@version: .cargo_vcs_info.json} for offline release authorization")
    parser.add_argument("--resume", action="store_true", help="Resume a saved dispatch plan without rediscovery")
    parser.add_argument("--dispatch", action="store_true", help="Submit pending main-only workflow dispatches")
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()
    catalog = load(args.output) if args.resume else discover(args.kind,
        load(args.pulls_json) if args.pulls_json else None,
        load(args.crates_json) if args.crates_json else None,
        load(args.history_json) if args.history_json else None,
        load(args.vcs_json) if args.vcs_json else None)
    save(args.output, catalog)
    if args.dispatch:
        dispatch(catalog, args.output, args.limit)


if __name__ == "__main__":
    main()
