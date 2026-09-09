"""Trusted Linux/Docker supervisor. This process alone owns host capabilities."""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import re
import secrets
import shutil
import stat
import subprocess
import tarfile
import tempfile
import time
import urllib.request
from pathlib import Path

if __package__:
    from .contracts import (DEEP_SECONDS, INITIAL_SECONDS, Deferred, InvalidResult, QuotaUnavailable,
                            REPOSITORY, diagnosis, identifier, safe_patch_path, sha, text)
    from .gateway import RunGateway
else:
    from contracts import (DEEP_SECONDS, INITIAL_SECONDS, Deferred, InvalidResult, QuotaUnavailable,
                           REPOSITORY, diagnosis, identifier, safe_patch_path, sha, text)
    from gateway import RunGateway

GIB = 1024 ** 3
PATCH_LIMIT = 2 * 1024 * 1024
ENV = {"PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
       "LANG": "C.UTF-8", "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null",
       "GIT_TERMINAL_PROMPT": "0"}


def command(args, *, cwd=None, input=None, timeout=120, check=True):
    result = subprocess.run(args, cwd=cwd, input=input, capture_output=True,
                            env=ENV, timeout=timeout, check=False)
    if check and result.returncode:
        raise Deferred("trusted host command failed: " + Path(args[0]).name)
    return result


def secret_file(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor) as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
            raise Deferred("credential file must be regular and private")
        value = stream.read(4097).strip()
    if not 20 <= len(value) <= 4096 or any(c.isspace() for c in value):
        raise Deferred("credential file is invalid")
    return value


def memory_available():
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    except OSError:
        pass
    raise Deferred("Linux memory accounting is unavailable")


def validate_patch(patch: bytes) -> list[str]:
    if not patch or len(patch) > PATCH_LIMIT:
        raise InvalidResult("patch is empty or exceeds size limit")
    try:
        decoded = patch.decode("utf-8")
    except UnicodeDecodeError as error:
        raise InvalidResult("binary patch is not permitted") from error
    text(decoded, PATCH_LIMIT)
    if re.search(r"^[+-](?![+-]).*\bmaintenance\b", decoded, re.M | re.I):
        raise InvalidResult("patch changes maintenance wiring in a shared source file")
    if re.search(r"^(?:GIT binary patch|Binary files |(?:new|old|deleted) (?:file )?mode (?!100644$|100755$))", decoded, re.M):
        raise InvalidResult("binary or special-file patch requires human review")
    paths = []
    for line in decoded.splitlines():
        if line.startswith("diff --git "):
            match = re.fullmatch(r"diff --git a/([^\s]+) b/([^\s]+)", line)
            if not match or match[1] != match[2] or not safe_patch_path(match[1]):
                raise InvalidResult("patch changes a protected or ambiguous path")
            paths.append(match[1])
        elif line.startswith(("--- ", "+++ ")):
            value = line[4:]
            if value != "/dev/null" and (not value.startswith(("a/", "b/")) or not safe_patch_path(value[2:])):
                raise InvalidResult("patch header escapes allowed paths")
        elif line.startswith(("rename from ", "rename to ", "copy from ", "copy to ")):
            raise InvalidResult("rename/copy patch requires explicit file changes")
    if not paths or len(paths) > 100 or len(paths) != len(set(paths)):
        raise InvalidResult("patch file list is invalid")
    return paths


def validate_output(value, stage):
    if not isinstance(value, dict) or set(value) != {"diagnosis", "outcome", "feedback"}:
        raise InvalidResult("unexpected worker result fields")
    diagnosis(value["diagnosis"])
    if value["outcome"] not in ("patch", "no_fix", "needs_human"):
        raise InvalidResult("invalid worker outcome")
    if value["outcome"] == "patch" and (stage == "initial" or value["diagnosis"]["next_action"] != "fix"):
        raise InvalidResult("patch was not justified by a deep diagnosis")
    if not isinstance(value["feedback"], list) or len(value["feedback"]) > 100:
        raise InvalidResult("invalid feedback responses")
    for response in value["feedback"]:
        if not isinstance(response, dict) or set(response) != {"kind", "id", "action", "body"}:
            raise InvalidResult("invalid feedback response")
        if response["kind"] not in ("comment", "thread") or response["action"] not in ("fixed", "rebuttal"):
            raise InvalidResult("invalid feedback resolution")
        identifier(str(response["id"]))
        text(response["body"], 12000)
    return value


def verification_receipt(data, exit_code):
    checks = json.loads(data)
    names = ("fmt", "clippy", "tests")
    if not isinstance(checks, list) or not 1 <= len(checks) <= len(names):
        raise InvalidResult("invalid verification receipt")
    for index, item in enumerate(checks):
        if (not isinstance(item, dict) or set(item) != {"name", "passed"}
                or item["name"] != names[index] or type(item["passed"]) is not bool
                or (index < len(checks) - 1 and not item["passed"])):
            raise InvalidResult("invalid verification check")
    passed = len(checks) == 3 and all(item["passed"] for item in checks)
    if (exit_code == 0) != passed or (not passed and checks[-1]["passed"]):
        raise InvalidResult("verification process and receipt disagree")
    return checks


class Runner:
    def __init__(self, config, api, cancelled=lambda job: False):
        self.config = config
        self.api = api
        self.cancelled = cancelled
        self.state = Path(config["state_dir"]).resolve()
        self.source = Path(config["source_dir"]).resolve()
        self.image = config["image"]
        if not re.fullmatch(r"(?:[a-zA-Z0-9./:_-]+@)?sha256:[a-f0-9]{64}", self.image):
            raise Deferred("worker image must be pinned by digest or immutable image ID")
        self.state.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.instance = hashlib.sha256(str(self.state).encode()).hexdigest()[:12]

    def refresh_source(self):
        if not self.source.exists():
            command(["git", "clone", "--mirror", "https://github.com/" + REPOSITORY + ".git", str(self.source)])
        remote = command(["git", "-C", str(self.source), "remote", "get-url", "origin"]).stdout.decode().strip()
        if remote != "https://github.com/" + REPOSITORY + ".git":
            raise Deferred("source mirror is not the configured repository")
        command(["git", "-C", str(self.source), "fetch", "--prune", "origin",
                 "+refs/heads/*:refs/heads/*"], timeout=300)
        return sha(command(["git", "-C", str(self.source), "rev-parse", "refs/heads/main"]).stdout.decode().strip())

    def contains(self, ancestor, descendant):
        try:
            sha(ancestor)
            sha(descendant)
        except InvalidResult:
            return None
        for revision in (ancestor, descendant):
            if command(["git", "-C", str(self.source), "cat-file", "-e", revision + "^{commit}"], check=False).returncode:
                return None
        result = command(["git", "-C", str(self.source), "merge-base", "--is-ancestor", ancestor, descendant], check=False)
        return True if result.returncode == 0 else False if result.returncode == 1 else None

    def host_snapshot(self):
        snapshot = {"available": True, "memory_available_bytes": memory_available(),
                    "disk_available_bytes": shutil.disk_usage(self.state).free,
                    "cpu_count": os.cpu_count(), "load": list(os.getloadavg()),
                    "runtime": {"available": False}}
        name = identifier(self.config["production_container"])
        result = command(["docker", "inspect", name], check=False)
        if result.returncode == 0:
            item = json.loads(result.stdout)[0]
            build_revision = (item.get("Config", {}).get("Labels") or {}).get("org.opencontainers.image.revision")
            if not re.fullmatch(r"[a-f0-9]{40}", build_revision or ""):
                build_revision = None
            deployed = re.fullmatch(r"ghcr\.io/iamwavecut/openplotva:([a-f0-9]{40})",
                                    item.get("Config", {}).get("Image", ""))
            # Deployment promotes an identical-tree PR image under the merge SHA;
            # its immutable OCI label still records the earlier PR build revision.
            revision = deployed.group(1) if deployed else build_revision
            snapshot["revision"] = revision
            snapshot["runtime"] = {"available": True, "image": item["Image"], "revision": revision,
                "build_revision": build_revision,
                "restart_count": item["RestartCount"], "oom_killed": item["State"]["OOMKilled"],
                "running": item["State"]["Running"]}
            stats = command(["docker", "stats", "--no-stream", "--format", "{{json .}}", name], check=False, timeout=20)
            if stats.returncode == 0:
                measured = json.loads(stats.stdout)
                snapshot["runtime"]["cpu_percent"] = measured.get("CPUPerc")
                snapshot["runtime"]["memory"] = measured.get("MemUsage")
        return snapshot

    def preflight(self):
        if os.name != "posix" or not Path("/proc/meminfo").exists() or os.geteuid() != 0:
            raise Deferred("worker supervisor requires the dedicated Linux host service")
        if memory_available() < 6 * GIB:
            raise Deferred("less than 6 GiB available RAM")
        if shutil.disk_usage(self.state).free < 16 * GIB:
            raise Deferred("workspace allocation would breach the 8 GiB host disk reserve")
        for executable in ("docker", "git", "iptables", "fallocate", "mkfs.ext4", "mount", "umount", "findmnt"):
            if not shutil.which(executable, path=ENV["PATH"]):
                raise Deferred("missing host dependency: " + executable)
        image = json.loads(command(["docker", "image", "inspect", self.image]).stdout)[0]
        labels = image.get("Config", {}).get("Labels") or {}
        if labels.get("openplotva.maintenance.omp") != "18.1.14" or labels.get("openplotva.maintenance.rust") != "1.95.0":
            raise Deferred("worker image provenance/version labels do not match")
        token = secret_file(self.config["gateway_token_file"])
        if self.config["gateway_url"] != "http://127.0.0.1:4000":
            raise Deferred("native gateway must use the dedicated loopback endpoint")
        try:
            request = urllib.request.Request(self.config["gateway_url"] + "/v1/models",
                                             headers={"Authorization": "Bearer " + token})
            with urllib.request.urlopen(request, timeout=10) as response:
                models = json.loads(response.read(2 * 1024 * 1024)).get("data", [])
            if not any(model.get("id") == "zai/glm-5.3" and model.get("owned_by") == "zai" for model in models):
                raise Deferred("GLM 5.3 Coding Plan profile is unavailable")
        except (OSError, ValueError) as error:
            raise Deferred("native GLM gateway is unavailable") from error

    @contextlib.contextmanager
    def slot(self):
        with (self.state / "agent.lock").open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise Deferred("another agent holds the compute slot") from error
            yield

    def container_args(self, name, work, network, gateway=None):
        args = ["docker", "create", "--name", name, "--label", "openplotva.maintenance=" + self.instance,
                "--user", "1000:1000", "--read-only", "--cap-drop", "ALL",
                "--security-opt", "no-new-privileges=true", "--cpus", "2", "--memory", "4g",
                "--memory-swap", "4g", "--pids-limit", "256", "--ulimit", "nofile=1024:1024",
                "--network", network, "--dns", "127.0.0.1", "--ipc", "private",
                "--mount", "type=bind,src=" + str(work) + ",dst=/work",
                "--tmpfs", "/tmp:rw,nosuid,nodev,size=64m,uid=1000,gid=1000,mode=1777",
                "--env", "PI_CONFIG_DIR=.omp", "--env", "PI_CODING_AGENT_DIR=/work/omp/agent",
                "--env", "CARGO_HOME=/work/cargo", "--env", "CARGO_TARGET_DIR=/work/target",
                "--env", "CARGO_PROFILE_DEV_DEBUG=0", "--env", "CARGO_PROFILE_TEST_DEBUG=0",
                "--env", "CARGO_INCREMENTAL=0", "--env", "CARGO_BUILD_JOBS=2",
                "--env", "CARGO_NET_OFFLINE=true", "--env", "TMPDIR=/work/tmp",
                "--env", "GIT_CONFIG_NOSYSTEM=1", "--env", "GIT_CONFIG_GLOBAL=/dev/null"]
        if gateway:
            # Only a revocable capability, never a real provider/host credential.
            args += ["--env", "MAINTENANCE_GATEWAY=http://" + gateway.address[0] + ":" + str(gateway.address[1]),
                     "--env", "MAINTENANCE_RUN_TOKEN=" + gateway.token,
                     "--env", "MAINTENANCE_INCIDENT_ID=" + str(gateway.incident_id)]
        return args + [self.image, "/bin/sleep", str(DEEP_SECONDS + 300)]

    def _checkout(self, work, base):
        repository = work / "repo"
        command(["git", "clone", "--no-hardlinks", "--no-checkout", str(self.source), str(repository)])
        command(["git", "-C", str(repository), "-c", "core.hooksPath=/dev/null", "checkout", "--detach", base])
        command(["git", "-C", str(repository), "remote", "remove", "origin"])
        for path in (work, repository, work / "tmp"):
            path.mkdir(exist_ok=True)
        command(["chown", "-hR", "1000:1000", str(work)])

    def _exec(self, name, arguments, job_id, deadline, *, check=True, quota=None):
        # Output is only trusted driver status/check receipts. Raw model logs stay
        # inside the fixed-size workspace and are never streamed to service logs.
        with tempfile.TemporaryFile(dir=self.state) as output:
            process = subprocess.Popen(["docker", "exec", name, "python3", "/opt/maintenance/worker.py", *arguments],
                                       stdout=output, stderr=subprocess.DEVNULL, env=ENV)
            while process.poll() is None:
                if quota and quota.quota_unavailable.is_set():
                    command(["docker", "stop", "--time", "1", name], check=False, timeout=15)
                    process.wait(timeout=15)
                    raise QuotaUnavailable(usage=quota.usage, retry_after_seconds=quota.retry_after_seconds)
                if (self.cancelled(job_id) or time.monotonic() >= deadline
                        or shutil.disk_usage(self.state).free < 8 * GIB
                        or memory_available() < 2 * GIB):
                    command(["docker", "stop", "--time", "5", name], check=False, timeout=15)
                    process.wait(timeout=15)
                    raise Deferred("job stopped; cancellation, time or host resource boundary reached")
                if output.tell() > PATCH_LIMIT:
                    command(["docker", "stop", "--time", "1", name], check=False)
                    process.wait(timeout=15)
                    raise InvalidResult("worker output exceeds limit")
                time.sleep(2)
            output.seek(0)
            data = output.read(PATCH_LIMIT + 1)
            if len(data) > PATCH_LIMIT:
                raise InvalidResult("worker output exceeds limit")
            if quota and quota.quota_unavailable.is_set():
                raise QuotaUnavailable(usage=quota.usage, retry_after_seconds=quota.retry_after_seconds)
            if check and process.returncode:
                raise Deferred("isolated worker did not produce a verified result")
            return process.returncode, data

    def _read_worker_file(self, work, filename, limit):
        descriptor = os.open(work / filename, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise InvalidResult("worker result is not a regular file")
            value = stream.read(limit + 1)
        if len(value) > limit:
            raise InvalidResult("worker result is too large")
        return value

    def _network(self, tag):
        name = "opm-" + tag
        bridge = "opm" + tag[:10]
        subnet = self.config.get("worker_subnet", "172.31.253.0/28")
        command(["docker", "network", "create", "--internal", "--subnet", subnet,
                 "--label", "openplotva.maintenance=" + self.instance,
                 "--opt", "com.docker.network.bridge.name=" + bridge, name])
        data = json.loads(command(["docker", "network", "inspect", name]).stdout)[0]
        return name, bridge, data["IPAM"]["Config"][0]["Gateway"]

    @contextlib.contextmanager
    def _firewall(self, bridge, port):
        rules = [("INPUT", ["-i", bridge, "-j", "DROP"]),
                 ("INPUT", ["-i", bridge, "-p", "tcp", "--dport", str(port), "-j", "ACCEPT"]),
                 ("FORWARD", ["-i", bridge, "-j", "DROP"])]
        installed = []
        try:
            for chain, rule in rules:
                command(["iptables", "-w", "-I", chain, "1", *rule])
                installed.append((chain, rule))
            yield
        finally:
            for chain, rule in reversed(installed):
                command(["iptables", "-w", "-D", chain, *rule], check=False)

    def run(self, job, context):
        job_id = identifier(job["id"])
        base = sha(job["base_sha"])
        stage = job["stage"]
        if stage not in ("initial", "deep", "review") or type(job["incident_id"]) is not int or job["incident_id"] <= 0:
            raise InvalidResult("invalid job scope")
        seconds = min(int(job["remaining_seconds"]), INITIAL_SECONDS if stage == "initial" else DEEP_SECONDS)
        if seconds <= 0 or self.cancelled(job_id):
            raise Deferred("job has no remaining active budget")
        with self.slot():
            self.preflight()
            self.refresh_source()
            tag = secrets.token_hex(6)
            directory = self.state / "workspaces" / tag
            directory.mkdir(parents=True, mode=0o700)
            work = directory / "work"
            work.mkdir()
            volume = directory / "workspace.ext4"
            artifacts = self.state / "artifacts" / job_id / tag
            artifacts.mkdir(parents=True, mode=0o700)
            name = "opm-" + tag
            network = None
            mounted = False
            saved_patch = False
            gateway = None
            failure = None
            started = time.monotonic()
            deadline = started + seconds
            manifest = {"job_id": job_id, "base_sha": base, "container": name,
                        "network": name, "work": str(work), "artifact_dir": str(artifacts), "tag": tag}
            (directory / "manifest.json").write_text(json.dumps(manifest))
            (artifacts / "attempt.json").write_text(json.dumps({"base_sha": base, "stage": stage, "started_at": time.time()}))
            try:
                command(["fallocate", "-l", str(8 * GIB), str(volume)])
                command(["mkfs.ext4", "-q", "-F", "-m", "0", str(volume)])
                command(["mount", "-o", "loop,nodev,nosuid", str(volume), str(work)])
                mounted = True
                self._checkout(work, base)
                if stage != "initial":
                    previous = sorted((self.state / "artifacts" / job_id).glob("*/attempt.json"),
                                      key=lambda path: path.stat().st_mtime, reverse=True)
                    for attempt in previous:
                        if attempt.parent == artifacts or json.loads(attempt.read_text()).get("base_sha") != base:
                            continue
                        for filename in ("patch.diff", "partial.patch"):
                            candidate = attempt.parent / filename
                            if candidate.is_file() and 0 < candidate.stat().st_size <= PATCH_LIMIT:
                                patch = candidate.read_bytes()
                                validate_patch(patch)
                                command(["git", "-c", "safe.directory=" + str(work / "repo"), "-C", str(work / "repo"),
                                         "apply", "--index", "--whitespace=error", "-"], input=patch)
                                break
                        break
                deployed = context.get("deployed", {}).get("revision")
                if deployed and self.contains(deployed, deployed):
                    archive = command(["git", "-C", str(self.source), "archive", sha(deployed)]).stdout
                    archive_path = directory / "deployed.tar"
                    archive_path.write_bytes(archive)
                    (work / "deployed").mkdir()
                    with tarfile.open(archive_path) as source:
                        source.extractall(work / "deployed", filter="data")
                    archive_path.unlink()
                (work / "context.json").write_text(json.dumps({**context, "stage": stage, "base_sha": base}))
                os.chown(work / "context.json", 1000, 1000)
                network, bridge, address = self._network(tag)
                with RunGateway((address, 0), self.config["gateway_url"], secret_file(self.config["gateway_token_file"]),
                                job["incident_id"], self.api.evidence, self.host_snapshot, seconds) as gateway:
                    manifest.update({"bridge": bridge, "port": gateway.address[1]})
                    (directory / "manifest.json").write_text(json.dumps(manifest))
                    with self._firewall(bridge, gateway.address[1]):
                        command(self.container_args(name, work, network, gateway))
                        command(["docker", "start", name])
                        try:
                            self._exec(name, ["agent", str(max(1, int(deadline - time.monotonic()) - 20))], job_id, deadline, quota=gateway)
                        except Deferred as error:
                            error.usage = dict(gateway.usage)
                            error.active_seconds = time.monotonic() - started
                            raise
                        usage = dict(gateway.usage)
                # Quiesce background processes before reading worker-controlled files.
                command(["docker", "rm", "-f", name])
                value = validate_output(json.loads(self._read_worker_file(work, "result.json", 256 * 1024)), stage)
                command(self.container_args(name, work, "none"))
                command(["docker", "start", name])
                _, patch = self._exec(name, ["export", base], job_id, deadline)
                command(["docker", "rm", "-f", name])
                (artifacts / "patch.diff").write_bytes(patch)
                saved_patch = True
                (artifacts / "result.json").write_text(json.dumps(value))
                checks = []
                if value["outcome"] == "patch":
                    paths = validate_patch(patch)
                    for target in work.iterdir():
                        if target.is_symlink():
                            target.unlink()
                        elif target.is_dir():
                            shutil.rmtree(target)
                        else:
                            target.unlink()
                    self._checkout(work, base)
                    command(["git", "-c", "safe.directory=" + str(work / "repo"),
                             "-C", str(work / "repo"), "apply", "--index", "--whitespace=error", "-"], input=patch)
                    (work / "changed.json").write_text(json.dumps(paths))
                    os.chown(work / "changed.json", 1000, 1000)
                    command(self.container_args(name, work, "none"))
                    command(["docker", "start", name])
                    exit_code, receipt = self._exec(name, ["verify"], job_id, deadline, check=False)
                    command(["docker", "rm", "-f", name])
                    checks = verification_receipt(receipt, exit_code)
                    if {item["name"] for item in checks if item.get("passed")} != {"fmt", "clippy", "tests"}:
                        value["outcome"] = "needs_human"
                        # Failure logs contain only repository code and synthetic tests;
                        # bound and credential-check before reuse by a subsequent agent.
                        for item in checks:
                            if not item.get("passed"):
                                log = self._read_worker_file(work, item["name"] + ".log", 64 * 1024 * 1024)
                                excerpt = log[-4000:].decode("utf-8", errors="replace")
                                try:
                                    value["diagnosis"]["missing"].append(text(excerpt, 4000))
                                except InvalidResult:
                                    value["diagnosis"]["missing"].append("Verification failed; diagnostic text was withheld by the output policy.")
                result = {**value, "patch_path": str(artifacts / "patch.diff") if patch else None,
                          "base_sha": base, "checks": checks, "usage": usage,
                          "active_seconds": time.monotonic() - started, "artifact_dir": str(artifacts)}
                (artifacts / "receipt.json").write_text(json.dumps(result))
                return result
            except Exception as error:
                failure = error
            finally:
                try:
                    command(["docker", "rm", "-f", name], check=False)
                    if mounted and not saved_patch:
                        # On cancellation/OOM/agent error, export after all agent processes
                        # have stopped; a failed export retains the bounded workspace.
                        try:
                            command(self.container_args(name, work, "none"))
                            command(["docker", "start", name])
                            result = command(["docker", "exec", name, "python3", "/opt/maintenance/worker.py", "export", base], timeout=30)
                            if len(result.stdout) <= PATCH_LIMIT:
                                (artifacts / "partial.patch").write_bytes(result.stdout)
                                saved_patch = True
                        except (Deferred, OSError, subprocess.SubprocessError):
                            pass
                        finally:
                            command(["docker", "rm", "-f", name], check=False)
                    if network:
                        command(["docker", "network", "rm", network], check=False)
                    if mounted:
                        command(["umount", str(work)])
                    if saved_patch or not mounted:
                        shutil.rmtree(directory)
                except Exception as error:
                    # Keep the original quota classification and the recovery journal.
                    if failure is None:
                        failure = error
                    try:
                        (artifacts / "cleanup-failed.json").write_text(json.dumps({"recovery_required": True}))
                    except OSError:
                        pass
                if failure is not None:
                    failure.active_seconds = max(0, time.monotonic() - started)
                    if gateway is not None:
                        failure.usage = dict(gateway.usage)
                    raise failure

    def recover(self):
        """Reconcile only this supervisor's journaled containers after a crash."""
        with self.slot():
            directory = self.state / "workspaces"
            if not directory.exists():
                return
            for record in directory.glob("*/manifest.json"):
                manifest = json.loads(record.read_text())
                tag = manifest["tag"]
                if not re.fullmatch(r"[a-f0-9]{12}", tag) or record.parent.name != tag:
                    raise Deferred("invalid workspace recovery journal")
                name = "opm-" + tag
                work = record.parent / "work"
                volume = record.parent / "workspace.ext4"
                base = sha(manifest["base_sha"])
                job_id = identifier(manifest["job_id"])
                artifacts = self.state / "artifacts" / job_id / tag
                artifacts.mkdir(parents=True, exist_ok=True, mode=0o700)
                command(["docker", "rm", "-f", name], check=False)
                if "port" in manifest:
                    bridge = "opm" + tag[:10]
                    port = int(manifest["port"])
                    if not 1 <= port <= 65535:
                        raise Deferred("invalid recovery gateway port")
                    rules = [("INPUT", ["-i", bridge, "-j", "DROP"]),
                             ("INPUT", ["-i", bridge, "-p", "tcp", "--dport", str(port), "-j", "ACCEPT"]),
                             ("FORWARD", ["-i", bridge, "-j", "DROP"])]
                    for chain, rule in rules:
                        command(["iptables", "-w", "-D", chain, *rule], check=False)
                command(["docker", "network", "rm", name], check=False)
                if not volume.exists():
                    continue
                if not os.path.ismount(work):
                    command(["mount", "-o", "loop,nodev,nosuid", str(volume), str(work)])
                exported = False
                try:
                    if (work / "repo" / ".git").exists():
                        command(self.container_args(name, work, "none"))
                        command(["docker", "start", name])
                        result = command(["docker", "exec", name, "python3", "/opt/maintenance/worker.py", "export", base], timeout=30)
                        if len(result.stdout) <= PATCH_LIMIT:
                            (artifacts / "partial.patch").write_bytes(result.stdout)
                            exported = True
                    else:
                        exported = True  # Crash occurred before source preparation.
                finally:
                    command(["docker", "rm", "-f", name], check=False)
                    command(["umount", str(work)])
                if exported:
                    shutil.rmtree(record.parent)
                else:
                    raise Deferred("partial workspace preserved for operator recovery")
