"""Read-only GitHub gate; SSH transmits only the issue and workflow run IDs."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import urllib.request

REPO = "iamwavecut/openplotva"
OWNER = {"login": "iamwavecut", "id": 239034}


def owned(value):
    return isinstance(value, dict) and all(value.get(key) == expected for key, expected in OWNER.items())


def validate(event, issue, environment):
    if (environment.get("GITHUB_REPOSITORY") != REPO or environment.get("GITHUB_EVENT_NAME") != "issues"
            or environment.get("GITHUB_ACTOR") != OWNER["login"]
            or environment.get("GITHUB_ACTOR_ID") != str(OWNER["id"])):
        return False
    return (event.get("action") in ("opened", "labeled", "reopened")
            and owned(event.get("sender")) and owned(issue.get("user"))
            and event.get("repository", {}).get("full_name") == REPO
            and issue.get("number") == event.get("issue", {}).get("number")
            and issue.get("state") == "open" and "pull_request" not in issue
            and {"agent:created", "agent:queued"}.issubset({label["name"] for label in issue.get("labels", [])}))


def main():
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    number = event.get("issue", {}).get("number")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    if type(number) is not int or number <= 0 or not run_id.isdigit():
        raise SystemExit("invalid dispatch identifiers")
    request = urllib.request.Request(f"https://api.github.com/repos/{REPO}/issues/{number}",
        headers={"Authorization": "Bearer " + os.environ["GH_TOKEN"], "Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(request, timeout=30) as response:
        issue = json.load(response)
    if not validate(event, issue, os.environ):
        raise SystemExit("current issue is not authorized for maintenance")
    with tempfile.TemporaryDirectory(prefix="maintenance-dispatch-") as directory:
        key, hosts = Path(directory) / "key", Path(directory) / "known_hosts"
        key.write_text(os.environ["MAINTENANCE_SSH_KEY"] + "\n")
        key.chmod(0o600)
        hosts.write_text(os.environ["MAINTENANCE_SSH_KNOWN_HOSTS"] + "\n")
        hosts.chmod(0o600)
        subprocess.run(["ssh", "-T", "-i", str(key), "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes",
                        "-o", "StrictHostKeyChecking=yes", "-o", "UserKnownHostsFile=" + str(hosts),
                        "-o", "ConnectTimeout=15", "openplotva-maintenance-dispatch@geta.moe",
                        f"enqueue {number} {run_id}"], check=True, timeout=90)
    print("Accepted by the durable maintenance controller; this is not a repair result.")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.SubprocessError):
        raise SystemExit("dispatch failed; retry this workflow after checking controller status") from None
