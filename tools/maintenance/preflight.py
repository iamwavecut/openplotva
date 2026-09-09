#!/usr/bin/env python3
"""Read-only activation checks. Does not enable services or publish anything."""
from __future__ import annotations

if not __package__:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    __package__ = Path(__file__).resolve().parent.name

import argparse
import json
from pathlib import Path

from .api import MaintenanceAPI
from .contracts import Deferred, InvalidResult
from .github import GitHub
from .runner import Runner


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="/etc/openplotva-maintenance/config.json")
    arguments = parser.parse_args()
    try:
        config = json.loads(Path(arguments.config).read_text())
        api = MaintenanceAPI(config)
        runtime = api.request("GET", "/health")
        if runtime != {"status": "ok", "version": 1}:
            raise Deferred("unexpected maintenance API contract")
        github = GitHub(config)
        github.assert_owner()
        runner = Runner(config, api)
        runner.preflight()
        host = runner.host_snapshot()
        if not host.get("revision") or not host.get("runtime", {}).get("running"):
            raise Deferred("running production revision could not be verified")
        print(json.dumps({"ready_for_controlled_smoke": True, "github_identity_verified": True,
                          "runtime_api_version": 1, "host": host}, indent=2))
        return 0
    except (Deferred, InvalidResult, ValueError, TypeError, KeyError, OSError):
        print(json.dumps({"ready_for_controlled_smoke": False,
                          "reason": "Check private credential files, owner identity, runtime TLS, pinned image and host resource limits."}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
