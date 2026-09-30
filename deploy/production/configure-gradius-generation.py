#!/usr/bin/env python3
"""Install a generation placement key received on stdin into the runtime env."""

import base64
import json
import os
from pathlib import Path
import re
import sys
import tempfile


def configure(env_path: Path, payload: dict) -> None:
    key = payload.get("key", "")
    if not isinstance(key, str) or not re.fullmatch(r"ad-[A-Za-z0-9_-]{30,}", key):
        raise ValueError("Invalid generation placement key")
    if env_path.is_symlink():
        raise ValueError("Runtime env must be a regular file")
    if env_path.exists():
        original = env_path.read_text()
    else:
        original = base64.b64decode(payload["production_env_b64"], validate=True).decode()
        if not original.strip():
            raise ValueError("Production env is required for bootstrap")
    retained = [line for line in original.splitlines()
                if not re.match(r"\s*(?:export\s+)?(?:GRADIUS_GENERATION_API_KEY|GRADIUS_UTILITY_IMAGE_ENABLED)\s*=", line)]
    content = "\n".join(retained) + "\nGRADIUS_GENERATION_API_KEY=" + key + "\nGRADIUS_UTILITY_IMAGE_ENABLED=true\n"
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=env_path.parent, delete=False) as output:
            temporary = output.name
            os.chmod(temporary, 0o600)
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, env_path)
        temporary = None
    finally:
        if temporary is not None:
            os.unlink(temporary)


if __name__ == "__main__":
    try:
        configure(Path(sys.argv[1]), json.load(sys.stdin))
    except (ValueError, OSError, KeyError, IndexError):
        sys.exit("Cannot install Gradius generation key")
    print("Gradius generation key installed")
