"""Opt-in real image smoke; fake inference uses no account credentials."""
import json
import os
import subprocess
import sys
import threading
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


@unittest.skipUnless(os.environ.get("MAINTENANCE_TEST_IMAGE"), "set MAINTENANCE_TEST_IMAGE to a built image")
class ImageTests(unittest.TestCase):
    def test_pinned_omp_runs_headless_and_writes_validated_artifact(self):
        calls = []
        result = {"diagnosis": {"external_cause": "confirmed", "code_defect": "not_observed",
            "observations": ["Synthetic provider outage"], "hypotheses": [], "supporting": ["Synthetic failure"],
            "contradicting": [], "related_changes": [], "missing": [], "acceptance": [],
            "next_action": "observe", "title": "Synthetic outage", "summary": "Synthetic fixture only", "matches": []},
            "outcome": "no_fix", "feedback": []}

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                calls.append(data)
                if len(calls) == 1:
                    message = {"role": "assistant", "content": None, "tool_calls": [{"id": "call_fixture", "type": "function",
                        "function": {"name": "write", "arguments": json.dumps({"path": "/work/result.json", "content": json.dumps(result)})}}]}
                else:
                    message = {"role": "assistant", "content": "Artifact written."}
                # OMP requests streaming; return actual OpenAI SSE chunks.
                chunk = {"id": "fixture", "object": "chat.completion.chunk", "created": 1,
                         "model": "glm-5.3", "choices": [{"index": 0, "delta": message, "finish_reason": None}]}
                if "tool_calls" in message:
                    message["tool_calls"][0]["index"] = 0
                end = {"id": "fixture", "object": "chat.completion.chunk", "created": 1, "model": "glm-5.3",
                       "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls" if len(calls) == 1 else "stop"}]}
                body = b"".join(b"data: " + json.dumps(item).encode() + b"\n\n" for item in (chunk, end)) + b"data: [DONE]\n\n"
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = ThreadingHTTPServer(("0.0.0.0", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        container_name = "opm-image-smoke-" + uuid.uuid4().hex[:12]
        network = ["--add-host", "host.docker.internal:host-gateway"] if sys.platform == "linux" else []
        try:
            status = subprocess.run(["docker", "run", "--name", container_name, *network, "--rm", "--user", "1000:1000", "--read-only",
                "--cap-drop", "ALL", "--security-opt", "no-new-privileges=true", "--cpus", "2",
                "--memory", "4g", "--memory-swap", "4g", "--pids-limit", "256",
                "--tmpfs", "/work:rw,exec,size=512m,uid=1000,gid=1000,mode=0700",
                "--tmpfs", "/tmp:rw,size=64m,uid=1000,gid=1000,mode=1777",
                "--workdir", "/work", "--env", "PI_CONFIG_DIR=.omp", "--env", "PI_CODING_AGENT_DIR=/work/omp/agent",
                "--env", "MAINTENANCE_RUN_TOKEN=synthetic-revocable-capability", "--env", "MAINTENANCE_INCIDENT_ID=1",
                "--env", "MAINTENANCE_GATEWAY=http://host.docker.internal:" + str(server.server_port),
                os.environ["MAINTENANCE_TEST_IMAGE"], "sh", "-c",
                "mkdir /work/repo /work/tmp && printf '{\"stage\":\"initial\"}' > /work/context.json && "
                "python3 /opt/maintenance/worker.py agent 60; result=$?; "
                "if [ $result -ne 0 ]; then tail -c 4000 /work/agent-events.jsonl; fi; exit $result"],
                capture_output=True, text=True, timeout=90)
            self.assertEqual(status.returncode, 0, status.stdout + status.stderr)
            self.assertGreaterEqual(len(calls), 2)
            self.assertEqual(calls[0]["model"], "glm-5.3")
            system = "\n".join(message["content"] for message in calls[0]["messages"]
                               if message["role"] == "system" and isinstance(message.get("content"), str))
            self.assertIn("Trusted launch budget", system)
            self.assertIn("Stage: initial", system)
            self.assertIn("Available runtime: 60 seconds", system)
            self.assertIn("Checkpoint deadline (UTC):", system)
            self.assertIn("Hard deadline (UTC):", system)
            self.assertIn("normal exit", system)
            self.assertTrue(any(tool["function"]["name"] == "write" for tool in calls[0]["tools"]))
        finally:
            subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, timeout=20, check=False)
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
