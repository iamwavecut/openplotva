import json
import sys
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from gateway import RunGateway


class GatewayTests(unittest.TestCase):
    def test_native_quota_response_defers_without_exposing_provider_body(self):
        class Provider(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                self.rfile.read(int(self.headers["Content-Length"]))
                self.send_response(429)
                self.send_header("Retry-After", "7200")
                self.end_headers()
                self.wfile.write(b"private-provider-canary")

        server = ThreadingHTTPServer(("127.0.0.1", 0), Provider)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with RunGateway(("127.0.0.1", 0), "http://127.0.0.1:" + str(server.server_port),
                            "synthetic-provider-token", 1, None, None, 10) as gateway:
                request = urllib.request.Request("http://127.0.0.1:" + str(gateway.address[1]) + "/v1/chat/completions",
                    data=b'{"model":"glm-5.3","messages":[]}',
                    headers={"Authorization": "Bearer " + gateway.token})
                with self.assertRaises(urllib.error.HTTPError) as caught:
                    urllib.request.urlopen(request)
                with caught.exception as response:
                    self.assertEqual(response.code, 429)
                    self.assertNotIn(b"private-provider-canary", response.read())
                self.assertTrue(gateway.quota_unavailable.is_set())
                self.assertEqual(gateway.retry_after_seconds, 7200)
                self.assertEqual(gateway.usage["rate_limited"], 1)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_capability_is_scoped_expires_and_cannot_read_credentials(self):
        now = [100.0]
        with RunGateway(("127.0.0.1", 0), "http://127.0.0.1:1", "synthetic-upstream-secret", 42,
                        lambda n: {"incident": n, "available": True}, lambda: {"memory_available": 12},
                        10, clock=lambda: now[0]) as gateway:
            base = "http://127.0.0.1:" + str(gateway.address[1])
            def request(path, token=gateway.token, data=None):
                req = urllib.request.Request(base + path, data=data, headers={"Authorization": "Bearer " + token})
                try:
                    with urllib.request.urlopen(req) as response:
                        return response.status, json.load(response)
                except urllib.error.HTTPError as error:
                    with error:
                        return error.code, json.load(error)
            self.assertEqual(request("/incidents/42/evidence"), (200, {"incident": 42, "available": True}))
            self.assertEqual(request("/incidents/43/evidence")[0], 404)
            self.assertEqual(request("/v1/snapshot")[0], 404)
            self.assertEqual(request("/host", "wrong")[0], 401)
            self.assertEqual(request("/v1/chat/completions", data=b'{"model":"paid-fallback"}')[0], 403)
            now[0] = 111
            self.assertEqual(request("/incidents/42/evidence")[0], 401)

    def test_usage_stores_only_counters(self):
        gateway = RunGateway(("127.0.0.1", 0), "http://127.0.0.1:1", "secret", 1, None, None, 10)
        self.addCleanup(gateway.server.server_close)
        gateway.observe_usage(b'data: {"usage":{"prompt_tokens":12,"completion_tokens":3},"secret":"private"}\n\ndata: [DONE]\n')
        self.assertEqual(gateway.usage["input_tokens"], 12)
        self.assertEqual(gateway.usage["output_tokens"], 3)
        self.assertNotIn("secret", json.dumps(gateway.usage))


if __name__ == "__main__":
    unittest.main()
