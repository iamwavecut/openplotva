"""Short-lived per-job capability in front of the private OMP auth gateway."""
from __future__ import annotations

import hmac
import http.client
import json
import secrets
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

if __package__:
    from .contracts import MODEL
    from .quota import limited, retry_after
else:
    from contracts import MODEL
    from quota import limited, retry_after


class LimitedServer(ThreadingHTTPServer):
    request_queue_size = 8

    def __init__(self, *args):
        self.capacity = threading.BoundedSemaphore(8)
        super().__init__(*args)

    def process_request(self, request, address):
        if not self.capacity.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, address)
        except BaseException:
            self.capacity.release()
            raise

    def process_request_thread(self, request, address):
        try:
            super().process_request_thread(request, address)
        finally:
            self.capacity.release()


class RunGateway:
    def __init__(self, bind, upstream, upstream_token, incident_id, evidence, host,
                 lifetime, clock=time.time):
        self.token = secrets.token_urlsafe(32)
        self.clock = clock
        self.expires = clock() + lifetime
        self.incident_id = int(incident_id)
        self.evidence = evidence
        self.host = host
        self.upstream = urlsplit(upstream)
        if self.upstream.scheme != "http" or self.upstream.hostname != "127.0.0.1":
            raise ValueError("native auth gateway must be on loopback")
        self.upstream_token = upstream_token
        self.usage = {"requests": 0, "input_tokens": 0, "output_tokens": 0, "rate_limited": 0}
        self.quota_unavailable = threading.Event()
        self.retry_after_seconds = None
        self.lock = threading.Lock()
        self.connections = set()
        self.requests = threading.BoundedSemaphore(1)
        self.cache = {}
        gateway = self

        class Handler(BaseHTTPRequestHandler):
            def setup(self):
                super().setup()
                self.connection.settimeout(min(600, max(1, gateway.expires - gateway.clock())))

            def log_message(self, *args):
                pass  # Never log provider payloads, request URLs, or bearer tokens.

            def reply(self, code, value):
                body = json.dumps(value).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def allowed(self):
                auth = self.headers.get("Authorization", "")
                return gateway.clock() < gateway.expires and hmac.compare_digest(
                    auth.encode(), ("Bearer " + gateway.token).encode())

            def do_GET(self):
                if not self.allowed():
                    return self.reply(401, {"error": "expired or invalid job capability"})
                try:
                    if self.path == f"/incidents/{gateway.incident_id}/evidence":
                        return self.reply(200, gateway.cached("evidence", lambda: gateway.evidence(gateway.incident_id)))
                    if self.path == "/host":
                        return self.reply(200, gateway.cached("host", gateway.host))
                except Exception:
                    return self.reply(503, {"available": False})
                self.reply(404, {"error": "not available to this job"})

            def do_POST(self):
                if not self.allowed():
                    return self.reply(401, {"error": "expired or invalid job capability"})
                if self.path != "/v1/chat/completions":
                    return self.reply(404, {"error": "route not allowed"})
                if gateway.quota_unavailable.is_set():
                    return self.reply(429, {"error": "job waiting for quota recovery"})
                if not gateway.requests.acquire(blocking=False):
                    return self.reply(429, {"error": "one provider request at a time"})
                connection = None
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                    if not 0 < length <= 2 * 1024 * 1024:
                        return self.reply(413, {"error": "request exceeds limit"})
                    payload = json.loads(self.rfile.read(length))
                    if not isinstance(payload, dict) or payload.get("model") != MODEL:
                        return self.reply(403, {"error": "model not allowed"})
                    # Qualified native provider ID prevents a same-named paid model
                    # from being selected from another broker credential pool.
                    payload["model"] = "zai/" + MODEL
                    connection = http.client.HTTPConnection(gateway.upstream.hostname,
                        gateway.upstream.port or 4000, timeout=min(600, max(1, gateway.expires - gateway.clock())))
                    with gateway.lock:
                        gateway.connections.add(connection)
                    connection.request("POST", "/v1/chat/completions", json.dumps(payload), {
                        "Authorization": "Bearer " + gateway.upstream_token,
                        "Content-Type": "application/json",
                    })
                    response = connection.getresponse()
                    with gateway.lock:
                        gateway.usage["requests"] += 1
                    if response.status != 200:
                        try:
                            error = json.loads(response.read(8192))
                        except (ValueError, OSError):
                            error = None
                        if limited(response.status, error):
                            gateway.mark_quota(retry_after(response.getheader("Retry-After"), gateway.clock()))
                        connection.close()
                        return self.reply(response.status, {"error": "GLM gateway unavailable"})
                    self.send_response(200)
                    self.send_header("Content-Type", response.getheader("Content-Type", "application/json"))
                    self.end_headers()
                    body = bytearray()
                    while gateway.clock() < gateway.expires:
                        chunk = response.read1(8192)
                        if not chunk:
                            break
                        body.extend(chunk)
                        if len(body) > 8 * 1024 * 1024:
                            break
                        self.wfile.write(chunk)
                        self.wfile.flush()
                    gateway.observe_usage(bytes(body))
                    connection.close()
                except (OSError, ValueError, http.client.HTTPException):
                    # If streaming started, closing the connection is the only honest
                    # failure signal; a second JSON response would corrupt the stream.
                    self.close_connection = True
                finally:
                    if connection:
                        connection.close()
                        with gateway.lock:
                            gateway.connections.discard(connection)
                    gateway.requests.release()

        self.server = LimitedServer(bind, Handler)
        self.server.daemon_threads = True
        self.server.timeout = 1
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def cached(self, key, read):
        with self.lock:
            previous = self.cache.get(key)
            if previous and self.clock() - previous[0] < 15:
                return previous[1]
            value = read()
            self.cache[key] = (self.clock(), value)
            return value

    @property
    def address(self):
        return self.server.server_address

    def observe_usage(self, body):
        documents = [body] if b"data:" not in body else [
            line[5:].strip() for line in body.splitlines() if line.startswith(b"data:")]
        for document in documents:
            try:
                event = json.loads(document)
                if limited(200, event):
                    self.mark_quota()
                usage = event.get("usage", {})
                if isinstance(usage, dict):
                    with self.lock:
                        for dest, source in (("input_tokens", "prompt_tokens"), ("output_tokens", "completion_tokens")):
                            amount = usage.get(source, 0)
                            if type(amount) is int and 0 <= amount <= 10000000:
                                self.usage[dest] += amount
            except (ValueError, AttributeError):
                continue

    def mark_quota(self, delay=None):
        with self.lock:
            if not self.quota_unavailable.is_set():
                self.usage['rate_limited'] += 1
            self.retry_after_seconds = delay
            self.quota_unavailable.set()

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.expires = 0
        with self.lock:
            for connection in self.connections:
                connection.close()
            self.connections.clear()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
