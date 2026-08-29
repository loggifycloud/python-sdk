from __future__ import annotations

import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from loggify import Monitor
from loggify.wsgi import wsgi


class Collector:
    def __init__(self) -> None:
        self.posts: list[tuple[str, str]] = []
        self.lock = threading.Lock()
        parent = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                length = int(self.headers.get("content-length") or 0)
                body = self.rfile.read(length).decode("utf-8")
                with parent.lock:
                    parent.posts.append((self.path, body))
                self.send_response(202)
                self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def endpoint(self) -> str:
        host, port = self.server.server_address[:2]
        return f"http://{host}:{port}"

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    def bodies(self, path: str) -> str:
        with self.lock:
            return "".join(body for item, body in self.posts if item == path)


class MonitorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.collector = Collector()
        Monitor.init(
            api_key="test-key",
            service="orders-api",
            environment="test",
            endpoint=self.collector.endpoint,
            flush_interval_ms=60_000,
            capture_logging=False,
        )

    def tearDown(self) -> None:
        self.collector.stop()

    def wait_until(self, check) -> None:
        deadline = time.time() + 2
        while time.time() < deadline:
            if check():
                return
            time.sleep(0.02)
        raise AssertionError("timed out waiting for collector posts")

    def test_records_logs_and_explicit_spans(self) -> None:
        Monitor.info("order accepted", {"orderId": "ord_123"})
        Monitor.warn("queue delayed", {"lagMs": 420})
        self.wait_until(lambda: "order accepted" in self.collector.bodies("/v1/logs"))

        def charge(span):
            span.set_attribute("payment.provider", "test")
            ctx = Monitor.current_trace_context()
            self.assertEqual(span.trace_id, ctx.trace_id)
            return None

        Monitor.with_span("charge", charge, kind="client")
        Monitor.flush()
        self.wait_until(lambda: "charge" in self.collector.bodies("/v1/ingest"))
        ingest = self.collector.bodies("/v1/ingest")
        self.assertIn('"name": "charge"', ingest)
        self.assertIn('"kind": "client"', ingest)
        self.assertIn("payment.provider", ingest)

    def test_records_incoming_http_route_templates(self) -> None:
        with Monitor.begin_request("GET", "/orders/42") as scope:
            Monitor.set_http_route("/orders/{id}")
            Monitor.set_span_name("GET /orders/{id}")
            Monitor.set_span_attribute("http.route", "/orders/{id}")
            scope.set_status(200)
        Monitor.flush()
        self.wait_until(lambda: "/orders/{id}" in self.collector.bodies("/v1/ingest"))
        ingest = self.collector.bodies("/v1/ingest")
        self.assertIn('"/orders/{id}"', ingest)
        self.assertIn("GET /orders/{id}", ingest)
        self.assertIn('"kind": "server"', ingest)
        self.assertNotIn("/orders/42", ingest.replace("/orders/{id}", ""))

    def test_captures_exceptions(self) -> None:
        Monitor.capture_exception(RuntimeError("payment failed"), endpoint="/pay", method="POST", status_code=500)
        Monitor.flush()
        self.wait_until(lambda: "payment failed" in self.collector.bodies("/v1/ingest"))
        ingest = self.collector.bodies("/v1/ingest")
        self.assertIn('"exceptionType": "RuntimeError"', ingest)
        self.assertIn('"/pay"', ingest)

    def test_wsgi_and_traceparent(self) -> None:
        captured: list[str] = []

        class Echo(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                captured.append(self.headers.get("traceparent") or "")
                self.send_response(204)
                self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                return

        echo = ThreadingHTTPServer(("127.0.0.1", 0), Echo)
        threading.Thread(target=echo.serve_forever, daemon=True).start()
        try:
            parent_trace = "a" * 32
            parent_span = "b" * 16

            def app(environ, start_response):
                opener = Monitor.opener()
                opener.open(f"http://127.0.0.1:{echo.server_address[1]}/pay")
                start_response("200 OK", [("content-type", "text/plain")])
                return [b"ok"]

            wrapped = wsgi(app)
            environ = {
                "REQUEST_METHOD": "GET",
                "PATH_INFO": "/orders/1",
                "HTTP_TRACEPARENT": f"00-{parent_trace}-{parent_span}-01",
                "CONTENT_LENGTH": "0",
            }
            wrapped(environ, lambda status, headers, exc_info=None: None)
            Monitor.flush()
            self.wait_until(lambda: '"kind": "client"' in self.collector.bodies("/v1/ingest"))
            ingest = self.collector.bodies("/v1/ingest")
            self.assertIn(parent_trace, ingest)
            self.assertIn("GET /orders/1", ingest)
            self.assertEqual(len(captured), 1)
            self.assertTrue(captured[0].startswith(f"00-{parent_trace}-"))
            self.assertTrue(captured[0].endswith("-01"))
        finally:
            echo.shutdown()
            echo.server_close()

    def test_extract_traceparent(self) -> None:
        ctx = Monitor.extract_traceparent("00-" + "a" * 32 + "-" + "b" * 16 + "-01")
        self.assertEqual(ctx.trace_id, "a" * 32)
        self.assertEqual(ctx.span_id, "b" * 16)
        self.assertIsNone(Monitor.extract_traceparent("nope"))


if __name__ == "__main__":
    unittest.main()
