from __future__ import annotations

import atexit
import json
import logging
import os
import random
import re
import resource
import secrets
import socket
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterator, TypeVar

TRACEPARENT = re.compile(r"^00-([0-9a-f]{32})-([0-9a-f]{16})-0[01]$", re.IGNORECASE)
T = TypeVar("T")

SpanKind = str
SpanStatus = str
KIND_INTERNAL = "internal"
KIND_SERVER = "server"
KIND_CLIENT = "client"
STATUS_OK = "ok"
STATUS_ERROR = "error"
STATUS_UNSET = "unset"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _clip(value: str | None, max_len: int = 512) -> str:
    if not value:
        return ""
    return value if len(value) <= max_len else value[:max_len]


def _resolve_hostname(override: str | None = None) -> str:
    trimmed = (override or "").strip()
    if trimmed:
        return trimmed[:255]
    env = os.environ.get("HOSTNAME", "").strip()
    if env:
        return env[:255]
    try:
        host = socket.gethostname().strip()
        return host[:255] if host else ""
    except Exception:
        return ""


def _hex(n: int) -> str:
    return secrets.token_hex(n)


@dataclass
class TraceContext:
    trace_id: str
    span_id: str


@dataclass
class _Active:
    trace_id: str
    span_id: str
    span: SpanHandle
    http_route: str | None = None


class SpanHandle:
    def __init__(
        self,
        name: str,
        kind: str,
        trace_id: str,
        span_id: str,
        parent_span_id: str | None,
        started_at: str,
        started: float,
        attributes: dict[str, Any],
    ) -> None:
        self.trace_id = trace_id
        self.span_id = span_id
        self.parent_span_id = parent_span_id
        self._kind = kind or KIND_INTERNAL
        self._started_at = started_at
        self._started = started
        self._attributes = attributes
        self._name = _clip(name)
        self._status = STATUS_UNSET
        self._ended = False

    def set_name(self, name: str) -> SpanHandle:
        if not self._ended:
            self._name = _clip(name)
        return self

    def set_attribute(self, key: str, value: Any) -> SpanHandle:
        if not self._ended:
            self._attributes[key] = value
        return self

    def set_status(self, status: str) -> SpanHandle:
        self._status = status
        return self

    def end(self, status: str | None = None) -> None:
        if self._ended:
            return
        self._ended = True
        opts = Monitor._opts
        if opts is None or random.random() > opts.sample_rate:
            return
        event: dict[str, Any] = {
            "traceId": self.trace_id,
            "spanId": self.span_id,
            "name": self._name,
            "kind": self._kind,
            "status": status or self._status,
            "timestamp": self._started_at,
            "durationMs": (time.perf_counter() - self._started) * 1000.0,
            "attributes": self._attributes,
            "serviceName": opts.service,
            "environment": opts.environment,
        }
        if self.parent_span_id:
            event["parentSpanId"] = self.parent_span_id
        Monitor._span_buf.push(event)


class RequestScope:
    def __init__(
        self,
        span: SpanHandle,
        method: str,
        fallback_path: str,
        started: float,
        token: Token[_Active | None],
    ) -> None:
        self._span = span
        self._method = method
        self._fallback_path = fallback_path
        self._started = started
        self._token = token
        self._status = 200
        self._request_size: int | None = None
        self._response_size: int | None = None
        self._closed = False

    @property
    def span(self) -> SpanHandle:
        return self._span

    def set_status(self, status_code: int) -> None:
        self._status = status_code

    def set_request_size(self, size: int | None) -> None:
        self._request_size = size

    def set_response_size(self, size: int | None) -> None:
        self._response_size = size

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            active = Monitor._context.get()
            path = active.http_route if active and active.http_route else self._fallback_path
            self._span.set_attribute("http.status_code", self._status)
            self._span.set_attribute("http.route", path)
            self._span.end(STATUS_ERROR if self._status >= 500 else STATUS_OK)
            opts = Monitor._opts
            if opts is not None and random.random() <= opts.sample_rate:
                event: dict[str, Any] = {
                    "method": self._method,
                    "route": path,
                    "statusCode": self._status,
                    "durationMs": (time.perf_counter() - self._started) * 1000.0,
                    "serviceName": opts.service,
                    "environment": opts.environment,
                    "timestamp": _now(),
                    "traceId": self._span.trace_id,
                }
                if self._request_size is not None:
                    event["requestSize"] = self._request_size
                if self._response_size is not None:
                    event["responseSize"] = self._response_size
                Monitor._http_buf.push(event)
        except Exception:
            self._span.end(STATUS_OK)
        finally:
            Monitor._context.reset(self._token)

    def __enter__(self) -> RequestScope:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc is not None:
            Monitor.capture_exception(exc, endpoint=self._fallback_path, method=self._method)
            if self._status < 500:
                self._status = 500
        self.close()


@dataclass
class _Options:
    api_key: str
    service: str
    environment: str
    endpoint: str = "https://ingest.loggify.cloud"
    sample_rate: float = 1.0
    flush_interval_ms: int = 2000
    max_buffer: int = 500
    timeout_ms: int = 1500
    capture_logging: bool = True
    hostname: str = ""


class _Buffer:
    def __init__(self, max_items: int = 500) -> None:
        self._items: list[Any] = []
        self._lock = threading.Lock()
        self.max = max_items

    def push(self, item: Any) -> None:
        with self._lock:
            if len(self._items) >= self.max:
                self._items.pop(0)
            self._items.append(item)

    def drain(self) -> list[Any]:
        with self._lock:
            out = self._items
            self._items = []
            return out


class _LogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        if Monitor._capturing_log or Monitor._opts is None or not Monitor._opts.capture_logging:
            return
        Monitor._capturing_log = True
        try:
            if record.levelno >= logging.ERROR:
                level = "ERROR"
            elif record.levelno >= logging.WARNING:
                level = "WARN"
            elif record.levelno >= logging.INFO:
                level = "INFO"
            else:
                level = "DEBUG"
            Monitor.log(level, record.getMessage(), {"source": "logging", "logger": record.name})
        except Exception:
            pass
        finally:
            Monitor._capturing_log = False


class Monitor:
    _opts: _Options | None = None
    _context: ContextVar[_Active | None] = ContextVar("loggify", default=None)
    _http_buf = _Buffer()
    _error_buf = _Buffer()
    _metric_buf = _Buffer()
    _span_buf = _Buffer()
    _timer: threading.Timer | None = None
    _runtime_timer: threading.Timer | None = None
    _started_at = time.time()
    _logging_handler: _LogHandler | None = None
    _capturing_log = False
    _atexit_registered = False
    opener: Callable[..., Any]
    wsgi: Callable[..., Any]
    flask: Callable[..., Any]
    ASGIMiddleware: Any

    @classmethod
    def init(
        cls,
        api_key: str | None = None,
        service: str | None = None,
        environment: str | None = None,
        endpoint: str | None = None,
        sample_rate: float = 1.0,
        flush_interval_ms: int = 2000,
        max_buffer: int = 500,
        timeout_ms: int = 1500,
        capture_logging: bool = True,
        **extra: Any,
    ) -> None:
        key = api_key or extra.get("apiKey")
        svc = service or extra.get("service")
        env = environment or extra.get("environment")
        if not key or not svc or not env:
            raise ValueError("api_key, service, and environment are required")
        cls._opts = _Options(
            api_key=key,
            service=svc,
            environment=env,
            endpoint=(endpoint or extra.get("endpoint") or "https://ingest.loggify.cloud").rstrip("/"),
            sample_rate=sample_rate,
            flush_interval_ms=flush_interval_ms,
            max_buffer=max_buffer,
            timeout_ms=timeout_ms,
            capture_logging=capture_logging if "captureLogging" not in extra else extra["captureLogging"],
            hostname=_resolve_hostname(extra.get("hostname")),
        )
        for buf in (cls._http_buf, cls._error_buf, cls._metric_buf, cls._span_buf):
            buf.max = max_buffer
        cls._instrument_logging()
        cls._started_at = time.time()
        cls._schedule_flush()
        cls._schedule_runtime()
        cls._collect_runtime()
        if not cls._atexit_registered:
            atexit.register(cls.flush)
            cls._atexit_registered = True

    @classmethod
    def capture_exception(
        cls,
        err: BaseException | None = None,
        endpoint: str | None = None,
        method: str | None = None,
        status_code: int | None = None,
        **extra: Any,
    ) -> None:
        try:
            error = err if isinstance(err, BaseException) else RuntimeError("unknown")
            endpoint = endpoint or extra.get("endpoint")
            method = method or extra.get("method")
            status_code = status_code if status_code is not None else extra.get("statusCode")
            payload: dict[str, Any] = {
                "message": str(error) or type(error).__name__,
                "exceptionType": type(error).__name__,
                "stackTrace": "".join(traceback.format_exception(type(error), error, error.__traceback__)),
            }
            active = cls._context.get()
            if active:
                payload["traceId"] = active.trace_id
            if endpoint:
                payload["endpoint"] = endpoint
            if method:
                payload["method"] = method
            if status_code is not None:
                payload["statusCode"] = status_code
            cls._error_buf.push(payload)
            attrs: dict[str, Any] = {
                "exceptionType": payload["exceptionType"],
                "stackTrace": payload["stackTrace"],
            }
            if endpoint:
                attrs["endpoint"] = endpoint
            if method:
                attrs["method"] = method
            if status_code is not None:
                attrs["statusCode"] = status_code
            cls.log("ERROR", f"{payload['exceptionType']}: {payload['message']}", attrs)
        except Exception:
            pass

    @classmethod
    def start_span(
        cls,
        name: str,
        kind: str = KIND_INTERNAL,
        attributes: dict[str, Any] | None = None,
        parent: TraceContext | None = None,
    ) -> SpanHandle:
        active = cls._context.get()
        if parent is not None:
            trace_id = parent.trace_id
            parent_span_id = parent.span_id
        elif active is not None:
            trace_id = active.trace_id
            parent_span_id = active.span_id
        else:
            trace_id = _hex(16)
            parent_span_id = None
        return SpanHandle(
            name,
            kind,
            trace_id,
            _hex(8),
            parent_span_id,
            _now(),
            time.perf_counter(),
            dict(attributes or {}),
        )

    @classmethod
    def with_span(cls, name: str, operation: Callable[[SpanHandle], T] | None = None, kind: str = KIND_INTERNAL) -> T | Iterator[SpanHandle]:
        if operation is None:
            return cls.span(name, kind=kind)
        with cls.span(name, kind=kind) as handle:
            return operation(handle)

    @classmethod
    @contextmanager
    def span(cls, name: str, kind: str = KIND_INTERNAL) -> Iterator[SpanHandle]:
        handle = cls.start_span(name, kind=kind)
        token = cls._context.set(_Active(handle.trace_id, handle.span_id, handle))
        try:
            yield handle
            handle.end()
        except Exception:
            handle.end(STATUS_ERROR)
            raise
        finally:
            cls._context.reset(token)

    @classmethod
    def current_trace_context(cls) -> TraceContext | None:
        active = cls._context.get()
        return TraceContext(active.trace_id, active.span_id) if active else None

    @classmethod
    def set_http_route(cls, route: str) -> None:
        try:
            active = cls._context.get()
            if active is None:
                return
            active.http_route = _clip(route)
        except Exception:
            pass

    @classmethod
    def set_span_name(cls, name: str) -> None:
        try:
            active = cls._context.get()
            if active and active.span:
                active.span.set_name(name)
        except Exception:
            pass

    @classmethod
    def set_span_attribute(cls, key: str, value: Any) -> None:
        try:
            active = cls._context.get()
            if active and active.span:
                active.span.set_attribute(key, value)
        except Exception:
            pass

    @classmethod
    def inject_traceparent(cls, context: TraceContext | None = None) -> str | None:
        ctx = context or cls.current_trace_context()
        if ctx is None:
            return None
        return f"00-{ctx.trace_id}-{ctx.span_id}-01"

    @classmethod
    def extract_traceparent(cls, header: str | None) -> TraceContext | None:
        if not header:
            return None
        match = TRACEPARENT.match(header.strip())
        if not match:
            return None
        return TraceContext(match.group(1).lower(), match.group(2).lower())

    @classmethod
    def begin_request(cls, method: str, path: str, traceparent: str | None = None) -> RequestScope:
        parent = cls.extract_traceparent(traceparent)
        handle = cls.start_span(
            f"{method} {path}",
            kind=KIND_SERVER,
            attributes={"http.method": method, "http.route": path},
            parent=parent,
        )
        token = cls._context.set(_Active(handle.trace_id, handle.span_id, handle, path))
        return RequestScope(handle, method, path, time.perf_counter(), token)

    @classmethod
    def log(cls, level: str, message: str, attributes: dict[str, Any] | None = None) -> None:
        try:
            if cls._opts is None:
                return
            active = cls._context.get()
            attrs = dict(attributes or {})
            if active:
                attrs["traceId"] = active.trace_id
                attrs["spanId"] = active.span_id
            event = {
                "level": str(level).upper(),
                "message": message,
                "attributes": attrs,
                "serviceName": cls._opts.service,
                "environment": cls._opts.environment,
                "timestamp": _now(),
            }
            cls._post("/v1/logs", {"logs": [event]})
        except Exception:
            pass

    @classmethod
    def debug(cls, message: str, attributes: dict[str, Any] | None = None) -> None:
        cls.log("DEBUG", message, attributes)

    @classmethod
    def info(cls, message: str, attributes: dict[str, Any] | None = None) -> None:
        cls.log("INFO", message, attributes)

    @classmethod
    def warn(cls, message: str, attributes: dict[str, Any] | None = None) -> None:
        cls.log("WARN", message, attributes)

    @classmethod
    def error(cls, message: str, attributes: dict[str, Any] | None = None) -> None:
        cls.log("ERROR", message, attributes)

    @classmethod
    def fatal(cls, message: str, attributes: dict[str, Any] | None = None) -> None:
        cls.log("FATAL", message, attributes)

    @classmethod
    def flush(cls) -> None:
        if cls._opts is None:
            return
        http_requests = cls._http_buf.drain()
        errors = cls._error_buf.drain()
        metrics = cls._metric_buf.drain()
        spans = cls._span_buf.drain()
        if not http_requests and not errors and not metrics and not spans:
            return
        grouped: dict[str, list[dict[str, Any]]] = {}
        for span in spans:
            grouped.setdefault(str(span.get("traceId")), []).append(span)
        traces = []
        for trace_id, items in grouped.items():
            cleaned = []
            for span in items:
                copy = dict(span)
                copy.pop("traceId", None)
                cleaned.append(copy)
            traces.append(
                {
                    "traceId": trace_id,
                    "serviceName": cls._opts.service,
                    "environment": cls._opts.environment,
                    "spans": cleaned,
                }
            )
        cls._post(
            "/v1/ingest",
            {
                "httpRequests": http_requests,
                "errors": errors,
                "metrics": metrics,
                "traces": traces,
            },
        )

    @classmethod
    def is_collector_url(cls, url: str | None) -> bool:
        return bool(cls._opts and url and url.startswith(cls._opts.endpoint))

    @classmethod
    def _post(cls, path: str, body: dict[str, Any], attempt: int = 0) -> None:
        opts = cls._opts
        if opts is None:
            return

        def run() -> None:
            try:
                data = json.dumps(body).encode("utf-8")
                req = urllib.request.Request(
                    opts.endpoint + path,
                    data=data,
                    method="POST",
                    headers={
                        "content-type": "application/json",
                        "x-api-key": opts.api_key,
                    },
                )
                urllib.request.urlopen(req, timeout=opts.timeout_ms / 1000.0).read()
            except Exception as err:
                retry = isinstance(err, urllib.error.HTTPError) and err.code == 429
                if (retry or not isinstance(err, urllib.error.HTTPError)) and attempt < 3:
                    time.sleep(0.2 * (2**attempt))
                    cls._post(path, body, attempt + 1)

        threading.Thread(target=run, daemon=True).start()

    @classmethod
    def _schedule_flush(cls) -> None:
        if cls._timer is not None:
            cls._timer.cancel()
        interval = (cls._opts.flush_interval_ms if cls._opts else 2000) / 1000.0

        def tick() -> None:
            try:
                cls.flush()
            except Exception:
                pass
            cls._schedule_flush()

        cls._timer = threading.Timer(interval, tick)
        cls._timer.daemon = True
        cls._timer.start()

    @classmethod
    def _schedule_runtime(cls) -> None:
        if cls._runtime_timer is not None:
            cls._runtime_timer.cancel()

        def tick() -> None:
            try:
                cls._collect_runtime()
            except Exception:
                pass
            cls._schedule_runtime()

        cls._runtime_timer = threading.Timer(15.0, tick)
        cls._runtime_timer.daemon = True
        cls._runtime_timer.start()

    @classmethod
    def _runtime_tags(cls) -> dict[str, str]:
        tags = {"pid": str(os.getpid())}
        if cls._opts and cls._opts.hostname:
            tags["hostname"] = cls._opts.hostname
        return tags

    @classmethod
    def _push_metric(cls, name: str, value: float) -> None:
        event: dict[str, Any] = {
            "metricName": name,
            "value": value,
            "tags": cls._runtime_tags(),
        }
        if cls._opts:
            event["serviceName"] = cls._opts.service
            event["environment"] = cls._opts.environment
        cls._metric_buf.push(event)

    @classmethod
    def _collect_runtime(cls) -> None:
        try:
            usage = resource.getrusage(resource.RUSAGE_SELF)
            rss = float(usage.ru_maxrss)
            if sys.platform != "darwin":
                rss *= 1024.0
            cls._push_metric("memory_usage", rss / 1024.0 / 1024.0)
            cls._push_metric("process_uptime", time.time() - cls._started_at)
        except Exception:
            pass

    @classmethod
    def _instrument_logging(cls) -> None:
        if cls._logging_handler is not None:
            return
        cls._logging_handler = _LogHandler()
        logging.getLogger().addHandler(cls._logging_handler)
