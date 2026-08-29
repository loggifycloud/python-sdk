from __future__ import annotations

import urllib.request
from typing import Any

from loggify.monitor import KIND_CLIENT, Monitor


class TracingOpener:
    """urllib opener that records client spans and injects W3C traceparent."""

    def __init__(self, inner: urllib.request.OpenerDirector) -> None:
        self._inner = inner

    def open(self, fullurl: Any, data: Any = None, timeout: float | None = None) -> Any:
        req = fullurl if isinstance(fullurl, urllib.request.Request) else urllib.request.Request(fullurl, data=data)
        url = req.full_url
        if Monitor.is_collector_url(url):
            return self._inner.open(req, timeout=timeout)
        method = req.get_method()

        def run(span: Any) -> Any:
            span.set_attribute("http.method", method)
            span.set_attribute("http.url", url[:512])
            header = Monitor.inject_traceparent()
            if header:
                req.add_header("traceparent", header)
            response = self._inner.open(req, timeout=timeout)
            status = getattr(response, "status", None) or getattr(response, "code", None)
            if status is not None:
                span.set_attribute("http.status_code", status)
                if int(status) >= 500:
                    span.set_status("error")
            return response

        return Monitor.with_span(f"HTTP {method}", run, kind=KIND_CLIENT)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def opener(base: urllib.request.OpenerDirector | None = None) -> TracingOpener:
    return TracingOpener(base or urllib.request.build_opener())
