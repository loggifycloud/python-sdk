from __future__ import annotations

from typing import Any, Callable, Awaitable

from loggify.monitor import Monitor

Receive = Callable[[], Awaitable[dict[str, Any]]]
Send = Callable[[dict[str, Any]], Awaitable[None]]
ASGIApp = Callable[[dict[str, Any], Receive, Send], Awaitable[None]]


class ASGIMiddleware:
    """Records incoming HTTP as a server span. Use with FastAPI / Starlette / Django ASGI."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: dict[str, Any], receive: Receive, send: Send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        method = scope.get("method", "GET")
        path = scope.get("path") or "/"
        headers = {key.decode("latin1").lower(): value.decode("latin1") for key, value in scope.get("headers", [])}
        request_scope = Monitor.begin_request(method, path, headers.get("traceparent"))
        status_code = 200

        async def send_wrapper(message: dict[str, Any]) -> None:
            nonlocal status_code
            if message.get("type") == "http.response.start":
                status_code = int(message.get("status") or 200)
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
            route = scope.get("route")
            template = getattr(route, "path", None) if route is not None else None
            if template:
                Monitor.set_http_route(template)
                Monitor.set_span_name(f"{method} {template}")
                Monitor.set_span_attribute("http.route", template)
        except Exception as err:
            Monitor.capture_exception(err, endpoint=path, method=method, status_code=500)
            request_scope.set_status(500)
            raise
        else:
            request_scope.set_status(status_code)
        finally:
            try:
                request_scope.set_request_size(int(headers["content-length"]))
            except (KeyError, TypeError, ValueError):
                pass
            request_scope.close()
