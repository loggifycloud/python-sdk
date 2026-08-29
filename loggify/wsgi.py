from __future__ import annotations

from typing import Any, Callable

from loggify.monitor import Monitor

StartResponse = Callable[[str, list[tuple[str, str]]], Any]
WsgiApp = Callable[[dict[str, Any], StartResponse], Any]


def wsgi(app: WsgiApp) -> WsgiApp:
    def middleware(environ: dict[str, Any], start_response: StartResponse) -> Any:
        method = environ.get("REQUEST_METHOD", "GET")
        path = environ.get("PATH_INFO") or "/"
        traceparent = environ.get("HTTP_TRACEPARENT")
        scope = Monitor.begin_request(method, path, traceparent)
        status_code = 200

        def wrapped_start(status: str, headers: list[tuple[str, str]], exc_info: Any = None) -> Any:
            nonlocal status_code
            try:
                status_code = int(str(status).split(" ", 1)[0])
            except ValueError:
                status_code = 500
            if exc_info is not None:
                return start_response(status, headers, exc_info)
            return start_response(status, headers)

        try:
            result = app(environ, wrapped_start)
            return result
        except Exception as err:
            Monitor.capture_exception(err, endpoint=path, method=method, status_code=500)
            scope.set_status(500)
            raise
        finally:
            length = environ.get("CONTENT_LENGTH")
            try:
                scope.set_request_size(int(length) if length else None)
            except (TypeError, ValueError):
                pass
            scope.set_status(status_code)
            scope.close()

    return middleware


def flask(app: Any) -> Any:
    app.wsgi_app = wsgi(app.wsgi_app)

    def after_request(response: Any) -> Any:
        try:
            from flask import request

            rule = getattr(request, "url_rule", None)
            if rule is not None and getattr(rule, "rule", None):
                Monitor.set_http_route(rule.rule)
                Monitor.set_span_name(f"{request.method} {rule.rule}")
                Monitor.set_span_attribute("http.route", rule.rule)
        except Exception:
            pass
        return response

    if hasattr(app, "after_request"):
        app.after_request(after_request)
    return app
