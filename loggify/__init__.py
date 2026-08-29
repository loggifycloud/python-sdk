from loggify.asgi import ASGIMiddleware
from loggify.http import opener
from loggify.monitor import Monitor, SpanKind, SpanStatus
from loggify.wsgi import flask, wsgi

Monitor.opener = staticmethod(opener)
Monitor.wsgi = staticmethod(wsgi)
Monitor.flask = staticmethod(flask)
Monitor.ASGIMiddleware = ASGIMiddleware

__all__ = [
    "ASGIMiddleware",
    "Monitor",
    "SpanKind",
    "SpanStatus",
    "flask",
    "opener",
    "wsgi",
]
