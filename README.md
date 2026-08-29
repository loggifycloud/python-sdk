# loggify

Python monitoring SDK for Loggify. Incoming HTTP is captured with WSGI / ASGI middleware. Logs, errors, traces, and runtime metrics are posted as Loggify JSON to ingest.

Call `Monitor.init` **before** creating the web server.

```python
import os
from loggify import Monitor

Monitor.init(
    api_key=os.environ["LOGGIFY_KEY"],
    service="orders-api",
    environment="production",
    endpoint=os.environ.get("LOGGIFY_ENDPOINT", "http://localhost:3001"),
)
```

## Install

```bash
pip install loggify
```

From this repository:

```bash
pip install -e ./python-sdk
```

## HTTP

Wrap WSGI (Flask, Django, any WSGI app) or ASGI (FastAPI / Starlette):

```python
from loggify import Monitor

app.wsgi_app = Monitor.wsgi(app.wsgi_app)
# Flask route templates (GET /orders/<id>):
Monitor.flask(app)

# FastAPI / Starlette
app.add_middleware(Monitor.ASGIMiddleware)
```

Incoming `traceparent` continues a distributed trace. Outbound urllib calls through `Monitor.opener()` inject the **client** span as W3C `traceparent`.

```python
opener = Monitor.opener()
opener.open("https://pay.example/charge")
```

## Logs

```python
Monitor.info("order accepted", {"orderId": "ord_123"})
Monitor.warn("queue delayed", {"lagMs": 420})
Monitor.error("payment failed", {"provider": "stripe"})
```

After init, the stdlib `logging` module is captured too (`capture_logging=False` to disable).

## Errors

```python
try:
    charge(order)
except Exception as err:
    Monitor.capture_exception(err, endpoint="/pay", method="POST", status_code=500)
    raise
```

HTTP 5xx from the WSGI / ASGI middleware is captured automatically.

## Traces

```python
with Monitor.span("charge") as span:
    span.set_attribute("order.id", order.id)
    charge(order)

header = Monitor.inject_traceparent()  # 00-{traceId}-{spanId}-01
parent = Monitor.extract_traceparent(incoming_header)
```

Datastore queries are not auto-patched. Wrap work with `Monitor.span` or send OTLP from existing instrumentations to the same ingest URL.
