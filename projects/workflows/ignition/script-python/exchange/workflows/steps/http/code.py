# exchange/workflows/steps/http.py
"""
HTTP steps.

This uses Ignition's system.net.httpClient().
"""

from exchange.workflows.engine.runtime import step

log = system.util.getLogger("exchange." + system.util.getProjectName().lower() +".steps.http")

@step(name="http.request")
def request(method, url, headers=None, params=None, data=None, json_body=None, timeout_ms=10000):
    """
    Simple HTTP request wrapper.

    Args:
        method (object): Input value for this call.
        url (object): Input value for this call.
        headers (object): Input value for this call.
        params (dict): Input value for this call.
        data (object): Input value for this call.
        json_body (object): Input value for this call.
        timeout_ms (object): Time value in milliseconds.

    Returns:
        dict: Structured payload with results or status details.
    """
    headers = headers or {}
    params = params or {}

    client = system.net.httpClient(timeout=timeout_ms)

    m = (method or "GET").upper()
    kwargs = {}
    if headers:
        kwargs["headers"] = headers
    if params:
        kwargs["params"] = params
    if json_body is not None:
        kwargs["json"] = json_body
    elif data is not None:
        kwargs["data"] = data

    resp = client.request(m, url, **kwargs)
    try:
        text = resp.getText()
    except:
        text = None

    try:
        rh = dict(resp.getHeaders())
    except:
        rh = {}

    return {"status": int(resp.getStatusCode()), "text": text, "headers": rh}