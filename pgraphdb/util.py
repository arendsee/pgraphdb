from __future__ import annotations
import sys
import requests
import http.client

def handle_response(response : requests.Response, writeResult : bool = True) -> requests.Response:
    if response.status_code >= 400:
        print(f"DatabaseError: {response.status_code}: {response.text}", file=sys.stderr)
        sys.exit(1)
    else:
        if writeResult:
            print(response.text, file=sys.stderr)
    return response

def handle_http_response(response : http.client.HTTPResponse, writeResult : bool = True) -> http.client.HTTPResponse:
    if response.status >= 400:
        print(f"DatabaseError: {response.status}: {response.read().decode('utf-8')}", file=sys.stderr)
        sys.exit(1)
    else:
        if writeResult:
            print(response.read(), file=sys.stderr)
    return response
