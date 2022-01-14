from __future__ import annotations
import sys
import requests

def handle_response(response : requests.Response, writeResult : bool = True) -> requests.Response:
    if response.status_code >= 400:
        print(f"DatabaseError: {response.status_code}: {response.text}", file=sys.stderr)
        sys.exit(1)
    else:
        if writeResult:
            print(response.text, file=sys.stderr)
    return response
