from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

from .errors import OpsError


class OpsdClientError(OpsError):
    exit_code = 50


@dataclass
class OpsdClient:
    host: str
    port: int
    timeout: float = 1.0

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def health(self) -> dict[str, Any]:
        return self._request("GET", "/health")

    def post_batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/v1/events:batch", payload)

    def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        data = None
        headers = {"Content-Type": "application/json"}
        if payload is not None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = Request(url, data=data, headers=headers, method=method)
        try:
            with urlopen(request, timeout=self.timeout) as response:
                body = response.read().decode("utf-8")
        except URLError as exc:
            raise OpsdClientError(str(exc)) from exc
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise OpsdClientError(f"Invalid JSON response from opsd: {exc}") from exc
