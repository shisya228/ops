from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .errors import OpsError


class OpsdClientError(OpsError):
    exit_code = 50


@dataclass
class OpsdClient:
    endpoint: str
    timeout: float = 1.0

    def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        base = self.endpoint.rstrip("/")
        query = f"?{urlencode(params, doseq=True)}" if params else ""
        url = f"{base}{path}{query}"
        data = None
        headers = {"Content-Type": "application/json"}
        if payload is not None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = Request(url, data=data, headers=headers, method=method)
        try:
            with urlopen(request, timeout=self.timeout) as response:
                body = response.read().decode("utf-8")
        except HTTPError as exc:
            try:
                detail = exc.read().decode("utf-8")
            except Exception:  # noqa: BLE001
                detail = str(exc)
            raise OpsdClientError(detail) from exc
        except URLError as exc:
            raise OpsdClientError(str(exc)) from exc
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise OpsdClientError(f"Invalid JSON response from opsd: {exc}") from exc

    def health(self) -> dict[str, Any]:
        return self._request("GET", "/health")

    def post_batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/v1/events:batch", payload)

    def get_events(self, params: dict[str, Any]) -> dict[str, Any]:
        return self._request("GET", "/v1/events", params=params)

    def get_event(self, event_id: str) -> dict[str, Any]:
        return self._request("GET", f"/v1/events/{event_id}")

    def create_source(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/v1/sources", payload)

    def list_sources(self) -> dict[str, Any]:
        return self._request("GET", "/v1/sources")

    def get_source(self, name: str) -> dict[str, Any]:
        return self._request("GET", f"/v1/sources/{name}")

    def delete_source(self, name: str) -> dict[str, Any]:
        return self._request("DELETE", f"/v1/sources/{name}")

    def test_source(self, name: str) -> dict[str, Any]:
        return self._request("POST", f"/v1/sources/{name}:test")

    def run_ingest(self, name: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", f"/v1/ingests/{name}:run", payload)

    def create_view(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/v1/views", payload)

    def list_views(self) -> dict[str, Any]:
        return self._request("GET", "/v1/views")

    def get_view(self, name: str) -> dict[str, Any]:
        return self._request("GET", f"/v1/views/{name}")

    def delete_view(self, name: str) -> dict[str, Any]:
        return self._request("DELETE", f"/v1/views/{name}")

    def query_view(self, name: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", f"/v1/views/{name}:query", payload)

    def create_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/v1/jobs", payload)

    def list_jobs(self) -> dict[str, Any]:
        return self._request("GET", "/v1/jobs")

    def get_job(self, name: str) -> dict[str, Any]:
        return self._request("GET", f"/v1/jobs/{name}")

    def delete_job(self, name: str) -> dict[str, Any]:
        return self._request("DELETE", f"/v1/jobs/{name}")

    def run_job(self, name: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", f"/v1/jobs/{name}:run", payload)

    def job_runs(self, name: str) -> dict[str, Any]:
        return self._request("GET", f"/v1/jobs/{name}/runs")

    def list_artifacts(self, params: dict[str, Any]) -> dict[str, Any]:
        return self._request("GET", "/v1/artifacts", params=params)

    def pack_artifacts(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/v1/artifacts:pack", payload)
