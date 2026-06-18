"""Minimal Apache Unomi REST client (stdlib HTTP, no extra dependency)."""

from __future__ import annotations

import base64
import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from app.services.unomi_settings_service import UnomiConnectionConfig


class UnomiClientError(Exception):
    def __init__(self, message: str, *, status_code: int | None = None, body: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class UnomiClient:
    def __init__(self, config: UnomiConnectionConfig, *, timeout_sec: float = 30.0):
        self._config = config
        self._timeout = timeout_sec
        self._api_root = f"{config.base_url}/cxs"

    def _auth_header(self) -> str:
        token = base64.b64encode(f"{self._config.username}:{self._config.password}".encode("utf-8")).decode("ascii")
        return f"Basic {token}"

    def request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict | list | None = None,
        query: str = "",
        extra_headers: dict[str, str] | None = None,
    ) -> Any:
        path = path if path.startswith("/") else f"/{path}"
        url = f"{self._api_root}{path}"
        if query:
            url = f"{url}?{query.lstrip('?')}"

        data = None
        headers = {
            "Authorization": self._auth_header(),
            "Accept": "application/json",
        }
        if extra_headers:
            headers.update(extra_headers)
        if json_body is not None:
            data = json.dumps(json_body).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = Request(url, data=data, headers=headers, method=method.upper())
        try:
            with urlopen(req, timeout=self._timeout) as resp:
                raw = resp.read().decode("utf-8")
                if not raw.strip():
                    return None
                return json.loads(raw)
        except HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8")
            except Exception:
                pass
            raise UnomiClientError(
                f"Unomi HTTP {e.code} for {method} {path}",
                status_code=e.code,
                body=body,
            ) from e
        except URLError as e:
            raise UnomiClientError(f"Unomi connection failed for {method} {path}: {e}") from e

    def list_segment_metadata(self, *, offset: int = 0, size: int = 200) -> list[dict]:
        items = self.request("GET", "/segments/", query=f"offset={offset}&size={size}")
        if not items:
            return []
        if isinstance(items, list):
            return items
        return []

    def get_segment(self, segment_id: str) -> dict:
        return self.request("GET", f"/segments/{quote(segment_id, safe='')}")

    def save_segment(self, segment_def: dict) -> dict:
        return self.request("POST", "/segments", json_body=segment_def)

    def delete_segment(self, segment_id: str) -> None:
        self.request("DELETE", f"/segments/{quote(segment_id, safe='')}")

    def is_profile_in_segment(self, *, profile_id: str, segment_id: str) -> bool:
        """Uses Unomi match endpoint when available."""
        pid = quote(profile_id, safe="")
        sid = quote(segment_id, safe="")
        try:
            result = self.request("GET", f"/segments/{sid}/match/{pid}")
            if isinstance(result, bool):
                return result
            if isinstance(result, dict):
                return bool(result.get("match") or result.get("matched") or result.get("result"))
        except UnomiClientError as e:
            if e.status_code == 404:
                return False
            raise
        return False

    def get_profile(self, profile_id: str) -> dict | None:
        pid = quote(profile_id, safe="")
        try:
            result = self.request("GET", f"/profiles/{pid}")
            return result if isinstance(result, dict) else None
        except UnomiClientError as e:
            if e.status_code == 404:
                return None
            raise

    def save_profile(self, profile_body: dict) -> dict | None:
        return self.request("POST", "/profiles", json_body=profile_body)

    def collect_events(self, payload: dict, *, peer_key: str | None = None) -> dict | None:
        """POST /cxs/eventcollector — recommended Unomi 2.x path for profile create/update."""
        extra: dict[str, str] = {}
        if peer_key:
            extra["X-Unomi-Peer"] = peer_key
        return self.request("POST", "/eventcollector", json_body=payload, extra_headers=extra or None)

    def delete_profile(self, profile_id: str, *, with_data: bool = True) -> None:
        """Remove profile via Unomi privacy API (administrative delete)."""
        pid = quote(profile_id, safe="")
        flag = "true" if with_data else "false"
        try:
            self.request("DELETE", f"/privacy/profiles/{pid}", query=f"withData={flag}")
        except UnomiClientError as e:
            if e.status_code == 404:
                return
            raise

    def get_impacted_profile_ids(self, segment_id: str, *, limit: int = 5000) -> list[str]:
        """Best-effort list of profile itemIds currently in the segment."""
        sid = quote(segment_id, safe="")
        try:
            payload = self.request("GET", f"/segments/{sid}/impacted", query=f"limit={limit}")
        except UnomiClientError:
            return []

        ids: list[str] = []
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, str):
                    ids.append(item)
                elif isinstance(item, dict):
                    pid = item.get("itemId") or item.get("profileId") or item.get("id")
                    if pid:
                        ids.append(str(pid))
        elif isinstance(payload, dict):
            for item in payload.get("list") or payload.get("profiles") or []:
                if isinstance(item, str):
                    ids.append(item)
                elif isinstance(item, dict):
                    pid = item.get("itemId") or item.get("profileId") or item.get("id")
                    if pid:
                        ids.append(str(pid))
        return ids
