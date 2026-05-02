from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

import requests


class R2StorageError(RuntimeError):
    pass


class R2StorageNotConfiguredError(R2StorageError):
    pass


@dataclass(frozen=True)
class R2Config:
    account_id: str
    bucket_name: str
    access_key_id: str
    secret_access_key: str
    endpoint: str
    public_base_url: str = ""


def load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[7:].strip()

        name, value = line.split("=", 1)
        name = name.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[name] = value
    return values


def load_settings(root_dir: Path) -> dict[str, str]:
    settings: dict[str, str] = {}
    for candidate in (root_dir / "env.local", root_dir / ".env.local"):
        settings.update(load_env_file(candidate))
    settings.update({key: value for key, value in os.environ.items() if isinstance(value, str)})
    return settings


def normalize_r2_key(key: str) -> str:
    return (
        str(key or "")
        .replace("\\", "/")
        .lstrip("/")
        .split("?")[0]
    )


class R2JsonStorage:
    def __init__(self, config: R2Config | None) -> None:
        self.config = config
        self.session = requests.Session()

    @classmethod
    def from_environment(cls, root_dir: Path) -> "R2JsonStorage":
        settings = load_settings(root_dir)
        account_id = settings.get("CLOUDFLARE_ACCOUNT_ID", "").strip()
        bucket_name = settings.get("CLOUDFLARE_R2_BUCKET_NAME", "").strip()
        access_key_id = settings.get("CLOUDFLARE_R2_ACCESS_KEY_ID", "").strip()
        secret_access_key = settings.get("CLOUDFLARE_R2_SECRET_ACCESS_KEY", "").strip()
        endpoint = settings.get("CLOUDFLARE_R2_ENDPOINT", "").strip()
        public_base_url = (
            settings.get("CLOUDFLARE_R2_PUBLIC_BASE_URL", "").strip()
            or settings.get("CLOUDFLARE_R2_PUBLIC_URL", "").strip()
        )

        if not endpoint and account_id:
            endpoint = f"https://{account_id}.r2.cloudflarestorage.com"

        if not all([account_id, bucket_name, access_key_id, secret_access_key, endpoint]):
            return cls(None)

        return cls(
            R2Config(
                account_id=account_id,
                bucket_name=bucket_name,
                access_key_id=access_key_id,
                secret_access_key=secret_access_key,
                endpoint=endpoint.rstrip("/"),
                public_base_url=public_base_url.rstrip("/"),
            )
        )

    @property
    def is_configured(self) -> bool:
        return self.config is not None

    def require_config(self) -> R2Config:
        if self.config is None:
            raise R2StorageNotConfiguredError(
                ".env.local に R2 の接続情報を設定してください。"
            )
        return self.config

    def public_url(self, key: str) -> str:
        config = self.require_config()
        if not config.public_base_url:
            return ""
        return f"{config.public_base_url}/{quote(normalize_r2_key(key), safe='/-_.~')}"

    def read_json(self, key: str) -> dict[str, Any] | None:
        response = self._request("GET", key)
        if response.status_code == 404:
            return None
        if not response.ok:
            raise R2StorageError(f"R2からの読み込みに失敗しました。({response.status_code}) {key}")
        try:
            payload = response.json()
        except ValueError as exc:
            raise R2StorageError(f"R2上のJsonを読めませんでした。{key}") from exc
        return payload if isinstance(payload, dict) else None

    def write_json(self, key: str, payload: dict[str, Any]) -> str:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        response = self._request(
            "PUT",
            key,
            body=body,
            extra_headers={"content-type": "application/json; charset=utf-8"},
        )
        if not response.ok:
            raise R2StorageError(f"R2への保存に失敗しました。({response.status_code}) {key}")
        return normalize_r2_key(key)

    def write_bytes(self, key: str, body: bytes, content_type: str = "application/octet-stream") -> str:
        response = self._request(
            "PUT",
            key,
            body=body,
            extra_headers={"content-type": content_type},
        )
        if not response.ok:
            raise R2StorageError(f"R2への保存に失敗しました。({response.status_code}) {key}")
        return normalize_r2_key(key)

    def delete_object(self, key: str) -> None:
        response = self._request("DELETE", key)
        if response.status_code == 404:
            return
        if not response.ok:
            raise R2StorageError(f"R2上の削除に失敗しました。({response.status_code}) {key}")

    def _request(
        self,
        method: str,
        key: str,
        *,
        body: bytes = b"",
        extra_headers: dict[str, str] | None = None,
    ) -> requests.Response:
        config = self.require_config()
        normalized_key = normalize_r2_key(key)
        endpoint = config.endpoint.rstrip("/")
        url = f"{endpoint}/{quote(config.bucket_name, safe='')}/{quote(normalized_key, safe='/-_.~')}"
        parsed_url = urlsplit(url)
        now = datetime.now(timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        payload_hash = hashlib.sha256(body).hexdigest()

        headers = {
            "host": parsed_url.netloc,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
        }
        if extra_headers:
            headers.update({name.lower(): value for name, value in extra_headers.items()})

        canonical_headers = "".join(f"{name}:{headers[name].strip()}\n" for name in sorted(headers))
        signed_headers = ";".join(sorted(headers))
        canonical_uri = parsed_url.path or "/"
        canonical_request = "\n".join(
            [
                method.upper(),
                canonical_uri,
                parsed_url.query,
                canonical_headers,
                signed_headers,
                payload_hash,
            ]
        )
        credential_scope = f"{date_stamp}/auto/s3/aws4_request"
        string_to_sign = "\n".join(
            [
                "AWS4-HMAC-SHA256",
                amz_date,
                credential_scope,
                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
            ]
        )
        signing_key = self._signing_key(config.secret_access_key, date_stamp)
        signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        headers["authorization"] = (
            "AWS4-HMAC-SHA256 "
            f"Credential={config.access_key_id}/{credential_scope},"
            f"SignedHeaders={signed_headers},Signature={signature}"
        )

        return self.session.request(
            method.upper(),
            url,
            data=body,
            headers=headers,
            timeout=60,
        )

    @staticmethod
    def _signing_key(secret_access_key: str, date_stamp: str) -> bytes:
        date_key = hmac.new(f"AWS4{secret_access_key}".encode("utf-8"), date_stamp.encode("utf-8"), hashlib.sha256).digest()
        region_key = hmac.new(date_key, b"auto", hashlib.sha256).digest()
        service_key = hmac.new(region_key, b"s3", hashlib.sha256).digest()
        return hmac.new(service_key, b"aws4_request", hashlib.sha256).digest()
