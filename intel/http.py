from __future__ import annotations

import time
from typing import Any
import requests
from loguru import logger

from .settings import settings


def get(url: str, *, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None) -> requests.Response:
    h = {"User-Agent": settings.http_user_agent}
    if headers:
        h.update(headers)
    resp = requests.get(url, params=params, headers=h, timeout=settings.http_timeout_s)
    resp.raise_for_status()
    return resp


def get_bytes(url: str, *, params: dict[str, Any] | None = None) -> bytes:
    return get(url, params=params).content


def polite_sleep(seconds: float | None = None) -> None:
    time.sleep(settings.ctg_sleep_s if seconds is None else seconds)
