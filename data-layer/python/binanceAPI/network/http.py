from __future__ import annotations
from dataclasses import dataclass
from collections import deque
import requests
import random
import time
import logging
from typing import Any, Dict, Optional, Iterable


# ------------- Errors Management -------------

class HttpError(Exception):
    """Generic HTTP error connection"""
    def __init__(self, msg : str, 
            url : str, 
            status : int, 
            host : str,
            attempt : int, 
            body : str = ""):
        super().__init__(msg)
        
        self.msg = msg
        self.url = url
        self.status = status
        self.host = host
        self.attempt = attempt
        self.body = body[:512]

class RateLimitError(HttpError):
    """429 Too Many Requests: back off before retrying."""

class BannedError(HttpError):
    """418 (or similar): temporary ban—pause the whole job."""


# -------- Result container --------

@dataclass
class HttpResult:
    status: int
    headers: Dict[str, str]
    data: Any             # JSON or text
    elapsed_ms: float
    url: str
    host: str

# ------------- Client -------------

class HttpClient:
    """
    Http client equipped with 
    a light resiliency framework 
    to handle:
    - connection errors
    - rate limits 
    """

    def __init__(
            self,
            base_hosts: Iterable[str],
            connect_timeouts_s: float = 3.0,
            read_timeouts_s: float = 2.0,
            max_retries: int = 5,
            backoff_base_s: float = 0.25,
            backoff_factor: int = 2, 
            default_headers: Optional[Dict[str, str]] = None
    ):
        hosts = list(base_hosts)
        if not hosts:
            raise ValueError("HttpClient requires at least one base host")
        
        self.hosts = deque(hosts)
        self.session = requests.Session()
        self.timeout = (connect_timeouts_s, read_timeouts_s)
        self.max_retries = max_retries
        self.backoff_base_s = backoff_base_s
        self.backoff_factor = backoff_factor
        self.default_headers = dict(default_headers or {})

    # ------------- Inner Functions -------------
    
    # Normalize URLs
    @staticmethod
    def full_url(host: str, path: str):
        
        """Static function to avoid bugs on missing slashes"""

        if path.startswith("http://") or path.startswith("https://"):
            return path
        if not path.startswith("/"):
            path = "/" + path
        return f"{host}{path}"
    
    @staticmethod
    def parse_payload(resp: requests.Response) -> Any:
        ctype = resp.headers.get("Content-Type", "")
        if "application/json" in ctype:
            try:
                return resp.json()
            except ValueError:
                return resp.text
        return resp.text
    
    def merge_headers(self, headers: Optional[Dict[str, str]]):
        merged = dict(self.default_headers)
        if headers:
            merged.update(headers)
        return merged
    
    @staticmethod
    def retry_after_seconds(headers: Dict[str, str]):
        ra = headers.get("retry-after")
        if ra is None:
            return None
        try:
            return float(ra)
        except ValueError:
            return None

    def rotate_host(self) -> None:
        if len(self.hosts) > 1:
            self.hosts.rotate(-1)

    def backoff_seconds(self, attempt: int) :
        # simple exponential backoff with a tiny jitter
        base = self.backoff_base_s * (self.backoff_factor ** (attempt - 1))
        jitter = random.uniform(0.0, 0.1) # de-synchronized retries across clients
        return min(base + jitter, 8.0)

    def sleep_backoff(self, attempt: int) -> None:
        time.sleep(self.backoff_seconds(attempt))

    # ------------- Internal Logic -------------

    def _request(self, method: str, path: str, params: Optional[Dict[str, Any]], 
                 headers: Optional[Dict[str, str]]):
                 
        attempts = 0
        merged_headers = self._merge_headers(headers)
        qparams = {k: v for k, v in (params or {}).items() if v is not None}

        
        while True:
            
            attempts +=1
            host = self.hosts[0]
            url = self.full_url(host, path)

            start_time = time.perf_counter()
            try:
                resp = self.session.request(
                    method, url, params=qparams, headers=merged_headers, timeout=self.timeout
                )
                end_time = time.perf_counter()
                delta_time = (end_time - start_time) * 1000

            except (requests.ConnectionError, requests.Timeout) as e:
                # network hiccup: backoff and rotate host
                self.rotate_host()
                if attempts > self.max_retries:
                    raise HttpError(f"Network error after {attempts} attempts: {e}",
                                    url=url, status=None, host=host, attempt=attempts)
                self.sleep_backoff(attempts)

            status = resp.status_code
            hdrs = {k.lower(): v for k, v in resp.headers.items()}
            body_text = resp.text or ""

            if 200 <= status < 300:
                data = self.parse_payload(resp)
                return HttpResult(status=status, headers=hdrs, 
                                  data=data, elapsed_ms=delta_time, 
                                  url=url,
                                  host=host
                                  )
            if status == 418:
                raise BannedError("418 temporary ban", url=url, status=status, 
                                  host=host, attempt=attempts, body=body_text)

            if status == 429:
                retry = self.retry_after_seconds(hdrs)
                sleep_s = retry if retry is not None else self.backoff_seconds(attempts)    
                if attempts > self.max_retries:
                    raise RateLimitError("429 rate limit (retries exhausted)",
                                         url=url, status=status, 
                                         host=host, attempt=attempts, body=body_text)
                time.sleep(sleep_s)
                continue
            
            # 5xx server side errors: rotate host
            if 500 <= status < 600:
                self.rotate_host()
                if attempts > self.max_retries:
                    raise HttpError(f"Server error {status} (retries exhausted)",
                                    url=url, status=status, host=host, 
                                    attempt=attempts, body=body_text)
                self.sleep_backoff(attempts)
                continue
            
            # 4xx/odd codes: raise immediately
            raise HttpError(f"Sender malformed requests: {status}: {body_text[:120]}", 
                            url=url, status=status, host=host, attempt=attempts, 
                            body=body_text)

    # ------------- API -------------

    def get(self, path: str, params: Optional[Dict[str, Any]], 
            headers: Optional[Dict[str, str]]):
        return self._requests("GET", path, params=params, headers=headers)
    