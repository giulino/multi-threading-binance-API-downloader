from __future__ import annotations
from dataclasses import dataclass
from collections import deque
import requests
import random
import time
import logging
from typing import Any, Dict, Optional, Iterable


# ------------- Errors Management -------------

# Custom exception class that inherits from Python’s built-in Exception class
# It behaves like any other Python error but with extra info defined

class HttpError(Exception):
    """Generic HTTP error connection"""
    def __init__(self, 
            msg : str, 
            url : str, 
            status : int, 
            host : str,
            attempt : int, 
            body : str = ""):
        super().__init__(msg) # calls the constructor of the parent class 
                              # so that the base Python error also receives the message string
        
        self.msg = msg
        self.url = url
        self.status = status
        self.host = host
        self.attempt = attempt
        self.body = body[:512]

class RateLimitError(HttpError):
    """429 Too Many Requests: back off before retrying."""

class BannedError(HttpError):
    """418: temporary banned."""

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
            hosts: Iterable[str], # list of binance endpoints
            connection_timeouts_s: float = 9.0, # max time to wait to establish a TCP connection
            read_timeouts_s: float = 2.0, # max time to wait for the server to reply after the connetion
            max_retries: int = 6, # how many times to retry a failed requests before rising an error 
            backoff_base_s: float = 0.25, # base backoff
            backoff_factor: int = 2, # exponential backoff multiplier
            default_headers: Optional[Dict[str, str]] = None,
    ):
        hosts = list(hosts)
        if not hosts:
            raise ValueError("HttpClient requires at least one base host")
        
        self.hosts = deque(hosts)
        self.session = requests.Session() # creates a persistent http session 
        self.timeout = (connection_timeouts_s, read_timeouts_s)
        self.max_retries = max_retries
        self.backoff_base_s = backoff_base_s
        self.backoff_factor = backoff_factor
        self.default_headers = default_headers or {}

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
    def parse_payload(response: requests.Response):
        """Helper function deciding how to interpret the server’s response body, 
           depending on its Content-Type header"""
        
        type = response.headers.get("Content-Type", "")
        if "application/json" in type:
            try:
                return response.json() # parsing JSON in python object
            except ValueError:
                return response.text # returns the raw text as it is
        return response.text
    
    @staticmethod
    def retry_after_seconds(headers: Dict[str, str]):
        ra = headers.get("retry-after")
        if ra is None:
            return None
        try:
            return float(ra)
        except ValueError:
            return print("The client couldn't access the response header")

    def rotate_host(self) -> None:
        if len(self.hosts) > 1:
            self.hosts.rotate(-1)

    def backoff_seconds(self, attempt: int) :
        # simple exponential backoff with a tiny jitter
        base = self.backoff_base_s * (self.backoff_factor ** (attempt - 1))
        jitter = random.uniform(0.0, 0.1) # de-synchronized retries across clients
        return min(base + jitter, 8.0)

    def sleep_backoff(self, attempt: int):
        time.sleep(self.backoff_seconds(attempt))

    def _merge_headers(self, headers: Optional[Dict[str, str]]):
        merged = dict(self.default_headers)
        if headers:
            merged.update(headers)
        return merged

    # ------------- Internal Logic -------------

    def _request(self, 
                method: str, 
                path: str, 
                params: Optional[Dict[str, Any]], 
                headers: Optional[Dict[str, str]]):
                 
        attempts = 0
        merged_headers = self._merge_headers(headers)
        # if params is None, {} is used + remove paramas whose value is None
        qparams = {k: v for k, v in (params or {}).items() if v is not None} 

        while True:
            
            attempts +=1
            host = self.hosts[0]
            url = self.full_url(host, path)

            start_time = time.perf_counter()
            try:
                response = self.session.request(
                    method, url, params=qparams, headers=merged_headers, timeout=self.timeout
                )
                end_time = time.perf_counter()
                rtt = (end_time - start_time)

            except (requests.ConnectionError, requests.Timeout) as e:
                # network hiccup: rotate host and backoff 
                self.rotate_host()
                if attempts > self.max_retries:
                    raise HttpError(f"Network error after {attempts} attempts: {e}",
                                    url=url, status=None, host=host, attempt=attempts)
                self.sleep_backoff(attempts)
            
            # Edit the code to handle the case where there is a connection error and then it starts again resetting backoff
            status = response.status_code # check the response status code
            if not status:
                self.rotate_host()
                if attempts > self.max_retries:
                    raise HttpError(f"Network error after {attempts} attempts: {e}",
                                    url=url, status=None, host=host, attempt=attempts)
                self.sleep_backoff(attempts)
            hdrs = {k.lower(): v for k, v in response.headers.items()} # lower-case keys
            body_text = response.text or "" # extract the body of the http response or empty string if no body

            if 200 <= status < 300:
                data = self.parse_payload(response)
                return HttpResult(status=status, 
                                  headers=hdrs, 
                                  data=data, 
                                  elapsed_ms=rtt, 
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

    def get(self, 
            path: str, 
            params: Optional[Dict[str, Any]], 
            headers: Optional[Dict[str, str]]=None):
        """Public, user-facing method for performing HTTP GET requests"""
        
        return self._request("GET", path, params=params, headers=headers)
    

if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    
    # List of Binance endpoints
    hosts = [
        "https://api.binance.com",
        "https://api1.binance.com",
        "https://api2.binance.com",
    ]

    client = HttpClient(hosts=hosts, max_retries=3, connection_timeouts_s=3, read_timeouts_s=2)

    # Example request: BTCUSDT 1m klines, limit 5
    path = "/api/v3/klines"
    params = {"symbol": "BTCUSDT", "interval": "1m", "limit": 5}

    try:
        result = client.get(path, params=params, headers=None)
        logging.info(f"HTTP GET success! Status: {result.status}, elapsed: {result.elapsed_ms:.2f}ms")
        logging.info(f"Data returned (first item): {result.data[0] if result.data else 'No data'}")
    except RateLimitError as e:
        logging.warning(f"Rate-limited: {e}")
    except HttpError as e:
        logging.error(f"HTTP error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    