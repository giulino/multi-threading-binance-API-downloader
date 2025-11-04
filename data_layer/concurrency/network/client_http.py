from __future__ import annotations
from dataclasses import dataclass
from collections import deque
import requests
import random
import time
import logging
from typing import Any, Dict, Optional, Iterable


# Errors Management
class HttpError(Exception):
    """Generic HTTP error connection"""
    def __init__(self, 
            msg : str, 
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

# Client
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
            hosts: Iterable[str],
            connection_timeouts_s: float = 9.0,
            read_timeouts_s: float = 2.0,
            max_retries: int = 6,  
            backoff_base_s: float = 0.25,
            backoff_factor: int = 2,
            default_headers: Optional[Dict[str, str]] = None,
    ):
        hosts = list(hosts)
        if not hosts:
            raise ValueError("HttpClient requires at least one base host")
        
        self.hosts = deque(hosts)
        self.session = requests.Session() 
        self.timeout = (connection_timeouts_s, read_timeouts_s)
        self.max_retries = max_retries
        self.backoff_base_s = backoff_base_s
        self.backoff_factor = backoff_factor
        self.default_headers = default_headers or {}

    # Inner Functions
 
    @staticmethod
    def full_url(host: str, path: str):
        """Static Method to avoid bugs on missing slashes"""

        if path.startswith("http://") or path.startswith("https://"):
            return path
        if not path.startswith("/"):
            path = "/" + path
        return f"{host}{path}"
    
    @staticmethod
    def parse_payload(response: requests.Response):
        """
        Helper Method deciding how to interpret the server’s response body, 
        depending on its Content-Type header
        """
        
        type = response.headers.get("Content-Type", "")
        if "application/json" in type:
            try:
                return response.json() # parsing JSON in python object
            except ValueError:
                return response.text # returns the raw text as it is
        return response.text
    
    @staticmethod
    def retry_after_seconds(headers: Dict[str, str]):
        """Static Method that reads the header response"""

        ra = headers.get("retry-after")
        if ra is None:
            return None
        try:
            return float(ra)
        except ValueError:
            return print("The client couldn't access the response header")

    def rotate_host(self):
        """
        Method used to change host if the first one fails to connect.
        The failed host will be placed at the end of the list.
        """

        if len(self.hosts) > 1:
            self.hosts.rotate(-1)

    def backoff_seconds(self, attempt: int) :
        """
        Method calculating the sleeping time exponentially
        based on the number of attempts. More attempts, more sleep.            
        """

        base = self.backoff_base_s * (self.backoff_factor ** (attempt - 1))
        jitter = random.uniform(0.0, 0.1) # de-synchronized retries across clients
        total = base + jitter
        print(f"Sleep for: {total} seconds")
        
        return min(total, 8.0)

    def sleep_backoff(self, attempt: int):
        time.sleep(self.backoff_seconds(attempt))

    def _merge_headers(self, headers: Optional[Dict[str, str]]):
        merged = dict(self.default_headers)
        if headers:
            merged.update(headers)
        return merged

    # Build request
    def _request(self, 
                method: str, 
                path: str, 
                params: Optional[Dict[str, Any]], 
                headers: Optional[Dict[str, str]]):    
        """
        Method that builds the http request and handle responses.
        """
                 
        attempts = 0
        merged_headers = self._merge_headers(headers)
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
                self.rotate_host()
                if attempts > self.max_retries:
                    raise HttpError(f"Network error after {attempts} attempts: {e}",
                                    url=url, status=None, host=host, attempt=attempts)
                self.sleep_backoff(attempts)
            
            status = response.status_code
            if not status:
                self.rotate_host()
                if attempts > self.max_retries:
                    raise HttpError(f"Network error after {attempts} attempts: {e}",
                                    url=url, status=None, host=host, attempt=attempts)
                self.sleep_backoff(attempts)

            hdrs = {k.lower(): v for k, v in response.headers.items()}
            body_text = response.text or ""

            if 200 <= status < 300:
                data = self.parse_payload(response)
                return HttpResult(status=status, 
                                  headers=hdrs, 
                                  data=data, 
                                  elapsed_ms=rtt, 
                                  url=url,
                                  host=host
                                  )
            # temporary banned error   
            if status == 418:
                raise BannedError("418 temporary ban", url=url, status=status, 
                                  host=host, attempt=attempts, body=body_text)
            # rate limit error
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
                logging.info("Rotating host")
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

    # API
    def get(self, 
            path: str, 
            params: Optional[Dict[str, Any]], 
            headers: Optional[Dict[str, str]]=None):
        """Public, user-facing method for performing HTTP GET requests"""
        
        return self._request("GET", path, params=params, headers=headers)
    

if __name__ == "__main__":

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
    