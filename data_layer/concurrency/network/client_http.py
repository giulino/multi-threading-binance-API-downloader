
from dataclasses import dataclass
from collections import deque
import aiohttp
import asyncio
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
            pool_maxsize: int = 64,
    ):
        hosts = list(hosts)
        if not hosts:
            raise ValueError("HttpClient requires at least one base host")
        
        self.hosts = deque(hosts)
        self.timeout = aiohttp.ClientTimeout(
            total=None,
            sock_connect=connection_timeouts_s,
            sock_read=read_timeouts_s,
        )
        self.max_retries = max_retries
        self.backoff_base_s = backoff_base_s
        self.backoff_factor = backoff_factor
        self.default_headers = default_headers or {}
        self.pool_maxsize = pool_maxsize
        self.session: Optional[aiohttp.ClientSession] = None
        self._connector: Optional[aiohttp.TCPConnector] = None

    async def __aenter__(self):
        await self._ensure_session()
        return self
    
    async def __aexit__(self, exc_type, exc, tb):
        if self.session and not self.session.closed:
            await self.session.close()
        if self._connector and not self._connector.closed:
            await self._connector.close()

    async def _ensure_session(self):
        """Create the aiohttp session if it is not already available."""
        if self.session and not self.session.closed:
            return
        if self._connector and not self._connector.closed:
            await self._connector.close()
        self._connector = aiohttp.TCPConnector(
            limit=self.pool_maxsize,
            limit_per_host=self.pool_maxsize,
        )
        self.session = aiohttp.ClientSession(
            connector=self._connector,
            timeout=self.timeout,
        )

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
    def retry_after_seconds(headers: Dict[str, str]):
        """Static Method that reads the header response"""

        ra = headers.get("retry-after")
        if ra is None:
            return None
        try:
            return float(ra)
        except ValueError:
            return None

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
        return min(total, 8.0)

    def _merge_headers(self, headers: Optional[Dict[str, str]]):
        merged = dict(self.default_headers)
        if headers:
            merged.update(headers)
        return merged

    # Build request
    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]],
        headers: Optional[Dict[str, str]],
    ) -> HttpResult:
        """Build and execute an HTTP request with retries/backoff."""

        await self._ensure_session()

        attempts = 0
        merged_headers = self._merge_headers(headers)
        qparams = {k: v for k, v in (params or {}).items() if v is not None}

        last_status: Optional[int] = None
        last_body: str = ""
        last_host: Optional[str] = None
        last_url: Optional[str] = None

        while attempts < self.max_retries:
            attempts += 1
            host = self.hosts[0]
            url = self.full_url(host, path)
            last_host = host
            last_url = url

            try:
                start_time = time.perf_counter()
                async with self.session.request(
                    method,
                    url,
                    params=qparams,
                    headers=merged_headers,
                    timeout=self.timeout,
                ) as response:
                    end_time = time.perf_counter()
                    rtt = end_time - start_time

                    status = response.status
                    last_status = status

                    hdrs = {k.lower(): v for k, v in response.headers.items()}
                    body_text = await response.text()
                    last_body = body_text

                    content_type = hdrs.get("content-type", "")
                    if "application/json" in content_type:
                        try:
                            data = await response.json(content_type=None)
                        except Exception:
                            data = body_text
                    else:
                        data = body_text

                    if 200 <= status < 300:
                        return HttpResult(
                            status=status,
                            headers=hdrs,
                            data=data,
                            elapsed_ms=rtt * 1000.0,
                            url=url,
                            host=host,
                        )

                    if status == 418:
                        raise BannedError(
                            "418 temporary ban",
                            url=url,
                            status=status,
                            host=host,
                            attempt=attempts,
                            body=body_text,
                        )

                    if status == 429:
                        retry = self.retry_after_seconds(hdrs)
                        sleep_s = retry if retry is not None else self.backoff_seconds(attempts)
                        if attempts >= self.max_retries:
                            raise RateLimitError(
                                "429 rate limit (retries exhausted)",
                                url=url,
                                status=status,
                                host=host,
                                attempt=attempts,
                                body=body_text,
                            )
                        await asyncio.sleep(sleep_s)
                        continue

                    if 500 <= status < 600:
                        self.rotate_host()
                        await asyncio.sleep(self.backoff_seconds(attempts))
                        continue

                    raise HttpError(
                        f"Sender malformed requests: {status}: {body_text[:120]}",
                        url=url,
                        status=status,
                        host=host,
                        attempt=attempts,
                        body=body_text,
                    )

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                self.rotate_host()
                await asyncio.sleep(self.backoff_seconds(attempts))
                if attempts >= self.max_retries:
                    raise HttpError(
                        f"Network error after {attempts} attempts: {exc}",
                        url=last_url or "",
                        status=None,
                        host=last_host or "",
                        attempt=attempts,
                        body=str(exc),
                    ) from exc
                continue

        if last_status == 429:
            raise RateLimitError(
                f"429 rate limit (retries exhausted)",
                url=last_url or "",
                status=last_status,
                host=last_host or "",
                attempt=attempts,
                body=last_body,
            )

        raise HttpError(
            f"Max retries reached ({self.max_retries})",
            url=last_url or "",
            status=last_status,
            host=last_host or "",
            attempt=attempts,
            body=last_body,
        )

    # API
    async def get(self, 
            path: str, 
            params: Optional[Dict[str, Any]], 
            headers: Optional[Dict[str, str]]=None):
        """Public, user-facing method for performing HTTP GET requests"""
        
        return await self._request("GET", path, params=params, headers=headers)
    

async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    hosts = [
        "https://api.binance.com",
        "https://api1.binance.com",
        "https://api2.binance.com",
    ]

    async with HttpClient(hosts=hosts, max_retries=3, connection_timeouts_s=3, read_timeouts_s=2) as client:
        path = "/api/v3/klines"
        params = {"symbol": "BTCUSDT", "interval": "1m", "limit": 5}

        try:
            result = await client.get(path, params=params)
            logging.info(f"HTTP GET success! Status: {result.status}, elapsed: {result.elapsed_ms:.2f}ms")
            logging.info(f"Data returned (first item): {result.data[0] if result.data else 'No data'}")
        except RateLimitError as e:
            logging.warning(f"Rate-limited: {e}")
        except HttpError as e:
            logging.error(f"HTTP error: {e}")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
    
