"""
MEXC API Client v4.2 - fixed HTML 404 handling, proper fallback
"""
import time
import requests
from typing import Optional

import config

# API-only domains (not www which returns HTML)
MEXC_API_DOMAINS = [
    "https://api.mexc.com",
    "https://contract.mexc.com",
]

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
}


class MexcClientSync:
    def __init__(self):
        self.base_url = config.MEXC_BASE_URL
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._req_count = 0
        self._window_start = time.time()
        self.last_error = ""
        self._exchange_info_cache = None
        self._exchange_info_time = 0

    def _rate_limit(self):
        now = time.time()
        if now - self._window_start < 1.0:
            self._req_count += 1
            if self._req_count > 10:
                time.sleep(1.0 - (now - self._window_start) + 0.15)
                self._window_start = time.time()
                self._req_count = 0
        else:
            self._window_start = now
            self._req_count = 1

    def _is_json_response(self, r):
        """Check if response is actually JSON, not HTML"""
        ct = r.headers.get("content-type", "")
        if "application/json" in ct:
            return True
        # Some MEXC endpoints don't set content-type properly
        text = r.text.strip()
        return text.startswith("{") or text.startswith("[")

    def _get(self, endpoint, params=None, timeout=20, retries=2):
        self._rate_limit()
        last_err = ""
        for attempt in range(retries + 1):
            try:
                url = f"{self.base_url}{endpoint}"
                r = self.session.get(url, params=params, timeout=timeout)
                if r.status_code == 200:
                    if self._is_json_response(r):
                        self.last_error = ""
                        return r.json()
                    else:
                        last_err = f"Got HTML instead of JSON from {self.base_url}"
                        break
                elif r.status_code == 429:
                    last_err = f"429 Rate Limit (attempt {attempt+1})"
                    time.sleep(3 + attempt * 2)
                    continue
                elif r.status_code == 403:
                    last_err = "403 Forbidden - IP blocked"
                    break
                elif r.status_code == 404:
                    # 404 with HTML = wrong domain, try fallback
                    if not self._is_json_response(r):
                        last_err = f"404 HTML from {self.base_url} - wrong domain"
                        break
                    last_err = f"404 Not Found: {endpoint}"
                    break
                elif r.status_code == 503:
                    last_err = "503 Service Unavailable"
                    time.sleep(2)
                    continue
                else:
                    text = r.text[:150] if self._is_json_response(r) else "(HTML page)"
                    last_err = f"HTTP {r.status_code}: {text}"
                    break
            except requests.exceptions.ConnectTimeout:
                last_err = f"ConnectTimeout ({timeout}s)"
                time.sleep(1)
            except requests.exceptions.ReadTimeout:
                last_err = f"ReadTimeout ({timeout}s)"
                time.sleep(1)
            except requests.exceptions.ConnectionError as e:
                last_err = f"ConnectionError: {str(e)[:80]}"
                time.sleep(1)
            except Exception as e:
                last_err = f"{type(e).__name__}: {str(e)[:80]}"
                break
        self.last_error = last_err
        return None

    def _get_with_fallback(self, endpoint, params=None, timeout=20):
        result = self._get(endpoint, params, timeout)
        if result is not None:
            return result
        # Try fallback API domains
        original = self.base_url
        for domain in MEXC_API_DOMAINS:
            if domain == original:
                continue
            self.base_url = domain
            result = self._get(endpoint, params, timeout, retries=1)
            if result is not None:
                return result  # keep this domain as base
        self.base_url = original
        return None

    def get_exchange_info(self):
        now = time.time()
        if self._exchange_info_cache and now - self._exchange_info_time < 300:
            return self._exchange_info_cache
        result = self._get_with_fallback("/api/v3/exchangeInfo", timeout=30)
        if result:
            self._exchange_info_cache = result
            self._exchange_info_time = now
        return result

    def get_all_tickers_24h(self):
        return self._get_with_fallback("/api/v3/ticker/24hr", timeout=25)

    def get_order_book(self, symbol, limit=100):
        return self._get_with_fallback(
            "/api/v3/depth", {"symbol": symbol, "limit": limit})

    def get_recent_trades(self, symbol, limit=100):
        return self._get_with_fallback(
            "/api/v3/trades", {"symbol": symbol, "limit": limit})

    def get_klines(self, symbol, interval="60m", limit=100):
        return self._get_with_fallback(
            "/api/v3/klines",
            {"symbol": symbol, "interval": interval, "limit": limit})

    def get_agg_trades(self, symbol, limit=1000):
        return self._get("/api/v3/aggTrades",
                         {"symbol": symbol, "limit": limit})

    def get_ticker_24h(self, symbol):
        return self._get("/api/v3/ticker/24hr", {"symbol": symbol})

    def ping(self):
        for domain in [self.base_url] + MEXC_API_DOMAINS:
            try:
                r = self.session.get(f"{domain}/api/v3/ping", timeout=10)
                if r.status_code == 200 and self._is_json_response(r):
                    self.base_url = domain
                    return True, f"OK ({domain})"
            except Exception:
                continue
        return False, f"All domains failed"


# Async client (optional)
try:
    import asyncio
    import aiohttp
    class MexcClientAsync:
        def __init__(self):
            self.base_url = config.MEXC_BASE_URL
            self._session = None
            self._req_count = 0
            self._window_start = time.time()
        async def _get_session(self):
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=15), headers=HEADERS)
            return self._session
        async def close(self):
            if self._session and not self._session.closed:
                await self._session.close()
        async def _request(self, endpoint, params=None):
            session = await self._get_session()
            now = time.time()
            if now - self._window_start < 1.0:
                self._req_count += 1
                if self._req_count > 10:
                    await asyncio.sleep(1.1 - (now - self._window_start))
                    self._window_start = time.time(); self._req_count = 0
            else:
                self._window_start = now; self._req_count = 1
            try:
                async with session.get(f"{self.base_url}{endpoint}", params=params) as resp:
                    if resp.status == 200:
                        ct = resp.headers.get("content-type","")
                        if "json" in ct or "json" in (await resp.text())[:5]:
                            return await resp.json()
                    elif resp.status == 429:
                        await asyncio.sleep(5)
                        return await self._request(endpoint, params)
                    return None
            except: return None
        async def get_exchange_info(self):
            return await self._request("/api/v3/exchangeInfo")
        async def get_all_tickers_24h(self):
            return await self._request("/api/v3/ticker/24hr")
        async def get_order_book(self, symbol, limit=100):
            return await self._request("/api/v3/depth", {"symbol": symbol, "limit": limit})
        async def get_recent_trades(self, symbol, limit=100):
            return await self._request("/api/v3/trades", {"symbol": symbol, "limit": limit})
except ImportError:
    pass
