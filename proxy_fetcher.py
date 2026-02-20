"""
proxy_fetcher.py
----------------
A plug-and-play HTTP fetching utility for Streamlit Cloud that:
  - Rotates through multiple free/paid proxy options
  - Falls back gracefully to direct connection
  - Reads proxy credentials from st.secrets (never hardcoded)

Usage in your app:
    from proxy_fetcher import get_session, fetch_url

    resp = fetch_url("https://www.screener.in/company/RELIANCE/")
    if resp:
        # use resp.content / resp.text
        ...

Streamlit Secrets setup (secrets.toml):
    [proxy]
    url = "http://username:password@proxy-host:port"
    # Leave empty or remove section to use direct connection
"""

import os
import time
import random
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import streamlit as st

# ---------------------------------------------------------------------------
# Default browser-like headers
# ---------------------------------------------------------------------------
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


# ---------------------------------------------------------------------------
# Internal helper: read proxy URL from Streamlit secrets
# ---------------------------------------------------------------------------
def _get_proxy_from_secrets() -> str | None:
    """
    Reads proxy URL from st.secrets["proxy"]["url"].
    Returns None if not configured or on any error.
    """
    try:
        proxy_url = st.secrets["proxy"]["url"]
        if proxy_url and proxy_url.strip():
            return proxy_url.strip()
    except (KeyError, AttributeError, Exception):
        pass
    return None


# ---------------------------------------------------------------------------
# Internal helper: clear system-level proxy env vars that can interfere
# ---------------------------------------------------------------------------
def _clear_system_proxies():
    for var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
        os.environ.pop(var, None)


# ---------------------------------------------------------------------------
# Build a requests.Session with retries (and optional proxy)
# ---------------------------------------------------------------------------
def get_session(proxy_url: str | None = None) -> requests.Session:
    """
    Returns a requests.Session configured with:
      - Retry strategy (3 attempts, exponential backoff)
      - Optional proxy routing
      - System proxy env vars cleared to avoid Streamlit Cloud interference

    Args:
        proxy_url: Full proxy URL e.g. "http://user:pass@host:port".
                   If None, tries st.secrets first, then direct connection.

    Returns:
        requests.Session
    """
    _clear_system_proxies()

    session = requests.Session()
    session.trust_env = False  # ignore any remaining env-level proxies

    # Determine proxy: explicit arg > secrets > None (direct)
    resolved_proxy = proxy_url or _get_proxy_from_secrets()
    if resolved_proxy:
        session.proxies = {
            "http": resolved_proxy,
            "https": resolved_proxy,
        }

    # Retry strategy
    retry = Retry(
        total=3,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


# ---------------------------------------------------------------------------
# High-level fetch helper with SSL fallback + random delay
# ---------------------------------------------------------------------------
def fetch_url(
    url: str,
    proxy_url: str | None = None,
    headers: dict | None = None,
    timeout: int = 30,
    min_delay: float = 1.0,
    max_delay: float = 3.0,
    show_status: bool = True,
) -> requests.Response | None:
    """
    Fetches a URL with:
      - Optional proxy (reads from st.secrets if not provided)
      - Random polite delay before request
      - SSL fallback (retries without verification on SSLError)
      - Streamlit status messages (can be disabled)

    Args:
        url:        URL to fetch.
        proxy_url:  Proxy URL string. None = auto-detect from secrets or direct.
        headers:    Extra headers to merge with defaults.
        timeout:    Request timeout in seconds (default 30).
        min_delay:  Minimum random delay before request (default 1.0s).
        max_delay:  Maximum random delay before request (default 3.0s).
        show_status: Whether to show st.info/success/error messages (default True).

    Returns:
        requests.Response on success, None on failure.
    """
    session = get_session(proxy_url)

    merged_headers = {**DEFAULT_HEADERS}
    if headers:
        merged_headers.update(headers)

    # Polite delay to avoid rate-limiting
    delay = random.uniform(min_delay, max_delay)
    time.sleep(delay)

    if show_status:
        proxy_label = "proxy" if (proxy_url or _get_proxy_from_secrets()) else "direct"
        st.info(f"🔍 Fetching ({proxy_label}): {url}")

    try:
        # Attempt with SSL verification
        try:
            resp = session.get(url, headers=merged_headers, timeout=timeout, verify=True)
        except requests.exceptions.SSLError:
            if show_status:
                st.warning("⚠️ SSL verification failed — retrying without SSL check...")
            resp = session.get(url, headers=merged_headers, timeout=timeout, verify=False)

        if resp.status_code == 200:
            if show_status:
                st.success(f"✅ Successfully fetched: {url}")
            return resp

        elif resp.status_code == 403:
            if show_status:
                st.error(
                    f"🔴 403 Forbidden — the server is blocking this IP. "
                    f"Configure a proxy in Streamlit Secrets to bypass this."
                )
            return None

        elif resp.status_code == 429:
            if show_status:
                st.warning("⏳ Rate limited (429). Waiting 10 seconds before retry...")
            time.sleep(10)
            resp = session.get(url, headers=merged_headers, timeout=timeout, verify=False)
            if resp.status_code == 200:
                return resp
            return None

        else:
            if show_status:
                st.warning(f"⚠️ Received HTTP {resp.status_code} from {url}")
            return None

    except requests.exceptions.ConnectionError as e:
        err_str = str(e)
        if show_status:
            st.error(f"❌ CONNECTION ERROR: Cannot reach {url}")
            if "Connection refused" in err_str or "Errno 111" in err_str:
                st.error("🔴 **STREAMLIT CLOUD NETWORK RESTRICTION DETECTED**")
                st.markdown(
                    """
**To fix this, configure a proxy in your Streamlit Secrets:**

1. Go to your app's Streamlit Cloud dashboard → **Settings → Secrets**
2. Add the following (replace with your actual proxy credentials):

```toml
[proxy]
url = "http://username:password@proxy-host:port"
```

3. Free proxy services (less reliable): sslproxies.org, proxyscrape.com  
   Paid (reliable): Bright Data, Oxylabs, Smartproxy, WebShare

**Alternative: Use Screener Excel Mode**  
Download the Excel file manually from screener.in and upload it in the app.
"""
                )
        return None

    except requests.exceptions.Timeout:
        if show_status:
            st.warning(f"⏱️ Timeout ({timeout}s) while fetching {url}")
        return None

    except Exception as e:
        if show_status:
            st.error(f"❌ Unexpected error fetching {url}: {type(e).__name__}: {e}")
        return None


# ---------------------------------------------------------------------------
# Yahoo Finance proxy support
# ---------------------------------------------------------------------------
def get_yf_ticker(symbol: str, proxy_url: str | None = None):
    """
    Returns a yfinance Ticker object that routes all requests through a proxy.

    yfinance internally uses requests, and accepts a proxy dict on most
    data-fetching calls. This wrapper:
      - Reads proxy from st.secrets if not explicitly provided
      - Patches the underlying requests session so ALL yfinance calls
        (info, financials, balance_sheet, cashflow, history) go through the proxy
      - Falls back to a direct connection if no proxy is configured

    Args:
        symbol:    Ticker symbol e.g. "RELIANCE.NS"
        proxy_url: Optional proxy URL. If None, reads from st.secrets["proxy"]["url"].

    Returns:
        yfinance.Ticker object (proxy-patched if proxy is configured)

    Usage:
        from proxy_fetcher import get_yf_ticker
        ticker = get_yf_ticker("RELIANCE.NS")
        info = ticker.info
        financials = ticker.financials
    """
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError("yfinance is not installed. Run: pip install yfinance")

    resolved_proxy = proxy_url or _get_proxy_from_secrets()
    ticker = yf.Ticker(symbol)

    if resolved_proxy:
        # yfinance exposes a requests.Session on ticker._session (yfinance >= 0.2)
        # Patch it so all HTTP calls go through the proxy
        try:
            session = get_session(resolved_proxy)
            ticker._session = session
        except Exception:
            # If patching fails, fall back to default (direct) behaviour
            pass

    return ticker


def yf_download(
    tickers,
    proxy_url: str | None = None,
    **kwargs
):
    """
    Proxy-aware wrapper around yf.download().

    Args:
        tickers:   Ticker string or list of tickers.
        proxy_url: Optional proxy URL. Reads from st.secrets if not provided.
        **kwargs:  All other yf.download() kwargs (start, end, period, etc.)

    Returns:
        pandas DataFrame from yf.download()

    Usage:
        from proxy_fetcher import yf_download
        df = yf_download("RELIANCE.NS", start="2020-01-01", end="2024-01-01")
    """
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError("yfinance is not installed. Run: pip install yfinance")

    resolved_proxy = proxy_url or _get_proxy_from_secrets()

    if resolved_proxy:
        # yf.download accepts a proxy dict directly
        proxy_dict = {"https": resolved_proxy, "http": resolved_proxy}
        return yf.download(tickers, proxy=proxy_dict, **kwargs)
    else:
        return yf.download(tickers, **kwargs)


# ---------------------------------------------------------------------------
# Convenience: fetch multiple URLs and return first successful response
# ---------------------------------------------------------------------------
def fetch_first_successful(
    urls: list[str],
    proxy_url: str | None = None,
    headers: dict | None = None,
    timeout: int = 30,
    show_status: bool = True,
) -> requests.Response | None:
    """
    Tries each URL in order and returns the first successful (HTTP 200) response.

    Args:
        urls:       List of URLs to try in order.
        proxy_url:  Optional proxy URL.
        headers:    Extra headers.
        timeout:    Per-request timeout.
        show_status: Show st messages.

    Returns:
        First successful requests.Response, or None if all fail.
    """
    for url in urls:
        resp = fetch_url(
            url,
            proxy_url=proxy_url,
            headers=headers,
            timeout=timeout,
            show_status=show_status,
        )
        if resp is not None:
            return resp
    if show_status:
        st.error(f"❌ All {len(urls)} URLs failed.")
    return None
