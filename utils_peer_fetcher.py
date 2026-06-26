"""
Industry Peer Fetcher
=====================
Uses Yahoo Finance v6 recommendationsbysymbol API (JSON, no JS rendering needed).
Supports all exchanges: NSE (.NS), BSE (.BO), LSE (.L), SSE (.SS), HKEX (.HK),
NASDAQ/NYSE (bare tickers), and any other Yahoo Finance suffix.
NO HARDCODED PEERS
"""
import logging
import requests
import re

logger = logging.getLogger(__name__)

# Known Indian exchange suffixes — peers are returned bare by Yahoo and
# suffix is re-applied by the caller (PHASE5). Strip these from results.
_INDIAN_SUFFIXES = {'.NS', '.BO'}

# Known suffixes Yahoo Finance uses in recommendedSymbols responses.
# For non-Indian exchanges Yahoo typically returns the full ticker including suffix.
_KNOWN_SUFFIXES = {
    '.NS', '.BO',           # India
    '.L',                   # LSE
    '.SS', '.SZ',           # Shanghai / Shenzhen
    '.HK',                  # HKEX
    '.DE', '.F', '.XETRA',  # Germany
    '.PA', '.AS', '.MI',    # Europe
    '.TO', '.V',            # Canada
    '.AX',                  # Australia
    '.KS', '.KQ',           # Korea
    '.T',                   # Japan
    '.SW',                  # Switzerland
}


def _detect_suffix(ticker: str) -> str:
    """
    Detect the exchange suffix from a ticker string.
    Returns the suffix including the dot (e.g. '.NS', '.L', '.SS')
    or '' for bare tickers (NASDAQ/NYSE).
    """
    ticker = ticker.strip().upper()
    dot = ticker.rfind('.')
    if dot < 0:
        return ''
    candidate = ticker[dot:]   # e.g. '.NS', '.L', '.SS'
    # Accept any suffix that looks like a known one, or any short alphabetic suffix
    if re.match(r'\.[A-Z]{1,5}$', candidate):
        return candidate
    return ''


def get_peers_from_yahoo_comparison(ticker: str, max_peers: int = 20, exclude_self: bool = True):
    """
    Fetches peers via Yahoo Finance v6 recommendationsbysymbol API.
    Works for all exchanges — NSE/BSE/LSE/SSE/HKEX/NASDAQ/NYSE/Other.

    Returns:
        List of peer tickers as bare base symbols (suffix stripped).
        The caller (PHASE5) is responsible for re-appending the correct suffix.
    """
    ticker_upper = ticker.strip().upper()
    suffix = _detect_suffix(ticker_upper)
    ticker_base = ticker_upper[:-len(suffix)] if suffix else ticker_upper
    is_indian = suffix in _INDIAN_SUFFIXES

    # For bare tickers (NASDAQ/NYSE) use as-is; for others use full ticker with suffix
    full_ticker = ticker_upper  # already has suffix if any (e.g. SHEL.L, 600519.SS, RELIANCE.NS)
    peers = []

    try:
        url = f"https://query2.finance.yahoo.com/v6/finance/recommendationsbysymbol/{full_ticker}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
        }

        print(f"[PeerFetcher] Fetching: {url}")
        response = requests.get(url, headers=headers, timeout=15)

        if response.status_code != 200:
            print(f"[PeerFetcher] HTTP {response.status_code}")
            return []

        data = response.json()
        results = data.get('finance', {}).get('result', [])

        if not results:
            print(f"[PeerFetcher] Empty result set from API")
            return []

        for rec in results:
            for item in rec.get('recommendedSymbols', []):
                symbol = item.get('symbol', '').strip()
                if not symbol:
                    continue

                # Strip the suffix to get base ticker — caller re-applies the right suffix.
                # For Indian: strip .NS/.BO. For others: strip whatever suffix is present.
                peer_suffix = _detect_suffix(symbol)
                peer_base = symbol[:-len(peer_suffix)] if peer_suffix else symbol

                if not peer_base:
                    continue
                if exclude_self and peer_base == ticker_base:
                    continue
                if peer_base not in peers:
                    peers.append(peer_base)
                    print(f"   Found: {peer_base} (from {symbol})")

        result = peers[:max_peers]

        if result:
            print(f"\n[PeerFetcher] SUCCESS: Found {len(result)} peers for {full_ticker}")
            print(f"Peers: {', '.join(result)}")
            return result
        else:
            print(f"\n[PeerFetcher] No peers found")
            return []

    except Exception as e:
        print(f"[PeerFetcher] Error: {e}")
        return []


def get_industry_peers(ticker: str, max_peers: int = 20, exclude_self: bool = True):
    """
    Main entry point. Returns bare peer base tickers (no exchange suffix).
    The caller is responsible for appending the correct exchange suffix.
    """
    ticker_upper = ticker.strip().upper()

    print(f"\n{'='*70}")
    print(f"[PeerFetcher] Finding peers for {ticker_upper}")
    print('='*70)

    peers = get_peers_from_yahoo_comparison(ticker_upper, max_peers, exclude_self)

    if peers:
        print(f"\n{'='*70}")
        print(f"[PeerFetcher] FINAL RESULT: {len(peers)} peers found")
        print(f"\nPeer List:")
        for i, peer in enumerate(peers, 1):
            print(f"   {i:2d}. {peer}")
        print('='*70)
        return peers
    else:
        print(f"\n{'='*70}")
        print(f"[PeerFetcher] FAILED: No peers found for {ticker_upper}")
        print("="*70)
        return []


# Alias for compatibility
get_industry_peers_fast = get_industry_peers


if __name__ == "__main__":
    test_tickers = [
        "TATASTEEL.NS",   # NSE India
        "ADVAIT.BO",      # BSE India
        "GEV",            # NYSE
        "SHEL.L",         # LSE
        "600519.SS",      # Shanghai
    ]

    for ticker in test_tickers:
        peers = get_industry_peers(ticker, max_peers=10)
        print(f"\n{ticker}: {len(peers)} peers\n")
