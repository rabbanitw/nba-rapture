"""HTTP client for api.pbpstats.com, built around how it actually fails.

Measured behaviour (2026-07, from this repo's container):

  * It sheds load with a fast 503 -- the error comes back in ~0.05s, not as a hang.
    16 requests at concurrency 16 -> 14x 503.  At concurrency 4 -> 10x 503.
    Even strictly serial with a 1s gap, ~50% of get-wowy-stats calls 503.
  * Those 503s are transient. Retrying the identical request clears them: a 10-call
    sample reached 10/10 success at 2.2 HTTP calls per success.
  * 502s from its nginx also appear sporadically.

So the winning strategy is low concurrency plus patient retries, not parallelism.
wowy_scrape.py's asyncio.Semaphore(50) is why that scrape was painful. The old
retry loop also doubled its delay to 600s, which turns a blip into a ten-minute
stall; here the backoff is capped low because the failures are cheap and transient.

Only stdlib is used -- this container has no `requests`.
"""

import http.client
import json
import random
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = "https://api.pbpstats.com/"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# Retry these; anything else is a real error and should surface.
RETRY_STATUS = {429, 500, 502, 503, 504}

MAX_ATTEMPTS = 40
BASE_DELAY = 0.4
MAX_DELAY = 20.0
TIMEOUT = 90

# Everything pbpstats has been observed to do short of answering. IncompleteRead is
# the interesting one: it hangs up part way through a large body, and because it
# derives from HTTPException and ValueError rather than OSError, an except clause
# built around connection errors misses it and the whole run dies mid-cell.
TRANSIENT = (urllib.error.URLError, http.client.HTTPException, socket.timeout,
             TimeoutError, ConnectionError, json.JSONDecodeError, OSError)

_stats_lock = threading.Lock()
_stats = {"calls": 0, "retries": 0, "failures": 0}


def stats():
    with _stats_lock:
        return dict(_stats)


def _bump(key, n=1):
    with _stats_lock:
        _stats[key] += n


def get_json(path, params, max_attempts=MAX_ATTEMPTS):
    """GET BASE+path?params and return parsed JSON, retrying transient failures.

    Raises RuntimeError only after max_attempts consecutive transient failures, or
    immediately on a non-retryable status (a 400 means the params are wrong and no
    amount of waiting fixes it).
    """
    url = f"{BASE}{path}?{urllib.parse.urlencode(params)}"
    last = None
    for attempt in range(1, max_attempts + 1):
        req = urllib.request.Request(url, headers={"User-Agent": UA,
                                                   "Accept": "application/json"})
        try:
            _bump("calls")
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            last = f"HTTP {e.code}"
            if e.code not in RETRY_STATUS:
                _bump("failures")
                raise RuntimeError(f"{last} (not retryable) for {url}") from e
        except TRANSIENT as e:
            last = f"{type(e).__name__}: {e}"

        if attempt < max_attempts:
            _bump("retries")
            # Full jitter: pbpstats sheds load in bursts, and a fleet of retries
            # marching in lockstep just recreates the burst that caused the 503.
            delay = min(BASE_DELAY * (2 ** min(attempt - 1, 6)), MAX_DELAY)
            time.sleep(random.uniform(0, delay))

    _bump("failures")
    raise RuntimeError(f"gave up after {max_attempts} attempts ({last}) for {url}")
