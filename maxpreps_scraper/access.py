"""Knowing -- quickly and honestly -- whether MaxPreps is serving this machine.

Why this module exists
----------------------
MaxPreps answers some networks with its own ``403 Forbidden`` page (observed from a
non-US machine; the same pages load through a US connection). A crawl that hits that
wall used to retry every URL, give up, and finish **"done" with empty CSVs** -- hours
later, with nothing in the job status to say why. Three pieces fix that:

* ``MaxPrepsBlockBreaker`` (downloader middleware) -- watches the raw status of every
  MaxPreps response, *before* RetryMiddleware hides it. When most of the recent ones
  are 403/429 it stops the crawl gracefully (partial data kept, resumable) and, if
  the caller asked for one, drops a ``blocked.json`` marker so the worker can fail the
  job with a clear message instead of reporting an empty success. It runs inside the
  crawl, on the crawl's own HTTP client, so it can't disagree with what the crawl sees.
* ``MaxPrepsHeadersMiddleware`` -- attaches an optional header (``MAXPREPS_AUTH_HEADER``,
  e.g. a token issued by MaxPreps) to **maxpreps.com requests only**. The enrichment
  steps also talk to GoFan / NFHS; a MaxPreps credential must never ride along there.
* ``probe()`` / ``cached_status()`` -- a one-request check used by ``GET /health``. It is
  **advisory only**: it uses ``urllib`` rather than the crawl's client, so it is never
  used to refuse a job, only to show whether this server looks allowed.

None of this changes what a request looks like: with no env vars set, the User-Agent,
headers and crawl rate are byte-identical to before (see ``settings.py``).

No circumvention lives here, by design: no browser impersonation, no rotating proxies.
If MaxPreps refuses a network, the answer is a network it accepts or an arrangement
with MaxPreps -- not disguising the crawler.
"""
import json
import os
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from urllib.parse import urlsplit

# A stable, robots-allowed school page. Any school page works; this one is the
# Humphreys ABLE Charter page the CSV-seed feature was specified against.
PROBE_URL = "https://www.maxpreps.com/ca/stockton/humphreys-able-charter-legacy/"

# Statuses that mean "MaxPreps is refusing us", as opposed to a missing page (404) or a
# transient server error (5xx). 429 is included: being rate-limited across most of a
# window means we are too fast for them, and the polite response is to stop.
BLOCK_STATUSES = (401, 403, 429)


def is_maxpreps_host(url):
    host = (urlsplit(url).hostname or "").lower()
    return host == "maxpreps.com" or host.endswith(".maxpreps.com")


def parse_header(raw):
    """``"Name: value"`` -> ``("Name", "value")``; ``None`` for empty/malformed input."""
    if not raw or ":" not in raw:
        return None
    name, value = raw.split(":", 1)
    name, value = name.strip(), value.strip()
    return (name, value) if name and value else None


def blocked_message(status):
    if status == 429:
        return ("MaxPreps is rate-limiting this server (HTTP 429). Partial data is kept; "
                "resume later or lower the crawl rate (MAXPREPS_DOWNLOAD_DELAY).")
    return (f"MaxPreps refused access (HTTP {status}) from this server. MaxPreps blocks "
            "some networks (e.g. non-US); run from a network it accepts, or arrange "
            "access with MaxPreps.")


# --------------------------------------------------------------------------- #
# Downloader middlewares
# --------------------------------------------------------------------------- #
class MaxPrepsHeadersMiddleware:
    """Add ``MAXPREPS_AUTH_HEADER`` to maxpreps.com requests only. No-op when unset."""

    def __init__(self, header):
        self.header = header

    @classmethod
    def from_crawler(cls, crawler):
        return cls(parse_header(crawler.settings.get("MAXPREPS_AUTH_HEADER", "")))

    def process_request(self, request, spider):
        if self.header and is_maxpreps_host(request.url):
            request.headers.setdefault(self.header[0], self.header[1])
        return None


class MaxPrepsBlockBreaker:
    """Stop the crawl when MaxPreps is refusing most requests.

    Sits just *below* RetryMiddleware (550) in the response chain, so it sees every raw
    response -- including the 403s that RetryMiddleware would otherwise quietly retry
    eight times per URL. Only maxpreps.com responses count; robots.txt (always 200) is
    excluded so it can't dilute the window.

    Trips when at least ``ratio`` of the last ``window`` responses are blocking
    statuses. A handful of individually-forbidden pages can never do that; a network
    block does it within the first ``window`` responses (well under a minute).
    """

    def __init__(self, crawler, window, ratio, marker):
        self.crawler = crawler
        self.window = max(1, window)
        self.ratio = ratio
        self.marker = marker
        self.recent = deque(maxlen=self.window)
        self.tripped = False
        # Whole-run tallies, for crawls too small to ever fill the window (a one-state
        # /scrape starts from ONE directory page: 5 raw responses with retries).
        self.total = 0
        self.blocked_total = {}

    @classmethod
    def from_crawler(cls, crawler):
        from scrapy import signals

        s = crawler.settings
        mw = cls(
            crawler,
            window=s.getint("MAXPREPS_BLOCK_WINDOW", 50),
            ratio=s.getfloat("MAXPREPS_BLOCK_RATIO", 0.8),
            marker=s.get("MAXPREPS_BLOCK_MARKER") or None,
        )
        crawler.signals.connect(mw.spider_closed, signal=signals.spider_closed)
        return mw

    def process_response(self, request, response, spider):
        if self.tripped or not is_maxpreps_host(response.url):
            return response
        if urlsplit(response.url).path == "/robots.txt":
            return response
        self.recent.append(response.status)
        self.total += 1
        if response.status in BLOCK_STATUSES:
            self.blocked_total[response.status] = self.blocked_total.get(response.status, 0) + 1
        if len(self.recent) < self.window:
            return response
        blocked = [s for s in self.recent if s in BLOCK_STATUSES]
        if len(blocked) >= self.ratio * self.window:
            self._trip(spider, max(set(blocked), key=blocked.count))
        return response

    def spider_closed(self, spider, reason):
        """A run that ended without tripping, but in which MaxPreps refused EVERY page it
        served, was blocked just the same -- it was only too small to fill the window."""
        if self.tripped or not self.total:
            return
        if sum(self.blocked_total.values()) == self.total:
            status = max(self.blocked_total, key=self.blocked_total.get)
            spider.logger.error("MaxPreps refused all %d responses this run (%s): %s",
                                self.total, status, blocked_message(status))
            self._write_marker(spider, status)

    def _trip(self, spider, status):
        self.tripped = True
        spider.logger.error("MaxPreps block detected (%d of last %d responses were %s): %s",
                            sum(1 for s in self.recent if s in BLOCK_STATUSES),
                            self.window, status, blocked_message(status))
        self._write_marker(spider, status)
        self.crawler.engine.close_spider(spider, "maxpreps_blocked")

    def _write_marker(self, spider, status):
        if not self.marker:
            return
        try:
            os.makedirs(os.path.dirname(self.marker) or ".", exist_ok=True)
            tmp = self.marker + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump({"status": status, "message": blocked_message(status),
                           "at": time.time(), "window": list(self.recent),
                           "responses": self.total}, fh)
            os.replace(tmp, self.marker)
        except OSError as exc:
            spider.logger.warning("could not write block marker %s: %r", self.marker, exc)


def read_block_marker(path):
    """The ``blocked.json`` written by the breaker, or ``None`` if the crawl wasn't blocked."""
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


# --------------------------------------------------------------------------- #
# Advisory probe (GET /health)
# --------------------------------------------------------------------------- #
def _identity():
    """The same User-Agent / headers the crawl sends (read from project settings)."""
    from . import settings as s  # local import: settings reads env at import time

    headers = dict(getattr(s, "DEFAULT_REQUEST_HEADERS", {}) or {})
    headers["User-Agent"] = s.USER_AGENT
    extra = parse_header(getattr(s, "MAXPREPS_AUTH_HEADER", ""))
    if extra:
        headers[extra[0]] = extra[1]
    return headers


def probe(url=PROBE_URL, timeout=15):
    """One GET to a MaxPreps school page. Returns a small status dict, never raises.

    ``status`` is ``"ok"`` (2xx/3xx/404 -- MaxPreps is answering us), ``"blocked"``
    (401/403/429) or ``"error"`` (network trouble / 5xx -- says nothing either way).
    """
    result = {"status": "error", "http_status": None, "url": url,
              "checked_at": time.time(), "detail": ""}
    req = urllib.request.Request(url, headers=_identity())
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            result["http_status"] = resp.status
    except urllib.error.HTTPError as exc:
        result["http_status"] = exc.code
    except Exception as exc:  # noqa: BLE001 - a probe must never raise
        result["detail"] = f"{type(exc).__name__}: {exc}"[:300]
        return result

    code = result["http_status"]
    if code in BLOCK_STATUSES:
        result["status"] = "blocked"
        result["detail"] = blocked_message(code)
    elif code is not None and (code < 500):
        result["status"] = "ok"
    else:
        result["detail"] = f"HTTP {code}"
    return result


_cache = {"result": None, "refreshing": False}
_cache_lock = threading.Lock()


def cached_status(max_age=900):
    """Last probe result, refreshed in the background when older than ``max_age``.

    Never blocks the caller (``/health`` is Render's liveness check and must stay fast):
    the first call returns ``{"status": "unknown"}`` and kicks off a probe.
    """
    with _cache_lock:
        result = _cache["result"]
        stale = result is None or (time.time() - result["checked_at"]) > max_age
        if stale and not _cache["refreshing"]:
            _cache["refreshing"] = True
            threading.Thread(target=_refresh, daemon=True).start()
    return result or {"status": "unknown", "detail": "first check in progress"}


def _refresh():
    try:
        result = probe()
    finally:
        with _cache_lock:
            _cache["refreshing"] = False
    with _cache_lock:
        _cache["result"] = result


def fresh_status():
    """Synchronous probe that also updates the cache (``GET /health/maxpreps``)."""
    result = probe()
    with _cache_lock:
        _cache["result"] = result
    return result
