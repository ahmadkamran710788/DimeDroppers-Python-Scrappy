#!/usr/bin/env python
"""Show which GoFan / NFHS Network links MaxPreps actually publishes on a school page.

Diagnostic for the ``maxpreps_gofan_url`` / ``maxpreps_nfhs_url`` columns. Run it before
trusting those columns on a new run, or whenever they come back empty or suspiciously
uniform -- MaxPreps' Next.js payload shape is not contractual and has moved before.

For each URL it prints:

  * every gofan.co / nfhsnetwork.com URL found anywhere in ``__NEXT_DATA__``, with the
    JSON path it sits at, marked ACCEPT or reject by ``schoolinfo.PARTNERS``;
  * the same for URLs found only in the raw HTML;
  * what ``partner_links()`` would ultimately record.

Reading the output
------------------
Start at the ``partnerInfo`` block -- that is where MaxPreps publishes the school's own
links (``ticketingUrl`` / ``streamingUrl``) and it is what the scraper reads. Everything
after it is fallback and noise: a school page carries ~80 URLs on partner hosts, nearly
all of them ``schoolVideos[*]`` highlight clips of OTHER schools' games.

Three outcomes worth recognising:

* ``ticketingUrl``/``streamingUrl`` marked ACCEPT -- working as intended.
* marked ``rejected (not a school page)`` with a value like ``https://gofan.co/`` -- MaxPreps
  flags the school as a partner but links the partner's homepage. Correctly recorded as
  empty; a homepage identifies no school.
* marked rejected with a value that clearly DOES name a school -- a real gap: the accept
  pattern in ``maxpreps_scraper/schoolinfo.py`` needs widening. This is what hid every
  GoFan link once already (``/school/<id>`` vs ``/app/school/<id>``).

The summary also flags a URL that is identical across every school, which means site
chrome leaked past the filter.

    python scripts/probe_partner_links.py <school-url> [<school-url> ...]

Note MaxPreps geo-blocks non-US IPs (403 - Geo-block on every content page), so this has
to run from a machine that can reach it.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests  # noqa: E402  (after sys.path fix)

from maxpreps_scraper.nextdata import page_props  # noqa: E402
from maxpreps_scraper.schoolinfo import (  # noqa: E402
    _URL_RE, _match_partner, partner_links,
)

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")
HOSTS = ("gofan.co", "nfhsnetwork.com")


def walk(node, path="pageProps", depth=0):
    """Yield ``(json_path, url)`` for every http(s) string in a JSON-ish structure."""
    if depth > 12:
        return
    if isinstance(node, str):
        if node[:4].lower() == "http":
            yield path, node
    elif isinstance(node, dict):
        for k, v in node.items():
            yield from walk(v, f"{path}.{k}", depth + 1)
    elif isinstance(node, (list, tuple)):
        for i, v in enumerate(node):
            yield from walk(v, f"{path}[{i}]", depth + 1)


def interesting(url):
    return any(h in url.lower() for h in HOSTS)


def probe(url):
    print(f"\n{'=' * 78}\n{url}\n{'=' * 78}")
    try:
        resp = requests.get(url, headers={"User-Agent": UA}, timeout=30)
    except Exception as exc:  # noqa: BLE001 - diagnostic, report and continue
        print(f"  FETCH FAILED: {exc!r}")
        return None
    if resp.status_code != 200:
        print(f"  HTTP {resp.status_code}"
              + ("  <- geo-block: run this from a US IP" if resp.status_code == 403 else ""))
        return None

    pp = page_props(resp.text)
    if not pp:
        print("  no __NEXT_DATA__ / pageProps on this page")

    # The authoritative source. Everything below it is fallback/diagnostic noise.
    info = (pp.get("schoolContext") or {}).get("partnerInfo")
    print("\n  -- schoolContext.partnerInfo (authoritative) --")
    if info is None:
        print("    ABSENT -- MaxPreps moved the field; the scan below is what will be used")
    else:
        for k, v in info.items():
            note = ""
            if isinstance(v, str) and v:
                note = "  <- ACCEPT" if _match_partner(v) else "  <- rejected (not a school page)"
            print(f"    {k:22} = {v!r}{note}")

    print("\n  -- every partner URL in __NEXT_DATA__ --")
    in_blob = set()
    for jpath, found in walk(pp):
        if not interesting(found):
            continue
        in_blob.add(found)
        verdict = _match_partner(found)
        print(f"    {'ACCEPT ' + verdict if verdict else 'reject ':>12}  {jpath}\n"
              f"                  {found}")
    if not in_blob:
        print("    (none)")

    print("\n  -- in raw HTML only --")
    html_only = {u for u in _URL_RE.findall(resp.text) if interesting(u)} - in_blob
    for found in sorted(html_only):
        verdict = _match_partner(found)
        print(f"    {'ACCEPT ' + verdict if verdict else 'reject ':>12}  {found}")
    if not html_only:
        print("    (none)")

    result = partner_links(pp, resp.text)
    print(f"\n  -- recorded --\n    maxpreps_gofan_url = {result['gofan'] or '(empty)'}"
          f"\n    maxpreps_nfhs_url  = {result['nfhs'] or '(empty)'}")
    return result


def main():
    urls = sys.argv[1:]
    if not urls:
        print("usage: python scripts/probe_partner_links.py <school-url> [...]",
              file=sys.stderr)
        raise SystemExit(2)

    results = [r for r in (probe(u) for u in urls) if r]

    print(f"\n{'=' * 78}\nSUMMARY over {len(results)} page(s) fetched\n{'=' * 78}")
    for key in ("gofan", "nfhs"):
        values = [r[key] for r in results if r[key]]
        distinct = set(values)
        print(f"  {key:6}: {len(values)}/{len(results)} pages, {len(distinct)} distinct")
        if len(values) > 1 and len(distinct) == 1:
            print(f"    WARNING: identical on every page -- this is site chrome, not the\n"
                  f"    school's own link. Tighten PARTNERS['{key}']['accept'] in\n"
                  f"    maxpreps_scraper/schoolinfo.py so it is rejected.\n"
                  f"    value: {values[0]}")


if __name__ == "__main__":
    main()
