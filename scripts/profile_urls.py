"""
Profile page load times for a sequence of URLs.
Captures: navigation timing, all network requests, console errors.
Specifically surfaces buckaroo-related JS timing via performance marks/measures.
"""

import time

from playwright.sync_api import sync_playwright

URLS = [
    "http://localhost:7860/first-project/catalog/6fdd4dad1447",
    "http://localhost:7860/first-project/diff/route_distance/1/2",
    "http://localhost:7860/first-project/catalog/763193211746",
    "http://localhost:7860/first-project/catalog/0d0b75dfa627",
]

# How long to wait after DOMContentLoaded for async renders (buckaroo data fetch + render)
SETTLE_MS = 8000


def fmt_ms(ms):
    if ms is None:
        return "  N/A  "
    return f"{ms:7.0f}ms"


def profile_url(page, url, index):
    print(f"\n{'='*70}")
    print(f"[{index+1}/{len(URLS)}] {url}")
    print(f"{'='*70}")

    requests = []
    console_errors = []

    def on_request(req):
        requests.append({"url": req.url, "method": req.method, "start": time.monotonic()})

    def on_response(resp):
        for r in reversed(requests):
            if r["url"] == resp.url and "end" not in r:
                r["end"] = time.monotonic()
                r["status"] = resp.status
                r["size"] = resp.headers.get("content-length", "?")
                break

    def on_console(msg):
        if msg.type == "error":
            console_errors.append(msg.text)

    page.on("request", on_request)
    page.on("response", on_response)
    page.on("console", on_console)

    t_nav_start = time.monotonic()
    page.goto(url, wait_until="domcontentloaded")
    t_dom = time.monotonic()

    # Wait for the page to settle (network idle or timeout)
    try:
        page.wait_for_load_state("networkidle", timeout=SETTLE_MS)
    except Exception:
        pass
    t_idle = time.monotonic()

    # Grab Navigation Timing from the browser
    nav_timing = page.evaluate("""() => {
        const e = performance.getEntriesByType('navigation')[0];
        if (!e) return null;
        return {
            dns: e.domainLookupEnd - e.domainLookupStart,
            tcp: e.connectEnd - e.connectStart,
            ttfb: e.responseStart - e.requestStart,
            response_download: e.responseEnd - e.responseStart,
            dom_interactive: e.domInteractive,
            dom_complete: e.domComplete,
            load_event: e.loadEventEnd,
        };
    }""")

    # Grab all resource entries (JS, API calls)
    resources = page.evaluate("""() => {
        return performance.getEntriesByType('resource').map(e => ({
            name: e.name,
            type: e.initiatorType,
            duration: e.duration,
            transfer_size: e.transferSize,
            start: e.startTime,
        }));
    }""")

    # Any perf marks/measures (buckaroo may emit these)
    marks_measures = page.evaluate("""() => {
        const marks = performance.getEntriesByType('mark').map(e => ({kind:'mark', name:e.name, start:e.startTime}));
        const measures = performance.getEntriesByType('measure').map(
            e => ({kind:'measure', name:e.name, start:e.startTime, duration:e.duration}));
        return [...marks, ...measures].sort((a,b) => a.start - b.start);
    }""")

    # --- Print Navigation Timing ---
    if nav_timing:
        print("\n  Navigation Timing:")
        print(f"    DNS lookup:        {fmt_ms(nav_timing['dns'])}")
        print(f"    TCP connect:       {fmt_ms(nav_timing['tcp'])}")
        print(f"    TTFB:              {fmt_ms(nav_timing['ttfb'])}")
        print(f"    Response download: {fmt_ms(nav_timing['response_download'])}")
        print(f"    DOM interactive:   {fmt_ms(nav_timing['dom_interactive'])}")
        print(f"    DOM complete:      {fmt_ms(nav_timing['dom_complete'])}")
        print(f"    Load event end:    {fmt_ms(nav_timing['load_event'])}")

    wall_to_dom  = (t_dom  - t_nav_start) * 1000
    wall_to_idle = (t_idle - t_nav_start) * 1000
    print("\n  Wall-clock:")
    print(f"    To DOMContentLoaded: {fmt_ms(wall_to_dom)}")
    print(f"    To network idle:     {fmt_ms(wall_to_idle)}")

    # --- API requests ---
    api_reqs = [r for r in requests if "/api/" in r["url"] or r["url"].endswith(".json")]
    if api_reqs:
        print(f"\n  API requests ({len(api_reqs)}):")
        for r in sorted(api_reqs, key=lambda x: x["start"]):
            dur = (r.get("end", time.monotonic()) - r["start"]) * 1000
            path = r["url"].split("localhost:7860")[-1]
            status = r.get("status", "?")
            print(f"    [{status}] {fmt_ms(dur)}  {path}")

    # --- Slowest JS/resource loads ---
    js_resources = [r for r in resources if r["type"] in ("script", "fetch", "xmlhttprequest")]
    if js_resources:
        slow = sorted(js_resources, key=lambda x: -x["duration"])[:8]
        print("\n  Slowest resources (script/fetch, top 8):")
        for r in slow:
            name = r["name"].split("localhost:7860")[-1]
            kb = f"{r['transfer_size']/1024:.1f}kB" if r["transfer_size"] else "cached"
            print(f"    {fmt_ms(r['duration'])}  [{kb:>10}]  {name}")

    # --- Performance marks/measures (buckaroo + app) ---
    if marks_measures:
        print(f"\n  Perf marks/measures ({len(marks_measures)}):")
        for m in marks_measures:
            if m["kind"] == "measure":
                print(f"    MEASURE  {fmt_ms(m['duration'])}  @{m['start']:.0f}ms  {m['name']}")
            else:
                print(f"    mark                   @{m['start']:.0f}ms  {m['name']}")
    else:
        print("\n  No perf marks/measures found.")

    if console_errors:
        print(f"\n  Console errors ({len(console_errors)}):")
        for e in console_errors[:5]:
            print(f"    ERROR: {e[:120]}")

    page.remove_listener("request", on_request)
    page.remove_listener("response", on_response)
    page.remove_listener("console", on_console)

    return {
        "url": url,
        "wall_to_dom_ms": wall_to_dom,
        "wall_to_idle_ms": wall_to_idle,
        "api_count": len(api_reqs),
    }


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            # disable cache so we get real cold-load numbers per page
            # (but keep warm across the session so JS bundle only loads once)
        )
        page = context.new_page()

        results = []
        for i, url in enumerate(URLS):
            result = profile_url(page, url, i)
            results.append(result)
            time.sleep(0.5)

        print(f"\n{'='*70}")
        print("SUMMARY")
        print(f"{'='*70}")
        print(f"  {'URL':<45} {'To DOM':>9} {'To Idle':>9} {'API#':>5}")
        print(f"  {'-'*45} {'-'*9} {'-'*9} {'-'*5}")
        for r in results:
            path = r["url"].split("localhost:7860")[-1]
            print(
                f"  {path:<45} {fmt_ms(r['wall_to_dom_ms']):>9} "
                f"{fmt_ms(r['wall_to_idle_ms']):>9} {r['api_count']:>5}"
            )

        browser.close()


if __name__ == "__main__":
    main()
