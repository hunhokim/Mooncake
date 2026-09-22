#!/usr/bin/env python3
"""Check the metrics scraped by bench_te_metrics.sh against the bench's own report."""

import json
import re
import sys

out, batch, block = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
rc = 0


def report(label, ok, detail):
    global rc
    rc |= not ok
    print(f"  {'ok  ' if ok else 'FAIL'} {label:<20} {detail}")


def ints(d, pre=""):
    for k, v in d.items():
        yield from ints(v, pre + k + ".") if isinstance(v, dict) else [(pre + k, v)]


for op in ("read", "write"):
    m = json.load(open(f"{out}/{op}.json"))
    log = open(f"{out}/{op}.log").read()
    batches = int(re.search(r"batch count (\d+)", log).group(1))
    want = batches * batch
    other = "write" if op == "read" else "read"

    def g(k):
        return m[f"mooncake_te_{op}_{k}"]

    print(f"\n{op}: {re.search(r'Test completed.*', log).group(0)}")
    for k, v in ints(m):
        if v:
            print(f"       {k} = {v}")
    report(
        "requests_total",
        g("requests_total") == want,
        f"{g('requests_total')} == {batches} x {batch} = {want}",
    )
    # The bench polls every task to completion, so every task is observed and
    # requests/bytes must match its own batch count exactly.
    got, exp = g("bytes_total"), want * block
    report(
        "bytes_total",
        got == exp,
        f"{got} == {batches} x {batch} x {block} = {exp}",
    )
    report("failures_total", g("failures_total") == 0, g("failures_total"))
    report(
        "histogram counts",
        g("latency_us")["count"] == g("size_bytes")["count"] == want,
        f"{g('latency_us')['count']}/{g('size_bytes')['count']} == "
        f"{batches} x {batch} = {want}",
    )
    report(
        f"{other} side idle",
        m[f"mooncake_te_{other}_requests_total"] == 0,
        m[f"mooncake_te_{other}_requests_total"],
    )

nonzero = [k for k, v in ints(json.load(open(f"{out}/target.json"))) if v]
print("\ntarget (passive, records nothing)")
report("all counters zero", not nonzero, f"nonzero: {nonzero}" if nonzero else "")
sys.exit(rc)
