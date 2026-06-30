#!/usr/bin/env python3
"""
Benchmark + verify: /entityOperations/upsert (chunks of 1000) vs /scorpio/v1/bulkinsert.

For each path it inserts the file, retrieves the entities back, and compares them
against the original input, then cleans up.

Usage:
    python bench_bulk.py <entities.ndjson> [base_url]
    base_url defaults to http://localhost:9090

Requires: pip install requests
Each line of the NDJSON file must be one NGSI-LD entity (with an "id").
"""
import itertools
import json
import sys
import time

import requests

CHUNK = 1000          # upsert / delete batch size
RETRIEVE_CHUNK = 200  # ids per ?id= query
BASE = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:9090"
NGSILD = BASE + "/ngsi-ld/v1"
UPSERT_URL = NGSILD + "/entityOperations/upsert"
DELETE_URL = NGSILD + "/entityOperations/delete"
ENTITIES_URL = NGSILD + "/entities"
BULK_URL = BASE + "/scorpio/v1/bulkinsert"

# keys that the broker adds/normalizes and that we don't expect to match the input
VOLATILE = {"@context", "createdAt", "modifiedAt", "createdat", "modifiedat"}


def read_entities(path):
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def chunked(iterable, size):
    it = iter(iterable)
    while True:
        batch = list(itertools.islice(it, size))
        if not batch:
            return
        yield batch


def post(url, payload):
    r = requests.post(url, json=payload, headers={"Content-Type": "application/json"})
    if r.status_code >= 400:
        print(f"  ! {url} -> {r.status_code}: {r.text[:300]}")
    return r


def test_upsert(path):
    n = 0
    t0 = time.perf_counter()
    for batch in chunked(read_entities(path), CHUNK):
        post(UPSERT_URL, batch)
        n += len(batch)
    return n, time.perf_counter() - t0


def delete_all(ids):
    t0 = time.perf_counter()
    for batch in chunked(ids, CHUNK):
        post(DELETE_URL, batch)
    return time.perf_counter() - t0


def test_bulk(path):
    t0 = time.perf_counter()
    with open(path, "rb") as f:
        r = requests.post(BULK_URL, data=f, headers={"Content-Type": "application/x-ndjson"})
    dt = time.perf_counter() - t0
    print(f"  bulk response {r.status_code}: {r.text[:300]}")
    return dt


def retrieve_all(ids):
    """Fetch entities back, batched by id. Returns {id: entity}."""
    out = {}
    for batch in chunked(ids, RETRIEVE_CHUNK):
        r = requests.get(ENTITIES_URL,
                         params={"id": ",".join(batch), "limit": len(batch)},
                         headers={"Accept": "application/json"})
        if r.status_code >= 400:
            print(f"  ! GET entities -> {r.status_code}: {r.text[:300]}")
            continue
        for e in r.json():
            out[e["id"]] = e
    return out


def normalize(o):
    """Drop broker-added keys and round floats so semantic equality survives the round-trip."""
    if isinstance(o, dict):
        return {k: normalize(v) for k, v in o.items() if k not in VOLATILE}
    if isinstance(o, list):
        return [normalize(x) for x in o]
    if isinstance(o, float):
        return round(o, 6)
    return o


def verify(name, by_id, ids):
    t0 = time.perf_counter()
    got = retrieve_all(ids)
    dt = time.perf_counter() - t0
    missing, mismatch = [], []
    for eid in ids:
        r = got.get(eid)
        if r is None:
            missing.append(eid)
        elif normalize(by_id[eid]) != normalize(r):
            mismatch.append(eid)
    ok = len(ids) - len(missing) - len(mismatch)
    print(f"  retrieved {len(got)}/{len(ids)} in {dt:.2f}s  ->  OK={ok}  missing={len(missing)}  mismatch={len(mismatch)}")
    for eid in mismatch[:3]:
        print(f"    MISMATCH {eid}\n      input: {json.dumps(normalize(by_id[eid]), sort_keys=True)}"
              f"\n      stored:{json.dumps(normalize(got[eid]), sort_keys=True)}")
    for eid in missing[:3]:
        print(f"    MISSING {eid}")
    return len(missing) == 0 and len(mismatch) == 0


def rate(n, dt):
    return f"{n / dt:,.0f}/s" if dt > 0 else "n/a"


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    path = sys.argv[1]
    by_id = {e["id"]: e for e in read_entities(path)}
    ids = list(by_id.keys())
    total = len(ids)
    print(f"file: {path}  ({total} entities)  base: {BASE}\n")

    print("=== Test 1: /entityOperations/upsert in chunks of 1000 ===")
    n, dt1 = test_upsert(path)
    print(f"upsert: {n} entities in {dt1:.2f}s  ({rate(n, dt1)})")
    ok1 = verify("upsert", by_id, ids)
    print("  cleanup:", end=" ")
    print(f"deleted {total} in {delete_all(ids):.2f}s\n")

    print("=== Test 2: /scorpio/v1/bulkinsert (whole file, one stream) ===")
    dt2 = test_bulk(path)
    print(f"bulk: {total} entities in {dt2:.2f}s  ({rate(total, dt2)})")
    ok2 = verify("bulk", by_id, ids)
    print("  cleanup:", end=" ")
    print(f"deleted {total} in {delete_all(ids):.2f}s\n")

    print("=== comparison ===")
    print(f"upsert (1000-chunks): {dt1:.2f}s  ({rate(total, dt1)})   verify: {'OK' if ok1 else 'FAILED'}")
    print(f"bulkinsert:           {dt2:.2f}s  ({rate(total, dt2)})   verify: {'OK' if ok2 else 'FAILED'}")
    if dt1 > 0 and dt2 > 0:
        if dt2 < dt1:
            print(f"=> bulkinsert is {dt1 / dt2:.1f}x faster")
        else:
            print(f"=> upsert is {dt2 / dt1:.1f}x faster")


if __name__ == "__main__":
    main()
