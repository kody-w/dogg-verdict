#!/usr/bin/env python3
"""A federated tick-network node: this repo's own append-only chain, keyed to the
global tick spine at kody-w/dogg.

Every run reads the spine's current tick anchor, takes this node's themed snapshot of
keyless public APIs, and appends one frame referencing that tick. Different repos, run
by different people, each with their own outlook — all joinable on the tick key. To
start your own node: fork this repo, edit THEME/STREAM/SOURCES below, enable the
scheduled workflow. Frames verify with the reference implementation (tools/rapp.py,
from kody-w/rapp-1); CI re-verifies the whole chain on every push.
"""
import json, sys, pathlib, datetime, urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import rapp as R
import chainio

ROOT = pathlib.Path(__file__).resolve().parent.parent
SPINE_HEAD = "https://raw.githubusercontent.com/kody-w/dogg/main/ticks/HEAD.json"
TIMEOUT = 8

# ---- edit these three for your node -------------------------------------------------
THEME = "verdict"                     # also the data directory name
STREAM = "verdict:@kody-w/dogg-verdict"                   # your stream id (your repo, your name)
# SOURCES: name -> zero-arg callable returning a SMALL dict of facts.
# rapp/1 canonical hashing forbids floats: numeric facts ride as strings or ints.
# -------------------------------------------------------------------------------------

def utc():
    n = datetime.datetime.now(datetime.timezone.utc)
    return n.strftime("%Y-%m-%dT%H:%M:%S.") + f"{n.microsecond // 1000:03d}Z"

def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": f"tick-node-{THEME}"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        return json.loads(r.read().decode())

# This node is DERIVED: its facts are judgments computed from another doggcast's
# verified history (the world dimension), each traceable to the frames it read.
API = "https://raw.githubusercontent.com/kody-w/dogg-api/main/api/series"

def _series(name):
    return get(f"{API}/{name}.json")["rows"]

def _zlast(vals):
    """z-score of the newest value vs its own history (needs >= 8 points)."""
    if len(vals) < 8:
        return None
    hist, x = vals[:-1], vals[-1]
    mu = sum(hist) / len(hist)
    var = sum((v - mu) ** 2 for v in hist) / len(hist)
    sd = var ** 0.5
    return None if sd == 0 else (x - mu) / sd

def _judgment():
    checks = {}
    try:
        checks["btc_usd"] = _zlast([float(r["spot"]) for r in _series("btc_usd")])
    except Exception: checks["btc_usd"] = None
    try:
        checks["btc_fees"] = _zlast([float(r["fastest_sat_vb"]) for r in _series("btc_fees")])
    except Exception: checks["btc_fees"] = None
    try:
        checks["quakes"] = _zlast([float(r["count"]) for r in _series("earthquakes_past_hour")])
    except Exception: checks["quakes"] = None
    try:
        checks["mempool"] = _zlast([float(r["tx_count"]) for r in _series("btc_mempool")])
    except Exception: checks["mempool"] = None
    scored = {k: f"{v:.2f}" for k, v in checks.items() if v is not None}
    anomalies = sorted((k for k, v in checks.items() if v is not None and abs(v) >= 2.0),
                       key=lambda k: -abs(checks[k]))
    calm = all(abs(v) < 1.0 for v in checks.values() if v is not None)
    n = sum(1 for v in checks.values() if v is not None)
    return {"z_scores": scored, "anomalies": anomalies,
            "quiet_index": ("calm" if calm and n >= 2 else
                            "active" if anomalies else
                            "normal" if n >= 2 else "insufficient-history"),
            "series_evaluated": n,
            "method": "z-score of newest value vs own full history, threshold 2.0",
            "audit": "inputs are dogg-api series rows; every row carries its source frame hash"}

SOURCES = {"verdict": _judgment}

def load_chain(d):
    return chainio.load_chain(d)

def main():
    spine = get(SPINE_HEAD)
    tick_n, tick_hash = spine["count"] - 1, spine["head_frame"]
    d = ROOT / THEME
    d.mkdir(exist_ok=True)
    chain = load_chain(d)
    head = chain[-1] if chain else None
    if head is not None and head["payload"].get("tick") == tick_n:
        print(f"{THEME}: tick {tick_n} already recorded — nothing to do")
        return
    data, failed = {}, []
    for name, fn in SOURCES.items():
        try:
            data[name] = fn()
        except Exception:
            failed.append(name)
    payload = {"tick": tick_n, "tick_frame": tick_hash, "spine": "kody-w/dogg",
               "fetched_utc": utc(), THEME: data, "sources_failed": failed}
    if head is None:
        payload["about"] = (f"A federated node of the global tick network: this repo's "
                            f"own {THEME} outlook, one frame per observed tick, keyed to "
                            "the spine's tick anchors so it joins every other node's "
                            "data on the same clock.")
    f = R.build_frame(f"{THEME}.snapshot", STREAM, (head["seq"] + 1) if head else 0,
                      utc(), payload, prev=(head["payload_hash"] if head else None))
    ok, step, why = R.verify_frame(f, head=head, stream_id_of_record=STREAM)
    if not ok:
        raise ValueError(f"refusing invalid frame: {step}: {why}")
    chainio.append_frame(d, f, STREAM)
    print(f"{THEME} frame {f['seq']} @ spine tick {tick_n}: {', '.join(data) or 'nothing'}"
          + (f" (failed: {', '.join(failed)})" if failed else ""))

if __name__ == "__main__":
    main()
