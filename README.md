# dogg-verdict — the first DERIVED doggcast: judgment, not observation

Every half hour this node reads the world dimension's verified history (via
[dogg-api](https://github.com/kody-w/dogg-api)) and casts a JUDGMENT frame: per-series
anomaly z-scores, named anomalies (|z| >= 2), and a **quiet index** — is the world calm,
normal, or active right now?

Why it matters: raw doggcasts record what public APIs said; this one demonstrates the
layer above — verifiable derived conclusions. Every judgment is auditable: the inputs
are series rows that each carry their source frame hash, the method is stated in the
frame, and the judgment itself is an append-only rapp/1 chain. Conclusions with
provenance, cast on the same clock as the data they judge.

Fork it: swap `_judgment()` for your own analysis — your model's view of the world
becomes a subscribable, unforgeable doggcast. Verify: `python3 tools/verify_thread.py`.
