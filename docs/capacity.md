# Ad-cache capacity C=1000 vs C=5000 in the network model

This study asks what the registrar cache capacity C does at network scale:
the honest cost of C=1000 against C=5000, and how much of a sybil advertiser
flood reaches honest searchers at each capacity. The single-registrar study
(`onur-sim/compare`, RESULTS.md) has one registrar and no network; here every
node advertises through the registration walk and searches through the fork
searcher, on the 25k crawl mix.

## Setup

- Admission: topdisc 703d5a2 (`topictable.go`, `ticket.go`). `searchsim/go_registrar.py`
  is a byte-identical copy of `sim-admission/admissionsim/registrar.py`
  (a test pins its sha256 to the upstream file). `searchsim/admission.py` adds
  `GoTable`, copied from `onur-sim/compare/compare.py` (scaleBase, the G term
  1e-7, ticket key rotation), and `WorldTable`, which keeps a per-topic view of
  the ads for the world model. Advertisers send their ticket back and the
  registrar credits it as Go does: on time from the ticket's first issue, late
  only the scheduled wait, expired nothing.
- IP addresses: `--honest-nets crawl` gives every honest node a real crawl /24.
  A template copied more than once keeps the top 12 bits of the address (all
  the IP tree reads at 5000 ads or fewer) and gets distinct low bits, so a
  copy does not pile ten times the nodes onto one /24. Hosts number 1 to 254
  within a /24. Topics map to crawl chains by population rank, not by name.
- Advertisers: `--depth 18 --reg-bucket-size 10` as in Go (18 buckets, 10
  active and 20 standby each). A registrar dropped for a quote over 1.5
  lifetimes (single quote or running total) stays out for one ad lifetime, as
  Go's overloaded set does. The next request follows `Registration.Update`:
  one due attempt per bucket, attempts holding a ticket first, then the
  earliest due, buckets taken in turn from the one after the last request.
- Sybil advertisers (`--flood`): ids below zero that register only. They never
  answer a query and never enter a DHT table, aux reply or search table. Each
  sybil seeds its registration table from a DHT-shaped table around its own
  random id and refills from aux like an honest node. `target` floods the
  largest topic with `--flood-ratio` sybils per honest advertiser. Two cache
  occupation modes use `--flood-topics` T fake topics whose hashes share the
  target's top 24 bits (`--flood-spread` for uniform hashes):
  - `occupy`: every sybil registers every fake topic, S x T registration
    chains. This is the stronger attack: each fake topic has S advertisers and
    the attacker offers 300 times the distinct ads and sending concurrency.
  - `occupy-partitioned`: chain c is sybil c mod S registering fake topic
    c mod T, max(S, T) chains. Same chain count and message volume as the
    target flood; each fake topic has about S/T advertisers, so its topic
    term stays near zero. Read it as occupation at the target flood's volume,
    not as 300 topics per sybil.
  They are separate workloads; do not extrapolate from one to the other.
  Besides volume they differ in addresses: with the /24 pool indexed by sybil
  number, a partitioned fake topic gets its ads from one /24 and a full one
  from all of them, so the IP term differs per topic. Full S x T is costly:
  at rho = 1 it runs 164k chains at 2k nodes, 854k at 10k and about 2.1M at
  25k. The 10k gate reached 8.6 GB with 8 % of chains started and about 20 s
  of wall time per simulated second; only 2k fits in memory (projected 12 to
  16 GB, 7 to 11 h per run). In the matrix each size other than 25k
  has its own no-flood baselines; the S x T size also gets partitioned
  occupy runs, and the rho = 4 size only seed 1 (as its attack rows).
  Sybil addresses come from `--flood-nets` /24s (0 = one address each).
  Sybils wait out every quote unless `--flood-give-up`.
- Rate budget (`--flood-budget R`, REGTOPIC/s over all sybils): a chain due
  to send without a token is parked whole, not with a chosen attempt. A parked
  chain whose best attempt holds a ticket still inside its window goes first,
  in order of window start (ticket.last + ticket.wait); chains with only fresh
  attempts get the tokens left over, and move to the retry queue when one of
  their tickets comes due. A ticket past its window (start + 15 s) earns no
  credit, so it counts as a fresh attempt both in the queue and in the
  sybil's choice of attempt. A budgeted sybil sends its earliest valid
  ticket from any bucket, else the attempt due longest; it owes Go no send
  order. A chain parked as fresh is re-examined when its next ticket comes
  due. The matrix sets R = 2.5 x
  `regtopic_rate.honest_target` of the matching no-flood run (honest REGTOPIC/s
  of the target topic over the search phase).
- Metrics added to `summary.json`: `samples` (per-registrar snapshots every
  `--sample-period` s on the grid search start + j x period, saved as
  `samples.npz`, with search-phase averages), three hot sets of 20 registrars
  (`hot_load`: most ads held, `hot_target`: most target-topic ads, `hot_set`:
  most target-topic queries; the query-ranked set does not overlap the other
  two), `served` (query-weighted sybil share of what searchers were handed),
  `giveups`, `fanout_buckets`, `reg_stats`, `regtopic_rate` (over the exact
  search phase, from counter snapshots at its start and end),
  `ticket_timings` (registrar side, both classes) and
  `ticket_timings_advertiser` (honest and sybil apart), `target_search`,
  `flood`, honest-only t50/t90/t99 and `eclipsed16` per class. Every older key
  is kept.
- Censoring: `honest_tNN` and `admission_s.median/p90` are medians over the
  searchers or advertisers that got there. `honest_reachedNN_frac` and
  `admission_s.admitted_frac` give the share that did, and `honest_tNN_all`
  and `admission_s.median_all` count the rest as never (null when the median
  is never). Registrations and buckets per advertiser count every registrant,
  zero for one without a live ad. The aggregator marks seeds with no value.

## Deviations from Go

- One REGTOPIC in flight per advertiser and topic (Go: 4). This slows how fast
  buckets fill, for honest nodes and sybils alike; an unbudgeted attacker sends
  at most chains/rtt.
- Constant 60 ms RTT, no loss, no DHT churn.
- Tickets are trusted simulator objects (no HMAC) with Go's timing, credit and
  key rotation rules.
- Searchers never stop (Go: the caller closes the iterator).
- A registrar decides at the instant the advertiser sends.
- Registration.Update expires a registered attempt only while it is at the
  top of the heap ahead of any due waiting attempt; the model expires all due
  ones before choosing. The choice among waiting attempts is Go's.
- The attacker has no closest-registrar oracle and shares the honest walk: the
  results are a lower bound on attacker power.

## Rerun

PY=python3   # with requirements.txt installed
PY=~/workspace/datahop/sim-sybil/.venv/bin/python
COMMON="--nodes 25000 --topics 100 --model models/crawl-2026-09-18.json --fork-faithful \
  --honest-nets crawl --depth 18 --reg-bucket-size 10 --search-frac 0.08 --search-time 1800"
$PY -m searchsim.world $COMMON --cache 1000 --seed 1 --out runs/capacity/h-c1000-s1
$PY -m searchsim.world $COMMON --cache 1000 --seed 1 --flood target --flood-ratio 1 \
  --flood-nets 4 --out runs/capacity/t-c1000-s1
$PY -m searchsim.world $COMMON --cache 1000 --seed 1 --flood occupy --flood-topics 300 \
  --flood-nets 4 --out runs/capacity/o-c1000-s1
$PY -m searchsim.aggregate --matrix runs/capacity/*/ --out RESULTS.md   # matrix runs only, each once
```

The full matrix with baselines, calibration and resume:
`$PY runs/capacity/matrix.py --rho4-nodes N [--occupy-nodes M --occupy-gb G]`
lists the runs; add `--launch` to run them (under setsid nohup).

Tests: `$PY -m pytest tests -q`.
