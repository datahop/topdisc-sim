# topdisc-searchsim

A Python model of TopDisc (datahop/go-ethereum, branch topdisc) for
populations the simnet testbed cannot afford. Two entry points:

- `searchsim.world`: every node registers its topic through the registration
  walk (ported from `topicindex/registration.go` and `v5_topic.go`), serves as
  registrar with the admission rule of `topicindex/topictable.go`, and searches
  its topic with the searcher of `topicindex/search.go` (default and adaptive
  distance), all interleaved on one model clock.
- `searchsim.run`: lookups only, against a steady-state ad placement, with a
  sample of searchers per topic; minutes at 100k.

```
pip install -r requirements.txt
python3 -m searchsim.world --nodes 10000 --topics 100 \
    --model models/crawl-2026-09-18.json --floor 4 --target 16 --out runs/w10k
python3 -m searchsim.run --nodes 100000 --topics 100 \
    --model models/crawl-2026-09-18.json --floor 4 --target 16 --out runs/r100k
```

`--floor 0` runs the default searcher, `--floor 4` the adaptive distance;
`--target 16` stops a search at 16 registrants as a connection-driven node
does, `--target 0` searches continuously. The crawl model in `models/` (topic
shares and churn fits from the 24 h discv5 crawl of 2026-09-18, made by
`cmd/crawl/model.py` in datahop/topdisc-testbed) gives the topic sizes.

## What is modelled

- **IDs**: N random 64-bit IDs, sorted. Every "nodes at log-distance d from
  X" set is one slice of the array, so a random node in any subtree is a
  bisect and a draw; nothing is materialised per node.
- **DHT tables**: as discv5, 17 buckets by log-distance to the node, 16
  random nodes per bucket where the population allows, generated on demand
  from a per-node seed. This is what the aux path draws from: a registrar
  returns one node per requested distance from its own table, first 8
  distances only, exactly as `collectTopicAuxNodes` does.
- **Topics**: shares from the crawl model (the k-1 largest chains, the k-th
  the folded tail) or Zipf; every node has exactly one topic, registers it
  and searches it (`--register-frac` lowers the registering share for
  experiments; the crawl-mix runs before 26 September used 0.5).
- **Ads** (`run` only): every registrant holds `copies` (5) ads in each of
  `depth` (10) rings around the topic hash, on registrars drawn uniformly
  within the ring, up to the registrar's cache (5000): the mixed steady state
  of the walk. `world` simulates the walk instead: the 10-bucket registration
  table with 5 active and 20 standby per bucket, seeded from the node's DHT
  table and refilled from the aux nodes in REGTOPIC replies, tickets and
  re-sends at the quoted time, the 1.5-lifetime attempt budget, expiry and
  re-placement; registrars admit by cache occupancy, topic share, the
  per-topic lower bound and the 1 s floor (the IP term is zero).
- **DHT tables** are filled with a weight by spawn order, (spawn + 20)^-0.5,
  fitted on the 10k simnet runs, where the search load served falls 20x from
  the first ten nodes to the last half: early nodes sit in far more tables.
  `--no-spawn-bias` gives uniform tables.
- **Searches**: a sample of searchers per topic runs the real search logic;
  a reply carries up to 16 random ads of the topic held by the registrar plus
  the aux nodes. Continuous searches run to a time horizon or a query cap;
  `--target 16` stops a search at 16 distinct registrants, which is what a
  connection-driven searcher does once its outbound slots are full (topics
  smaller than the target never stop, as in the runs).
- **Load**: each query is charged to its registrar with the weight
  (searchers of the topic / sampled searchers), so the per-registrar
  distribution is exact up to sampling noise. Time is queries times `--rtt`
  plus the 2 s minimum pass gap.

## Not modelled

Bytes, handshakes and NODES traffic; churn and ad expiry (all ads are
present for the whole run); the registration walk and admission control;
IP limits and the admission IP score; the connection slot refill after a
peer leaves. The 15 min ad lifetime only drives the adaptive asked filter.

## Outputs

`summary.json` (registrar queries and REGTOPIC median/p99/max, ads held, fan-out, admission and quoted waits for `world`; per size class the
median queries per search, passes, queries and time to 50/90/99 % of the
topic, the settled bucket, and load by the registrar's bucket to its nearest
topic), `searches.json` (one row per sampled search), `load.png`, and the
arrays needed to draw ID-space figures.

## Validation

Event-driven model at 10k, 100 crawl topics, connection-driven search for 1 h,
against the simnet runs of 2026-09-21 (4 h, so per-hour where it matters):
fan-out 30 to 38 registrars per registrant (measured 36), ads held per
registrar consistent with the fan-out, queries per search by topic size
matched including the small-topic re-walkers (51k per hour, measured 46k),
the adaptive settled buckets within one of the measured ones, registrar
load median within 2x and p99 within 2x. The default's extreme maximum stays
about 7x under the measured 715k queries over 4 h: the spawn-order table
weight reproduces the direction and about a quarter of the measured gradient,
so the busiest nodes are under-loaded. Quoted waits are lower than measured
because the IP term is not modelled (the measured runs had it maxed by their
address scheme).


Against the 10k simnet runs of 2026-09-21 (100 crawl topics, 4 h,
connection-driven): registrar p99 and max relative to the median, queries
per search by topic size, and the settled bucket per class. Numbers the
model reproduces there can be trusted at 100k and 1M; numbers it does not
are the model's assumptions showing, the uniform placement first.
