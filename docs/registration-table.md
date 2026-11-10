# Registration table: K_register and depth

This study asks how many registrars an advertiser keeps per bucket (`K_register`,
`--reg-bucket-size`) and how many buckets its registration table has (`--depth`), with the
admission rules of go-ethereum and an ad cache of `C = 1,000`.

## Setup

- `K_register` sweep: 50,000 nodes, the 100-topic crawl mix (`models/crawl-2026-09-18.json`,
  crawl /24s with `--honest-nets crawl`), 8 % of nodes searching for 1,800 s, no attackers,
  two seeds. `K_register` 3, 5, 10 and 16 at depth 18; depth 10 and 14 at `K_register` 10.
- Rare topics: 50,000 nodes, 1,000 topics of Zipf-distributed size (exponent 1.0), 16 % of
  nodes searching for 1,800 s, `K_register` 10, one seed, depth 18 and 10.
- Both use the fork-faithful search with a search bucket limit of 2 per /24, 10 registrars per
  /24 in one search and mixing over 10 registrars.

Registrars per advertiser is the median number of registrars holding one advertiser's
advertisements. Admission wait is the median time from first contact to admission. t90 is the
median time until a searcher has found 90 % of its topic's providers.

## K_register

Mean of two seeds, depth 18:

| `K_register` | Registrars per advertiser | REGTOPIC received per registrar, median / 99th percentile | Median admission wait | t90, 1,000+ topics | Lowest recall |
|---:|---:|---:|---:|---:|---:|
| 3  | 24 | 140 / 4,976  | 0.06 s | 396 s | 99.5 % |
| 5  | 36 | 242 / 7,865  | 0.18 s | 328 s | 99.9 % |
| 10 | 60 | 512 / 15,313 | 1.2 s  | 246 s | 100 % |
| 16 | 82 | 844 / 22,696 | 4.3 s  | 232 s | 100 % |

From 10 to 16, t90 improves by 6 % while the REGTOPIC load per registrar grows by about 65 %.

## Depth

With the crawl mix, `K_register` 10, mean of two seeds:

| Depth | REGTOPIC received by the busiest registrar | t90, 1,000+ topics |
|---:|---:|---:|
| 18 | 97,880 | 246 s |
| 14 | 97,401 | 246 s |
| 10 | 66,758 | 247 s |

With 1,000 Zipf-sized topics, one seed:

| Depth | REGTOPIC received by the busiest registrar | t90, fewer than 10 providers | t90, 10 to 99 providers | t90, 1,000+ |
|---:|---:|---:|---:|---:|
| 18 | 44,314 | 0.96 s | 0.96 s | 59.7 s |
| 10 | 25,652 | 23.9 s | 4.7 s  | 60.4 s |

Recall is 100 % in every class at both depths. A smaller depth merges the registrars nearest
the topic into one bucket, which lowers the peak load but makes rare topics slower to find.

## Rerun

```sh
PY=python3   # with requirements.txt installed
COMMON="--cache 1000 --nodes 50000 --topics 100 --model models/crawl-2026-09-18.json \
  --fork-faithful --honest-nets crawl --search-frac 0.08 --search-time 1800 \
  --search-bucket-ip-limit 2 --w1p --w1p-limit 10 --fork-mix-registrars 10"
$PY -m searchsim.world $COMMON --reg-bucket-size 10 --depth 18 --seed 1 --out runs/regtable/k10-d18-s1/
RARE="--cache 1000 --nodes 50000 --topics 1000 --zipf 1.0 --fork-faithful --search-frac 0.16 \
  --search-time 1800 --search-bucket-ip-limit 2 --w1p --w1p-limit 10 --fork-mix-registrars 10 \
  --seed 1 --reg-bucket-size 10"
$PY -m searchsim.world $RARE --depth 10 --out runs/regtable/rare-d10/
```
