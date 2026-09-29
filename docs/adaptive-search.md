# Default and adaptive search in the model

The topic search as implemented has a load problem. One rule in the searcher fixes it. This page describes both and shows what the Python model of TopDisc measures at 30 000, 100 000 and 250 000 nodes. No testbed can run those sizes.

| | |
|---|---|
| model | datahop/topdisc-sim, `searchsim.world` |
| sizes | 30k, 100k, 250k, and 100 crawl topics at 30k |
| busiest registrar over the median | 47 to 3 at 30k, 850 to 14 at 250k |
| recall in 30 min at 100k, model only | 74 % to 96 % |

## 1. The problem

TopDisc has two halves. Only one of them regulates its own load. Registration does. A registrar quotes every advertiser a waiting time. The wait grows with the fill level of the cache and with the share the topic already takes. The registrars near a popular topic's hash receive every advertiser, so they slow admissions down. The ads then spread out to ten distances from the hash. Ads exist everywhere in the ID space, denser with each step closer to the hash. A searcher can find its topic from anywhere.

The search has no such mechanism. A registrar answers every query it receives. There is no waiting time and no refusal. Where searchers choose to ask is the only thing that decides how the query load falls on registrars. The search as implemented chooses badly.

The search table sorts registrars into ten buckets by distance to the hash. Bucket 0 is the farthest half of the ID space. Bucket 9 is the neighbourhood of the hash. The searcher opens one more bucket per round, farthest first. Then it picks any unasked node of any open bucket with equal probability. So each open bucket gets the same number of queries, whatever its population. Bucket 0 holds half of all registrars. Bucket 9 holds a few hundred. The same number of queries spread over 50 000 registrars is nothing. Spread over 200 registrars it is everything. When a pass runs out of unasked nodes, the next pass rebuilds the table from the same routing table. It asks the same seed nodes again, two seconds later, for as long as the node searches.

![Registrars per bucket, queries the default searcher sent to each bucket, and queries per registrar in each bucket, 100k nodes](figures/adaptive-search/sim-buckets-default-100k.png)

*The default searcher at 100 000 nodes, one topic, 30 minutes. Left: the registrars in each search-table bucket. Middle: the queries the searchers sent to each bucket. Right: the queries per registrar in each bucket.*

The middle panel shows the search rule at work. The default sends about 4.7 million queries to bucket 0. It sends about the same to buckets 1 and 2, which hold 25 000 and 12 700 registrars. The count tapers only in the closest buckets. The right panel shows the result per registrar. A node in the far bucket answers 95 queries in the half hour. A node at the hash answers 5 400, 57 times more, for no reason other than its ID. Admission control would push back at buckets 8 and 9. The search never does. That is the hotspot. At 30 000 nodes the busiest registrar answers 47 times the median. At 250 000 nodes it answers 850 times the median.

## 2. The solution

Keep the table, the messages and the registrars as they are. Give the searcher the one thing it lacks, a sense of where it is worth asking. **Go where replies still carry ads. Never ask the same node twice within an ad lifetime.**

![Ads per registrar by bucket with the three zones of the rule: full reply, stay, move closer](figures/adaptive-search/sim-rule.png)

*What the rule reads. The bars are the ads of one topic a registrar holds at each distance, from the model at 100 000 nodes. Registration places the ads, so the bars are the same under either searcher. A reply carries at most 16 ads. The rule reads a reply against the two thresholds. Above 16 the reply is full and the search steps one bucket farther out. Between 4 and 15 the search stays. Below 4 it moves closer.*

The search keeps an *active bucket*. It reads every reply as a measurement of ad density at that distance:

```
on every reply from a node in bucket b, carrying n ads:
    window[b] = last 4 values of n seen from bucket b
    if len(window[b]) >= 2:                  # one reply never moves the search
        m = median(window[b])
        if   m == 16: active = b - 1         # full reply: denser than a reply can show, step farther out
        elif m >= 4:  active = b             # dense enough: stay
        elif m >= 1:  active = b + ceil(log2(4 / m))   # density doubles per bucket: as many steps as it takes to reach 4
        else:         active = b + 4         # nothing here: the largest step allowed
    asked.add(node) for one ad lifetime      # never re-ask it before its ads could have changed
    file the aux nodes of the reply into their buckets

to choose the next node to query:
    for distance d = 0, 1, 2, ...:           # active bucket first, then its neighbours, farther side first
        for bucket in (active - d, active + d):
            if bucket has a node not in asked: return it
    if a spare already-asked node was offered this pass: ask it once, for its aux nodes only
    end the pass; next pass starts one bucket closer, where the nodes are different ones
```

A jump moves at most four buckets and stays inside the table. Every query lists the distances the searcher wants refilled. The registrar attaches one node per distance from its routing table. That is the only change on the registrar side. It is how the walk keeps finding new nodes at the distance where it settled. The wire format does not change. The rule is off unless the yield floor is set. Old and new nodes interoperate.

The effect is that a search settles where the ads of its topic reach the floor. For a large topic the far registrars already hold several ads each. The search stays in the outer buckets, and the load spreads over half the ID space. For a small topic the far replies are empty. The search jumps inward until replies carry four or more ads. It settles close to the hash, where the few ads of that topic are.

![Bucket where each adaptive search settled, by topic size class, 30 000 nodes and 100 crawl topics](figures/adaptive-search/sim-settled.png)

*The bucket where each adaptive search settled, by the size of its topic, in the 30 000-node run with the 100 crawl topics of section 7. Topics of a thousand members or more settle in buckets 1 to 4, far from the hash. Topics under a hundred members settle in buckets 5 to 9, close to it.*

Searches on the largest topics settle in bucket 2 most often, and never past bucket 6. Searches on topics of 100 to 900 members settle in buckets 5 and 6. Searches on topics under a hundred members settle in buckets 4 to 9. The size of the topic decides how far from the hash its searchers stay, and so how many registrars share their load.

## 3. The model and the scenarios

The Python model ports the registration walk, the admission rule with its lower bounds, and both searchers onto one event loop. The population is a sorted array of random IDs. The model generates DHT tables on demand. It weights them so that early-started nodes sit in more tables, as real routing tables do. The model stores nothing per node. That is why it runs 250 000 nodes in half an hour on one machine.

We checked the model against the 10 000-node simnet runs with the crawl topic mix. It reproduces the fan-out, 30 to 38 registrars per registrant against 36 measured. It reproduces the queries per search for each topic size. It places the adaptive searcher's settled bucket within one bucket of the measured one. It puts the registrar load median and p99 within a factor of two. It does not reproduce the extreme maximum of the default searcher. That value stays about seven times under the measured one. The maximum comes from which nodes sit in everyone's routing tables, and the model's weighting captures about a quarter of the real bias. The maxima of the default searcher below are therefore lower bounds. Read everything else as measured. The validation covers load and queries per search. It does not cover time to recall, and on that the testbed and the model disagree at small sizes. Section 6 explains.

### The scenarios

Four scenarios, each run twice with the same seed: once with the default searcher and once with the rule. The only difference between the two runs of a pair is the yield floor, 0 for the default and 4 for the rule. Three scenarios have one topic and grow the network. The fourth puts the topic mix of the crawl on 30 000 nodes.

| scenario | nodes | topics | who registers | who searches | lookup model | search phase |
|---|---|---|---|---|---|---|
| single topic, 30k | 30 000 | 1 | every node | 885 nodes, 3 % | continuous | 30 min |
| single topic, 100k | 100 000 | 1 | every node | 1 019 nodes, 1 % | continuous | 30 min |
| single topic, 250k | 250 000 | 1 | every node | 966 nodes, 0.4 % | continuous | 30 min |
| crawl mix, 30k | 30 000 | 100, sizes from the crawl of 2026-09-18 | half of the nodes, one topic each | every node, its own topic | until 16 distinct members are found | 60 min |

Every run has the same timeline. Nodes start over the first 15 minutes. Registration then runs alone for 22.5 minutes, so the ads are in place before the first search. The search phase starts after that and lasts 30 or 60 minutes. The searchers start staggered, 0.1 s apart in the single-topic runs and 0.03 s apart in the mix. A continuous searcher never stops. It keeps its search open for the whole phase, as a node with free peer slots would. A connection-driven searcher stops when it holds 16 distinct members of its topic.

The protocol parameters are the ones of the geth implementation. The search table has 10 buckets of 16 nodes. A registrar caches 5 000 ads and quotes waiting times from the same admission rule with its lower bounds. A reply carries at most 16 topic nodes and 8 aux nodes. An ad lives 15 minutes. Every query costs 60 ms of round trip. Routing tables use the spawn-order weighting described above and no bootnodes. The rule runs with a yield floor of 4 and an aux radius of 0, the recommended values.

The single-topic runs sample the searchers, so the registrar loads in sections 4 and 5 are per sampled run. They scale with the number of searchers. The ratios between the two searchers, and between buckets, do not depend on the sample. In the mix every node searches, so the loads of section 7 are the loads of the whole network.

## 4. Evaluation: registrar load

![CDF of queries received per registrar, default and adaptive, at 30k, 100k and 250k](figures/adaptive-search/sim-load-cdf.png)

*Queries received per registrar over 30 minutes, one topic, about a thousand continuous searchers, at three sizes. The axis is logarithmic.*

| nodes | queries per registrar, median / p99 / max, default | same, adaptive | max ÷ median | total queries |
|---|---|---|---|---|
| 30 000 | 438 / 7 495 / 20 495 | 547 / 1 054 / 1 753 | 47 → 3.2 | 25.9 M → 16.9 M |
| 100 000 | 68 / 2 684 / 14 896 | 283 / 781 / 1 818 | 219 → 6.4 | 29.7 M → 29.5 M |
| 250 000 | 11 / 1 215 / 9 335 | 107 / 381 / 1 511 | 850 → 14 | 28.2 M → 28.2 M |

The searchers send the same number of queries in both cases. The rule spreads them instead of concentrating them. The adaptive distribution is narrow at every size. Its p99 is within twice the median and its maximum within four times. The median of the default falls with size, because most registrars barely see the searchers. Its maximum stays in the tens of thousands. The same handful of nodes near the hash answers every searcher, however large the network. At 250 000 nodes one registrar answers 850 times the median with the default and 14 times with the rule. That residual comes from the routing-table bias, not from the search.

## 5. Evaluation: where the load goes

![Queries received per registrar against its position in the ID space, default and adaptive, 100k nodes](figures/adaptive-search/sim-idspace-100k.png)

*Search load by position in the ID space, 100 000 nodes, one topic, both searchers on one linear axis. The registrars are grouped in 250 bins of 400, and each line is the mean load of a bin. The dashed line is the topic hash. With the default, 73 % of the registrars in the far half are never asked. Its load doubles with each step of the table toward the hash and peaks at 4 300 per registrar next to the hash. The adaptive load is flat across the space, about 350 per registrar, and slightly lighter next to the hash.*

The default searcher opens the farthest bucket first. That bucket is half the ID space and holds sixteen candidates at a time. So the searcher never reaches most of its registrars, and everyone reaches the few in the closest buckets. The adaptive searcher asks where ads are dense enough to be worth a query. It refills its table with new nodes at that distance. So every registrar of a popular topic gets a share. The ones at the hash get slightly less, because their full replies push the search one step out.

## 6. Evaluation: what searchers get

![CDF over searchers of the share of the topic found in 30 minutes and of the time to half of it, at three sizes](figures/adaptive-search/sim-searchers.png)

*Top: the share of the topic each searcher found in 30 minutes. Bottom: the time to reach half of the topic. Both searchers make the same 29 000 queries per searcher in the 30 minutes.*

| nodes | found in 30 min, median, default / adaptive | time to half, median | time to 90 % |
|---|---|---|---|
| 30 000 | 96.5 % / 100 % | 201 s / 126 s | 1 017 s / 348 s |
| 100 000 | 74 % / 96 % | 788 s / 418 s | never / 1 230 s |
| 250 000 | 49 % / 69 % | 1 653 s, 41 % got there / 1 050 s, all did | never / never |

This section is a prediction of the model, not a measured result. The testbed runs on real nodes point the other way at small sizes. At 858 nodes on AWS the rule reached 99 % of the topic in 33 s against 20 s for the default. At 5 000 nodes in simnet it was 1.8 times slower to 99 %. At those sizes one pass of the default covers a large share of the network. The registrars near the hash hold every ad. So the default gets everything fast. The rule pays for its walk and gains nothing.

The model says the order flips when the topic outgrows a pass. With the same query budget the adaptive searcher finds more, sooner, and the gap widens with size. The default asks its seed nodes again every pass and gets back ads it already has. The adaptive searcher keeps walking new nodes, and at these sizes new nodes still carry new ads. We checked the model on load and on queries per search, not on time to recall. Treat the 30k column with caution, since it is closest to the sizes where the testbed measured the opposite. The 100k and 250k columns rest on the pass mechanism, which the model reproduces. The adaptive curves at 100k and 250k have two steps. That shape comes from the staggered starts of the searchers. The half that started earlier had longer to walk.

## 7. Evaluation: the crawl mix

![Registrar load, queries per searcher by topic size and time to 99 percent by topic size, default and adaptive, crawl mix](figures/adaptive-search/sim-mix-30k.png)

*Every node registers one topic drawn with the sizes of the crawl and searches it until its 16 slots are full. Left: queries per registrar. Middle: the queries a searcher makes in the hour, by the size of its topic. Right: the time to 99 % of the topic, for the sizes where a searcher can reach it.*

| measure | default | adaptive |
|---|---|---|
| queries per registrar, median / p99 / max | 1 276 / 4 746 / 15 020 | 791 / 1 326 / 2 033 |
| queries per searcher in 1 h, topics of 1000+ / 100–999 / 10–99 / under 10 | 6 / 23 / 463 / 52 548 | 6 / 10 / 244 / 29 106 |
| time to 99 % of the topic, median, topics of 10–99 / under 10 | 307 s / 213 s | 125 s / 86 s |
| searchers of topics under 10 that found all of it | 94 % | 100 % |

On the mix the busiest registrar comes down seven times and the p99 3.6 times. Large-topic searchers spend the same six queries to fill their slots in both cases. The gain on the searcher side is on small topics. A node on a topic of thirty members finds it in less than half the time with half the queries. The search jumps to the distance where the ads of that topic are, instead of walking in from the far end. Nodes on topics with fewer than sixteen members never fill their slots and search all hour. They make 52 000 queries each with the default and 29 000 with the rule. They are the top of the registrar load in every mixed run. That is the re-walk loop. A back-off between passes that found nothing new cuts that loop eight times for the default. It cuts the loop by a third to a half for the adaptive searcher. Recall does not change. We measured the back-off separately.

## 8. Conclusion

- The hotspot is a property of the search, not of registration. The default searcher gives every open bucket the same share and asks its seed nodes again every pass. So everyone asks the registrars near the hash.

- One rule removes it. Move the active bucket by the ad counts that replies carry. Do not ask a node again within an ad lifetime. The busiest registrar over the median falls from 47 to 3 at 30k and from 850 to 14 at 250k, with the same total queries.

- The rule costs search time at small sizes. Measured on real nodes it was 1.6 to 1.8 times slower to 99 % at 858 and 5 000 nodes. The model predicts a gain once a topic outgrows a pass of the default. It gives 96 % instead of 74 % of a 100k topic in 30 minutes. That prediction is not checked against real nodes.

- Recommended: yield floor 4, aux radius 0, the rest unchanged. The floor is insensitive between 2 and 12. The residual skew is the routing-table bias. The remaining load lever is the pass back-off for small topics.

## Sources

| what | where |
|---|---|
| model | datahop/topdisc-sim, `searchsim.world`. The README has the validation |
| runs | London, topdisc-sim/runs. sweep/default and floor4 (30k). single100k-cont-f0 and -f4. s250k/default and floor4. sweep2/default and floor4 (mix) |
| figures | drawn from each run's summary.json, searches.json, ids.npy, tq_rcv.npy and held.npy |
| the rule in the fork, and the testbed and AWS measurements referred to | datahop/go-ethereum, branch topicsearch-adaptive-distance, docs/topic-search-adaptive-distance.md |
