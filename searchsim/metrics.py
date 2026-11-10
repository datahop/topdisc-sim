"""Study-only observations; never feed back into protocol decisions.

The original part-1 artifacts retain their schema for attack-none runs.
search_metrics.json carries the per-searcher observations for all runs;
metrics.json aggregates each searcher's statistic with equal weight.
"""
import numpy as np


def stats(values):
    values = [float(v) for v in values if v is not None]
    if not values:
        return dict(mean=None, p90=None, n=0)
    return dict(mean=float(np.mean(values)), p90=float(np.percentile(values, 90)), n=len(values))


def study_row(s, duration, depth):
    is_sybil = s.w.is_sybil
    order = s.order
    first = {}
    for n in (16, 80):
        prefix = order[:n]
        first[str(n)] = dict(delivered=len(prefix), complete=len(prefix) == n,
                             sybil_share=sum(is_sybil(r) for r, *_ in prefix) / len(prefix) if prefix else None)
    windows = []
    for start in range(0, int(np.ceil(duration / 900)) * 900, 900):
        rows = [row for row in order if start <= row[2] < start + 900]
        sybil = sum(is_sybil(r) for r, *_ in rows)
        windows.append(dict(start=start, end=min(start + 900, duration), results=len(rows),
                            sybil_share=sybil / len(rows) if rows else None, honest_found=len(rows) - sybil))
    passes = [dict(p) for p in s.study_passes]
    if s.st is not None:
        passes.append(dict(start=s.pass_start - s.t0, end=duration, queries=s.pass_q,
                           sybil_queries=s.pass_sybil_q, honest_queries=s.pass_q - s.pass_sybil_q,
                           timeouts=s.pass_f, complete=False, start_bucket=s.active, end_bucket=s.st.active))
    for p in passes:
        p['sybil_query_share'] = p['sybil_queries'] / p['queries'] if p['queries'] else None
        p['length_s'] = p['end'] - p['start']
    completed = [p for p in passes if p['complete']]
    zero = [p for p in completed if p['queries'] and not p['honest_queries']]
    # A finite-horizon suffix is observable; permanence beyond the horizon
    # is not. Report its start and support, without calling it permanent.
    suffix = []
    for p in reversed(completed):
        if not p['queries'] or p['honest_queries']:
            break
        suffix.append(p)
    trace = s.active_trace
    deepest = depth - 1
    dwell = 0.0
    for (t, b), (end, _) in zip(trace, trace[1:] + [(duration, None)]):
        if b == deepest:
            dwell += max(0.0, min(end, duration) - t)
    first_mix = dict(s.first_mix) if s.first_mix else None
    if first_mix:
        first_mix['sybil_share'] = first_mix['sybil'] / first_mix['results']
    return dict(searcher=s.node, duration=duration, first=first, windows=windows, passes=passes,
                passes_with_zero_honest_queries=len(zero),
                zero_honest_pass_share=len(zero) / len(completed) if completed else None,
                zero_honest_suffix_start=suffix[-1]['start'] if suffix else None,
                zero_honest_suffix_passes=len(suffix),
                active_trace=trace, active_max=max((b for _, b in trace), default=0),
                time_to_deepest=next((t for t, b in trace if b == deepest), None),
                deepest_dwell_s=dwell, deepest_dwell_share=dwell / duration if duration else 0,
                first_honest=s.first_honest, first_mix=first_mix,
                sybil_query_share=s.sybil_queries / s.queries if s.queries else None,
                pass_sybil_query_share=stats([p['sybil_query_share'] for p in completed])['mean'],
                pass_queries=stats([p['queries'] for p in completed])['mean'],
                pass_length_s=stats([p['length_s'] for p in completed])['mean'],
                timeouts_per_pass=stats([p['timeouts'] for p in completed])['mean'])


def study_summary(rows, attack, nets, w1, m2=False, m3=False, w1p=False):
    keys = ('passes_with_zero_honest_queries', 'zero_honest_pass_share', 'zero_honest_suffix_start',
            'zero_honest_suffix_passes', 'active_max', 'time_to_deepest', 'deepest_dwell_s',
            'deepest_dwell_share', 'first_honest', 'sybil_query_share', 'pass_sybil_query_share',
            'pass_queries', 'pass_length_s', 'timeouts_per_pass')
    out = dict(attack=attack, attack_nets=nets, w1=w1, m2=m2, m3=m3, searchers=len(rows),
               aggregation='equal weight per searcher; pass statistics use completed passes only',
               metrics={key: stats([row[key] for row in rows]) for key in keys})
    if w1p:
        out["w1p"] = True
    for n in ('16', '80'):
        out['metrics']['first' + n + '_sybil_share'] = stats([r['first'][n]['sybil_share'] for r in rows])
        out['metrics']['first' + n + '_complete'] = stats([r['first'][n]['complete'] for r in rows])
    out['metrics']['first_mix_sybil_share'] = stats([r['first_mix']['sybil_share'] for r in rows if r['first_mix']])
    out['metrics']['first_mix_time'] = stats([r['first_mix']['time'] for r in rows if r['first_mix']])
    out['windows'] = []
    for j in range(max((len(r['windows']) for r in rows), default=0)):
        windows = [r['windows'][j] for r in rows if j < len(r['windows'])]
        out['windows'].append(dict(start=j * 900, sybil_share=stats([w['sybil_share'] for w in windows]),
                                   honest_found=stats([w['honest_found'] for w in windows])))
    out['passes_with_zero_honest_queries'] = sum(r['passes_with_zero_honest_queries'] for r in rows)
    out['searchers_reached_deepest'] = sum(r['time_to_deepest'] is not None for r in rows)
    out['searchers_without_honest_result'] = sum(r['first_honest'] is None for r in rows)
    return out
