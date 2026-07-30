# Aftertake close-boundary replay: V7 invalidated, V8 CLOB-only candidate

## Decision

Do not use the original V7 replay for promotion. It treated the epoch suffix in
`btc-updown-5m-<epoch>` as the round end, but the suffix is the round start.
The reported 25 signals, 80% direction hit rate, and 132.431 ms median were
therefore measured after market open rather than after market close.

The replay code now derives `(round_start, round_end)` centrally and uses
`round_end = round_start + 300`. A regression test exercises the actual replay
boundary seam.

## Corrected 100-market result

The corrected run uses the same 100 cached BTC five-minute PMData markets and a
970 ms submit-latency assumption. It can be reproduced with
`scripts/replay_aftertake_profiles.py`.

| Profile | Signals | Direction hit | Median decision | Marketable / correct |
|---|---:|---:|---:|---:|
| V6.7 current 50/100/3 | 2 | 1/2 | 716.166 ms | 1 / 0 |
| V7 event, vacuum>=3 | 7 | 6/7 | 91.554 ms | 3 / 2 |
| V7 event, vacuum>=2 | 10 | 9/10 | 106.891 ms | 3 / 2 |
| V8 refill guard, <=250 ms | 6 | 6/6 | 86.141 ms | 2 / 2 |

The V7 vacuum>=3 displayed-marketability proxy includes one wrong-direction
candidate. V8 removes exactly that late, still-refilling candidate. Split in
chronological order, V8 is 5/5 on the first 50 markets and 1/1 on the second
50. A subsequent initial-leader-stability gate is monotonic (it can only remove
candidates); targeted replay confirmed that all six V8 candidates remain and
all six directions are correct. This is encouraging but far too few signals
for a live accuracy claim.

### Added decision-wait sensitivity

Rechecking only the seven V7 signals shows how quickly the residual asks
disappear if decision or submission is delayed:

| Signal-to-arrival latency | Marketable | Correct | Wrong |
|---:|---:|---:|---:|
| 970 ms | 3/7 | 2 | 1 |
| 1,370 ms | 1/7 | 0 | 1 |
| 1,770 ms | 1/7 | 0 | 1 |
| 2,070 ms | 1/7 | 0 | 1 |

An additional 400 ms removed both correct marketable proxies in this small
corpus while the 1-cent wrong-side ask remained. Waiting for any late external
confirmation is therefore contrary to the strategy thesis: direction must be
inferred from the early CLOB transition itself.

## New research direction

V8 stays CLOB-only. It keeps the event-driven winner-support detector but makes
`loser_refill_failure` mandatory rather than allowing any three of the four
vacuum components to pass. It locks to the first observable post-close leader
and permanently abstains if that leader reverses. It also rejects candidates
first recognized after T+250 ms, when a new leader is more likely to be a later
reversal than the initial close reaction. Outcome data is used only after the
fact as a label.

Forward capture must establish:

1. whether the first 250 ms native YES/NO sequence is reconstructed correctly;
2. whether the loser side truly stops refilling before an entry candidate;
3. whether the residual ask remains after decision and submit latency;
4. the abstention rate and zero-error sample size on unseen markets.

### First native YES/NO forward shadow

One BTC round (`btc-updown-5m-1785399300`) completed with 2,951 paired
callbacks, including 315 after close. The first post-close callback arrived at
T+9.452 ms. V6.7, V7, and V8 all abstained, and the capture reported no stream
errors. This is a valid no-signal plumbing observation, not an additional
accuracy sample. Source-to-receive measurements from this host showed clock
skew and are excluded from latency conclusions.

No finite stochastic backtest proves a future 100% hit rate. With zero observed
errors, the one-sided 95% lower confidence bound is only 97.05% after 100
trades. Reaching a 99% lower bound requires 299 zero-error trades; reaching
99.9% requires 2,995. A literal stochastic guarantee is possible only by always
abstaining; the practical target is zero observed errors under a deliberately
high abstention rate plus a separate hard loss cap.

## Remaining limitations

- PMData `local_timestamp` is the collector's arrival time, not the production
  host's arrival time.
- NO depth is reconstructed from a single archived token book.
- Displayed marketability is not an exchange acknowledgement or fill.
- Final outcomes are labels only; they are never decision inputs.
