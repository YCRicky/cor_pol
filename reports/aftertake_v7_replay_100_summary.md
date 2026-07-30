# Aftertake V7 100-market replay

## Decision

Promote `v7_event_vacuum3` to forward shadow testing. Do not promote the
vacuum>=2 sensitivity profile and do not treat this replay as live fill proof.

## Corpus and method

- Same 100 cached BTC 5-minute PMData `poly_l2` markets for every profile.
- PMData `local_timestamp` determines replay order.
- NO depth is inferred from the binary complement of the archived YES book.
- That complement reconstruction cannot show a missing loser bid while also
  retaining an executable winner ask. The replay therefore measures V7's
  distinct-event timing and vacuum-score changes, not its new one-sided
  missing-loser-bid entry path.
- Current dynamic sizing and the runner's final-size classifier pass are used.
- Arrival checks require the last locally known book at the assumed arrival
  time to retain an ask no higher than the original GTC limit with enough size.
- Historical market and builder fees are unavailable and set to zero.

## Classifier comparison

| Profile | Signals | Median decision | Direction hit |
|---|---:|---:|---:|
| V6.7 current 50/100/3 | 16 | 294.702 ms | 12/16 = 75.0% |
| V7 event, vacuum>=3 | 25 | 132.431 ms | 20/25 = 80.0% |
| V7 event, vacuum>=2 | 43 | 102.199 ms | 30/43 = 69.8% |

V7 vacuum>=3 contains all 16 V6.7 signals and adds nine. Eight of the nine
added directions are correct in this corpus.

## Displayed-marketability sensitivity

| Assumed submit latency | V6.7 marketable | V7 vacuum>=3 marketable | V7 rate |
|---:|---:|---:|---:|
| 100 ms | 13/16 | 20/25 | 80.0% |
| 300 ms | 12/16 | 20/25 | 80.0% |
| 700 ms | 10/16 | 19/25 | 76.0% |
| 970 ms | 10/16 | 17/25 | 68.0% |

At 970 ms, six of the nine V7-added signals retain the displayed-marketability
proxy; five are correctly directed and one is wrong.

## Interpretation

The event-driven profile improves the measured decision median by 162.271 ms
and increases the number of residual asks still displayed at every tested
latency. Keeping vacuum>=3 is important: lowering it to two adds volume but
reduces direction quality.

The PnL proxy is not used for promotion because fill survivorship makes it
non-monotonic across latency assumptions. These results do not observe exchange
acknowledgement, queue position, hidden liquidity, or actual fills. Independent
native YES/NO forward shadow captures are specifically required to validate the
one-sided-vacuum path and actual residual-ask survival before live promotion.
