from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional, Tuple


def norm_cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def fair_up_from_spot(open_px: float, last_px: float, sigma_per_sec: float, tte_seconds: int) -> float:
    if open_px <= 0 or last_px <= 0 or tte_seconds <= 0:
        return 0.5
    sigma = max(1.8e-4, min(0.01, sigma_per_sec or 3.5e-4))
    x = math.log(last_px / open_px)
    z = x / (sigma * math.sqrt(max(float(tte_seconds), 1.0)))
    raw = norm_cdf(z)
    raw = 0.5 + (raw - 0.5) * 0.85
    if tte_seconds > 240:
        lo, hi = 0.15, 0.85
    elif tte_seconds > 180:
        lo, hi = 0.12, 0.88
    elif tte_seconds > 120:
        lo, hi = 0.08, 0.92
    elif tte_seconds > 60:
        lo, hi = 0.04, 0.96
    else:
        lo, hi = 0.01, 0.99
    return max(lo, min(hi, raw))


@dataclass
class RollingStats:
    window: int = 120
    btc_returns: Deque[float] = field(default_factory=lambda: deque(maxlen=480))
    eth_returns: Deque[float] = field(default_factory=lambda: deque(maxlen=480))
    last_btc: Optional[float] = None
    last_eth: Optional[float] = None

    def update(self, btc_px: float, eth_px: float) -> None:
        if self.last_btc is not None and self.last_btc > 0 and btc_px > 0:
            self.btc_returns.append(math.log(btc_px / self.last_btc))
        if self.last_eth is not None and self.last_eth > 0 and eth_px > 0:
            self.eth_returns.append(math.log(eth_px / self.last_eth))
        self.last_btc = btc_px
        self.last_eth = eth_px

    def correlation(self) -> Optional[float]:
        n = min(len(self.btc_returns), len(self.eth_returns), self.window)
        if n < 30:
            return None
        xs = list(self.btc_returns)[-n:]
        ys = list(self.eth_returns)[-n:]
        mx = sum(xs) / n
        my = sum(ys) / n
        sxx = sum((x - mx) ** 2 for x in xs)
        syy = sum((y - my) ** 2 for y in ys)
        sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
        denom = math.sqrt(sxx * syy)
        if denom <= 1e-12:
            return None
        return sxy / denom

    def beta(self) -> Optional[float]:
        n = min(len(self.btc_returns), len(self.eth_returns), self.window)
        if n < 30:
            return None
        xs = list(self.btc_returns)[-n:]
        ys = list(self.eth_returns)[-n:]
        mx = sum(xs) / n
        my = sum(ys) / n
        sxx = sum((x - mx) ** 2 for x in xs)
        sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
        if sxx <= 1e-12:
            return None
        return sxy / sxx

    def realized_sigma_per_sec(self, leg: str = "btc") -> Optional[float]:
        rets = self.btc_returns if leg == "btc" else self.eth_returns
        n = min(len(rets), self.window)
        if n < 30:
            return None
        xs = list(rets)[-n:]
        mu = sum(xs) / n
        var = sum((x - mu) ** 2 for x in xs) / max(n - 1, 1)
        return math.sqrt(var)


@dataclass
class GapHistogram:
    window: int = 600
    samples: Deque[float] = field(default_factory=lambda: deque(maxlen=2400))

    def add(self, gap: float) -> None:
        self.samples.append(gap)

    def quantile(self, q: float) -> Optional[float]:
        n = min(len(self.samples), self.window)
        if n < 30:
            return None
        xs = sorted(list(self.samples)[-n:])
        idx = max(0, min(n - 1, int(q * (n - 1))))
        return xs[idx]


@dataclass
class ArbSignal:
    direction: str
    leg_a: str
    leg_b: str
    price_a: float
    price_b: float
    size_a: float
    size_b: float
    gap: float
    correlation: float
    fair_diff: float


def evaluate_arb_box(
    btc_yes_ask: Optional[Tuple[float, float]],
    btc_no_ask: Optional[Tuple[float, float]],
    eth_yes_ask: Optional[Tuple[float, float]],
    eth_no_ask: Optional[Tuple[float, float]],
    rho: float,
    fair_up_btc: float,
    fair_up_eth: float,
) -> Optional[ArbSignal]:
    candidates = []
    if btc_yes_ask and eth_no_ask:
        a, asz = btc_yes_ask
        b, bsz = eth_no_ask
        if a > 0 and b > 0:
            candidates.append(("BTC_YES_ETH_NO", "BTC_YES", "ETH_NO", a, b, asz, bsz, fair_up_btc - (1.0 - fair_up_eth)))
    if btc_no_ask and eth_yes_ask:
        a, asz = btc_no_ask
        b, bsz = eth_yes_ask
        if a > 0 and b > 0:
            candidates.append(("BTC_NO_ETH_YES", "BTC_NO", "ETH_YES", a, b, asz, bsz, (1.0 - fair_up_btc) - fair_up_eth))
    if not candidates:
        return None
    best = None
    for direction, la, lb, pa, pb, sa, sb, fdiff in candidates:
        gap = 1.0 - pa - pb
        if best is None or gap > best.gap:
            best = ArbSignal(direction, la, lb, pa, pb, sa, sb, gap, rho, fdiff)
    return best


def single_leg_flip_signal(
    leg: str,
    leg_position_side: str,
    entry_price: float,
    fair_up: float,
    opp_ask: Optional[Tuple[float, float]],
    kill_threshold: float = 0.30,
) -> Optional[Tuple[float, float]]:
    if opp_ask is None:
        return None
    opp_px, opp_sz = opp_ask
    if opp_px <= 0 or opp_sz <= 0:
        return None
    fail_prob = (1.0 - fair_up) if leg_position_side == "YES" else fair_up
    if fail_prob < kill_threshold:
        return None
    if entry_price + opp_px >= 1.0:
        return None
    return opp_px, opp_sz


def leg_fail_prob(side: str, fair_up: float) -> float:
    """Probability this leg fails at settlement, given the current spot-derived fair_up.

    YES side fails when underlying ends below strike (fair_up small).
    NO side fails when underlying ends above strike (fair_up large).
    """
    return (1.0 - fair_up) if side == "YES" else fair_up


def policy_g_kill(
    leg_a_side: str, fair_up_a: float, entry_a: float,
    leg_b_side: str, fair_up_b: float, entry_b: float,
    leg_a_opp_ask: Optional[Tuple[float, float]],
    leg_b_opp_ask: Optional[Tuple[float, float]],
    qty: float,
    eps_loss: float = 0.05,
    t_recov: float = 0.55,
    t_lock: float = 0.80,
) -> Optional[Dict[str, Tuple[float, float, str]]]:
    """Policy G: dual-flip when both legs are in floating-loss AND the spot-derived
    fair model says recovery is unlikely (fa,fb >= t_recov), OR when either leg has
    crossed a hard "locked" threshold (max(fa,fb) >= t_lock).

    Floating PnL per leg uses the opposite-side ask as a conservative mark:
        mark_x = 1 - opp_x_ask    (i.e. what we could close the leg at right now)
        fpnl_x = mark_x - entry_x

    Returns the same shape as combo_kill_signal, or None if the policy does not fire
    or there is no executable liquidity on both opposites.
    """
    if leg_a_opp_ask is None or leg_b_opp_ask is None:
        return None
    apx, asz = leg_a_opp_ask
    bpx, bsz = leg_b_opp_ask
    if apx <= 0 or bpx <= 0 or asz < qty or bsz < qty:
        return None
    mark_a = 1.0 - apx
    mark_b = 1.0 - bpx
    fpnl_a = mark_a - entry_a
    fpnl_b = mark_b - entry_b
    fa = leg_fail_prob(leg_a_side, fair_up_a)
    fb = leg_fail_prob(leg_b_side, fair_up_b)
    both_under = (fpnl_a < -eps_loss) and (fpnl_b < -eps_loss) and (fa >= t_recov) and (fb >= t_recov)
    locked = max(fa, fb) >= t_lock
    if not (both_under or locked):
        return None
    if both_under:
        reason = f"G_both(fpnlA={fpnl_a:+.3f},fpnlB={fpnl_b:+.3f},fa={fa:.2f},fb={fb:.2f})"
    else:
        reason = f"G_lock(max_f={max(fa,fb):.2f})"
    return {"leg_a": (apx, asz, reason), "leg_b": (bpx, bsz, reason)}


def combo_kill_signal(
    leg_a_side: str, fair_up_a: float,
    leg_b_side: str, fair_up_b: float,
    leg_a_opp_ask: Optional[Tuple[float, float]],
    leg_b_opp_ask: Optional[Tuple[float, float]],
    qty: float,
    kill_threshold: float = 0.60,
) -> Optional[Dict[str, Tuple[float, float, str]]]:
    """4th-quadrant pre-emptive hedge: flip BOTH legs when EITHER leg's spot-derived
    fail prob has crossed kill_threshold. Caller must additionally gate this on a small
    tte window (e.g. tte <= 30s) so we only insure against last-second PM oracle
    manipulation, not against early-round mean-reverting drift.

    Rationale: empirically (Run #2 replay), Q4 catastrophes are characterized by one
    leg already losing badly while the other leg is a "weak winner" near strike. The
    weak winner gets flipped by sub-noise PM oracle moves in the final seconds. Waiting
    for BOTH legs to be adverse simultaneously is too late. Triggering when EITHER leg
    crosses the threshold inside the tte gate locks in $2 gross on both legs, accepting
    the flip cost as tail-risk insurance against the (lose, lose) Q4 outcome.

    PM order book is used ONLY to confirm executable liquidity on the opposite side.

    Returns {"leg_a": (opp_px, opp_sz, reason), "leg_b": (...)} or None.
    """
    fail_a = leg_fail_prob(leg_a_side, fair_up_a)
    fail_b = leg_fail_prob(leg_b_side, fair_up_b)
    if max(fail_a, fail_b) < kill_threshold:
        return None
    if leg_a_opp_ask is None or leg_b_opp_ask is None:
        return None
    apx, asz = leg_a_opp_ask
    bpx, bsz = leg_b_opp_ask
    if apx <= 0 or bpx <= 0 or asz < qty or bsz < qty:
        return None
    reason = f"spot_kill(fa={fail_a:.2f},fb={fail_b:.2f})"
    return {"leg_a": (apx, asz, reason), "leg_b": (bpx, bsz, reason)}


def evaluate_reverse_box(
    btc_yes_ask: Optional[Tuple[float, float]],
    btc_no_ask: Optional[Tuple[float, float]],
    eth_yes_ask: Optional[Tuple[float, float]],
    eth_no_ask: Optional[Tuple[float, float]],
    main_direction: str,
) -> Optional[ArbSignal]:
    """Build the box opposite to `main_direction` for tail-hedge purposes. No gap
    requirement; caller decides whether the cost is acceptable."""
    if main_direction == "BTC_NO_ETH_YES":
        if not (btc_yes_ask and eth_no_ask):
            return None
        a, asz = btc_yes_ask
        b, bsz = eth_no_ask
        if a <= 0 or b <= 0:
            return None
        return ArbSignal("BTC_YES_ETH_NO", "BTC_YES", "ETH_NO", a, b, asz, bsz, 1.0 - a - b, 0.0, 0.0)
    if main_direction == "BTC_YES_ETH_NO":
        if not (btc_no_ask and eth_yes_ask):
            return None
        a, asz = btc_no_ask
        b, bsz = eth_yes_ask
        if a <= 0 or b <= 0:
            return None
        return ArbSignal("BTC_NO_ETH_YES", "BTC_NO", "ETH_YES", a, b, asz, bsz, 1.0 - a - b, 0.0, 0.0)
    return None
