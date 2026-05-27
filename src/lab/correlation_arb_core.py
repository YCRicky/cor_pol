from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Optional, Tuple


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


@dataclass
class QuadrantEstimate:
    win_win: float
    win_lose: float
    lose_win: float
    lose_lose: float
    expected_gross: float
    model_edge: float
    event_corr: float


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def estimate_combo_quadrants(
    fair_a: float,
    fair_b: float,
    cost: float,
    rho: float,
) -> QuadrantEstimate:
    """Estimate the four post-entry quadrants for a two-leg combo.

    The combo legs are opposite-direction BTC/ETH legs, so a positive BTC/ETH
    return correlation maps to a negative event correlation between the two
    selected legs. This is deliberately simple: it is a tail-risk diagnostic and
    EV gate, not a pricing model.
    """
    pa = _clamp(fair_a, 0.001, 0.999)
    pb = _clamp(fair_b, 0.001, 0.999)
    event_corr = _clamp(-abs(rho), -0.95, 0.0)
    denom = math.sqrt(pa * (1.0 - pa) * pb * (1.0 - pb))
    p11 = pa * pb + event_corr * denom
    p11 = _clamp(p11, max(0.0, pa + pb - 1.0), min(pa, pb))
    p10 = pa - p11
    p01 = pb - p11
    p00 = 1.0 - pa - pb + p11
    expected_gross = pa + pb
    return QuadrantEstimate(
        win_win=p11,
        win_lose=p10,
        lose_win=p01,
        lose_lose=p00,
        expected_gross=expected_gross,
        model_edge=expected_gross - cost,
        event_corr=event_corr,
    )


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
