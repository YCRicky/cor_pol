"""Aftertake runtime settings and official CLOB V2 account identity."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple, Union

from .post_close_snapshot import (
    POST_CLOSE_SNAPSHOT_DELAY_S,
    POST_CLOSE_SNAPSHOT_LIMIT_PRICE,
    POST_CLOSE_SNAPSHOT_MAX_LATENESS_S,
    POST_CLOSE_SNAPSHOT_PAIRED_MAX_AGE_S,
)
from .resolver import parse_resolve_overrides
from .twap_tail import TailRuleConfig

POLYGON_CHAIN_ID = 137
DEFAULT_CLOB_HOST = "https://clob.polymarket.com"
DEFAULT_GAMMA_HOST = "https://gamma-api.polymarket.com"
DEFAULT_GEO_ENDPOINT = "https://polymarket.com/api/geoblock"
# Production/EC2 default: use normal system DNS. The override is an opt-in
# emergency guard for RPZ-poisoned environments, configured via
# AFTERTAKE_RESOLVE_OVERRIDES="host=ip,ip;...".
DEFAULT_RESOLVE_OVERRIDES = ""
DEFAULT_ASSETS = ("BTC", "ETH", "SOL", "XRP", "BNB", "DOGE")
_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
_PRIVATE_KEY_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")


def load_dotenv(path: Union[str, Path] = ".env") -> None:
    """Load a local dotenv file without replacing process-level values."""

    dotenv_path = Path(path)
    if not dotenv_path.exists():
        return
    for raw in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise ValueError("%s must be a boolean" % name)


def _float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    try:
        return float(raw) if raw else default
    except ValueError as exc:
        raise ValueError("%s must be numeric" % name) from exc


def _int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    try:
        return int(raw) if raw else default
    except ValueError as exc:
        raise ValueError("%s must be an integer" % name) from exc


def _parse_asset_list(raw: str) -> Tuple[str, ...]:
    return tuple(part.strip().upper() for part in str(raw or "").replace(";", ",").split(",") if part.strip())


def _signature_type_from_env() -> Optional[int]:
    raw = os.getenv("SIGNATURE_TYPE", "").strip().strip('"').strip("'")
    if not raw:
        return None
    aliases = {"EOA": 0, "POLY_PROXY": 1, "GNOSIS_SAFE": 2, "POLY_1271": 3}
    normalized = raw.upper()
    if normalized in aliases:
        return aliases[normalized]
    try:
        return int(normalized)
    except ValueError as exc:
        raise ValueError("SIGNATURE_TYPE is invalid") from exc


@dataclass(frozen=True)
class Settings:
    # Aftertake strategy controls.
    dry_run: bool = True
    strategy_family: str = "twap_tail_v2"
    v9_live_enabled: bool = False
    asset: str = "BTC"
    assets: Tuple[str, ...] = DEFAULT_ASSETS
    # The live TWAP-tail contract takes only a small, depth-checked quantity.
    qty: float = 5.0
    post_close_snapshot_delay_s: float = POST_CLOSE_SNAPSHOT_DELAY_S
    post_close_leader_bid_threshold: float = 0.80
    post_close_paired_max_age_s: float = POST_CLOSE_SNAPSHOT_PAIRED_MAX_AGE_S
    post_close_snapshot_max_lateness_s: float = POST_CLOSE_SNAPSHOT_MAX_LATENESS_S
    post_close_limit_price: float = POST_CLOSE_SNAPSHOT_LIMIT_PRICE
    # TWAP-tail live contract. Binance Futures is observational only; the PM
    # book selects the side and PM remains the configured resolution source.
    tail_decision_lead_s: float = 10.0
    tail_max_decision_lateness_s: float = 1.0
    tail_leader_bid_threshold: float = 0.90
    tail_pm_quote_max_age_s: float = 2.0
    tail_limit_price: float = 0.99
    tail_min_net_win_per_share: float = 0.001
    out_dir: Path = Path("out")
    max_daily_loss: float = 25.0
    max_open_positions: int = 3
    max_consecutive_losses: int = 5
    min_seconds_between_entries: int = 60
    live_max_account_risk_fraction: float = 0.50
    live_quantity_floor_step: float = 1.0
    dry_run_simulated_balance: float = 100.0
    resolve_overrides: str = DEFAULT_RESOLVE_OVERRIDES

    # Fixed execution mechanics, not environment strategy settings.
    state_db: Path = Path("out/aftertake.sqlite3")
    order_type: str = "GTC"
    order_ttl_s: float = 5.0
    reconcile_timeout_s: float = 45.0
    heartbeat_interval_s: float = 4.0
    builder_code: str = ""

    # Official CLOB V2 account fields.
    gamma_host: str = DEFAULT_GAMMA_HOST
    clob_host: str = DEFAULT_CLOB_HOST
    geo_endpoint: str = DEFAULT_GEO_ENDPOINT
    polymarket_private_key: str = ""
    polymarket_funder: str = ""
    polymarket_signature_type: Optional[int] = None
    polymarket_chain_id: int = POLYGON_CHAIN_ID
    clob_api_key: str = ""
    clob_api_secret: str = ""
    clob_api_passphrase: str = ""
    telegram_token: str = ""
    telegram_chat_id: str = ""

    @property
    def is_live(self) -> bool:
        return not self.dry_run

    @property
    def runtime_lock(self) -> Path:
        return self.out_dir / "aftertake.runtime.lock"

    @property
    def has_static_api_creds(self) -> bool:
        return bool(self.clob_api_key and self.clob_api_secret and self.clob_api_passphrase)

    def validate(self) -> None:
        allowed_assets = {"BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "HYPE"}
        assets = tuple(str(asset).upper().strip() for asset in (self.assets or (self.asset,)) if str(asset).strip())
        if not assets:
            raise ValueError("AFTERTAKE_ASSETS must contain at least one asset")
        invalid_assets = sorted(set(assets) - allowed_assets)
        if invalid_assets:
            raise ValueError("AFTERTAKE_ASSETS contains unsupported assets: %s" % ",".join(invalid_assets))
        object.__setattr__(self, "assets", assets)
        object.__setattr__(self, "asset", assets[0])
        if self.qty <= 0:
            raise ValueError("AFTERTAKE_QTY must be > 0")
        if self.max_daily_loss <= 0:
            raise ValueError("AFTERTAKE_MAX_DAILY_LOSS must be > 0")
        if self.max_open_positions <= 0:
            raise ValueError("AFTERTAKE_MAX_OPEN_POSITIONS must be > 0")
        if self.max_consecutive_losses <= 0:
            raise ValueError("AFTERTAKE_MAX_CONSECUTIVE_LOSSES must be > 0")
        if self.min_seconds_between_entries < 0:
            raise ValueError("AFTERTAKE_MIN_SECONDS_BETWEEN_ENTRIES must be >= 0")
        if not 0 < self.live_max_account_risk_fraction <= 1:
            raise ValueError("AFTERTAKE_LIVE_MAX_ACCOUNT_RISK_FRACTION must be in (0, 1]")
        if self.live_quantity_floor_step <= 0:
            raise ValueError("AFTERTAKE_LIVE_QTY_FLOOR_STEP must be > 0")
        if self.dry_run_simulated_balance <= 0:
            raise ValueError("AFTERTAKE_DRY_RUN_SIM_BALANCE must be > 0")
        if self.post_close_snapshot_delay_s <= 0:
            raise ValueError("AFTERTAKE_POST_CLOSE_SNAPSHOT_DELAY_S must be > 0")
        if not 0 < self.post_close_leader_bid_threshold < 1:
            raise ValueError("AFTERTAKE_POST_CLOSE_LEADER_BID_THRESHOLD must be in (0, 1)")
        if not 0 < self.post_close_paired_max_age_s <= 1:
            raise ValueError("AFTERTAKE_POST_CLOSE_PAIRED_MAX_AGE_S must be in (0, 1]")
        if not 0 <= self.post_close_snapshot_max_lateness_s <= 1:
            raise ValueError("AFTERTAKE_POST_CLOSE_SNAPSHOT_MAX_LATENESS_S must be in [0, 1]")
        if not 0 < self.post_close_limit_price < 1:
            raise ValueError("AFTERTAKE_POST_CLOSE_LIMIT_PRICE must be in (0, 1)")
        TailRuleConfig(
            decision_lead_s=self.tail_decision_lead_s,
            max_decision_lateness_s=self.tail_max_decision_lateness_s,
            leader_bid_threshold=self.tail_leader_bid_threshold,
            pm_quote_max_age_s=self.tail_pm_quote_max_age_s,
            entry_limit_price=self.tail_limit_price,
        ).validate()
        if self.tail_min_net_win_per_share < 0:
            raise ValueError("AFTERTAKE_TAIL_MIN_NET_WIN_PER_SHARE must be >= 0")
        strategy_family = str(self.strategy_family or "").strip().lower()
        if strategy_family not in {"v8", "v9", "twap_tail_v2"}:
            raise ValueError("AFTERTAKE_STRATEGY must be twap_tail_v2, v8, or v9")
        object.__setattr__(self, "strategy_family", strategy_family)
        if strategy_family == "v9" and self.is_live and not self.v9_live_enabled:
            raise ValueError("V9 live trading requires AFTERTAKE_V9_LIVE_ENABLED=true")
        parse_resolve_overrides(self.resolve_overrides)
        order_type = self.order_type.upper().strip()
        if order_type not in {"FAK", "FOK", "GTC", "GTD"}:
            raise ValueError("AFTERTAKE_ORDER_TYPE must be one of FAK, FOK, GTC, GTD")
        object.__setattr__(self, "order_type", order_type)
        if self.is_live and order_type != "GTC":
            raise ValueError("tail live entry requires AFTERTAKE_ORDER_TYPE=GTC")
        if not 0 < self.order_ttl_s <= 120:
            raise ValueError("internal order TTL must be in (0, 120]")
        if not 0 < self.reconcile_timeout_s <= 180:
            raise ValueError("internal reconciliation timeout must be in (0, 180]")
        if not 0 < self.heartbeat_interval_s <= 5:
            raise ValueError("internal heartbeat interval must be in (0, 5]")

        supplied_creds = [self.clob_api_key, self.clob_api_secret, self.clob_api_passphrase]
        if any(supplied_creds) and not all(supplied_creds):
            raise ValueError("CLOB API credentials must provide key, secret, and passphrase together")
        if self.dry_run:
            return

        missing = []
        if not _PRIVATE_KEY_RE.match(self.polymarket_private_key):
            missing.append("POLYMARKET_PRIVATE_KEY")
        if not _ADDRESS_RE.match(self.polymarket_funder):
            missing.append("FUNDER_ADDRESS")
        if self.polymarket_signature_type not in {0, 1, 2, 3}:
            missing.append("SIGNATURE_TYPE")
        if not self.has_static_api_creds:
            missing.extend(
                ("POLYMARKET_API_KEY", "POLYMARKET_API_SECRET", "POLYMARKET_PASSPHRASE")
            )
        if missing:
            raise ValueError("Live trading disabled until configured: " + ", ".join(missing))

    @classmethod
    def from_env(cls) -> Settings:
        load_dotenv()
        out_dir = Path(os.getenv("AFTERTAKE_OUT_DIR", "out"))
        raw_assets = os.getenv("AFTERTAKE_ASSETS", "").strip()
        legacy_asset = os.getenv("AFTERTAKE_ASSET", "").strip()
        if raw_assets:
            assets = _parse_asset_list(raw_assets)
        elif legacy_asset:
            # Be lenient with old EC2 .env files that accidentally put a
            # comma-separated universe in the legacy single-asset variable.
            assets = _parse_asset_list(legacy_asset)
            if assets == ("BTC",):
                assets = DEFAULT_ASSETS
        else:
            assets = DEFAULT_ASSETS
        settings = cls(
            dry_run=_bool("AFTERTAKE_DRY_RUN", True),
            strategy_family=os.getenv("AFTERTAKE_STRATEGY", "twap_tail_v2").strip().lower(),
            v9_live_enabled=_bool("AFTERTAKE_V9_LIVE_ENABLED", False),
            asset=assets[0] if assets else "BTC",
            assets=assets,
            qty=_float("AFTERTAKE_QTY", 5.0),
            post_close_snapshot_delay_s=_float(
                "AFTERTAKE_POST_CLOSE_SNAPSHOT_DELAY_S", POST_CLOSE_SNAPSHOT_DELAY_S
            ),
            post_close_leader_bid_threshold=_float(
                "AFTERTAKE_POST_CLOSE_LEADER_BID_THRESHOLD", 0.80
            ),
            post_close_paired_max_age_s=_float(
                "AFTERTAKE_POST_CLOSE_PAIRED_MAX_AGE_S", POST_CLOSE_SNAPSHOT_PAIRED_MAX_AGE_S
            ),
            post_close_snapshot_max_lateness_s=_float(
                "AFTERTAKE_POST_CLOSE_SNAPSHOT_MAX_LATENESS_S", POST_CLOSE_SNAPSHOT_MAX_LATENESS_S
            ),
            post_close_limit_price=_float(
                "AFTERTAKE_POST_CLOSE_LIMIT_PRICE", POST_CLOSE_SNAPSHOT_LIMIT_PRICE
            ),
            tail_decision_lead_s=_float("AFTERTAKE_TAIL_DECISION_LEAD_S", 10.0),
            tail_max_decision_lateness_s=_float("AFTERTAKE_TAIL_MAX_DECISION_LATENESS_S", 1.0),
            tail_leader_bid_threshold=_float("AFTERTAKE_TAIL_LEADER_BID_THRESHOLD", 0.90),
            tail_pm_quote_max_age_s=_float("AFTERTAKE_TAIL_PM_QUOTE_MAX_AGE_S", 2.0),
            tail_limit_price=_float("AFTERTAKE_TAIL_LIMIT_PRICE", 0.99),
            tail_min_net_win_per_share=_float("AFTERTAKE_TAIL_MIN_NET_WIN_PER_SHARE", 0.001),
            out_dir=out_dir,
            state_db=out_dir / "aftertake.sqlite3",
            max_daily_loss=_float("AFTERTAKE_MAX_DAILY_LOSS", 25.0),
            max_open_positions=_int("AFTERTAKE_MAX_OPEN_POSITIONS", 3),
            max_consecutive_losses=_int("AFTERTAKE_MAX_CONSECUTIVE_LOSSES", 5),
            min_seconds_between_entries=_int("AFTERTAKE_MIN_SECONDS_BETWEEN_ENTRIES", 60),
            live_max_account_risk_fraction=_float("AFTERTAKE_LIVE_MAX_ACCOUNT_RISK_FRACTION", 0.50),
            live_quantity_floor_step=_float("AFTERTAKE_LIVE_QTY_FLOOR_STEP", 1.0),
            dry_run_simulated_balance=_float("AFTERTAKE_DRY_RUN_SIM_BALANCE", 100.0),
            resolve_overrides=os.getenv("AFTERTAKE_RESOLVE_OVERRIDES", DEFAULT_RESOLVE_OVERRIDES).strip(),
            order_type=os.getenv("AFTERTAKE_ORDER_TYPE", "GTC").strip().upper(),
            order_ttl_s=_float("AFTERTAKE_ORDER_TTL_S", 5.0),
            reconcile_timeout_s=_float("AFTERTAKE_RECONCILE_TIMEOUT_S", 45.0),
            builder_code=os.getenv("POLY_BUILDER_CODE", "").strip(),
            clob_host=os.getenv("CLOB_API_URL", DEFAULT_CLOB_HOST).strip().rstrip("/"),
            polymarket_private_key=os.getenv("POLYMARKET_PRIVATE_KEY", "").strip(),
            polymarket_funder=os.getenv("FUNDER_ADDRESS", "").strip(),
            polymarket_signature_type=_signature_type_from_env(),
            polymarket_chain_id=_int("CHAIN_ID", POLYGON_CHAIN_ID),
            clob_api_key=os.getenv("POLYMARKET_API_KEY", "").strip(),
            clob_api_secret=os.getenv("POLYMARKET_API_SECRET", "").strip(),
            clob_api_passphrase=os.getenv("POLYMARKET_PASSPHRASE", "").strip(),
            telegram_token=os.getenv("TG_BOT_TOKEN", "").strip(),
            telegram_chat_id=os.getenv("TG_CHAT_ID", "").strip(),
        )
        settings.validate()
        return settings
