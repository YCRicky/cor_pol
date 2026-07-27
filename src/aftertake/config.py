"""Aftertake runtime settings and official CLOB V2 account identity."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

from .resolver import parse_resolve_overrides

POLYGON_CHAIN_ID = 137
DEFAULT_CLOB_HOST = "https://clob.polymarket.com"
DEFAULT_GAMMA_HOST = "https://gamma-api.polymarket.com"
DEFAULT_GEO_ENDPOINT = "https://polymarket.com/api/geoblock"
# Production/EC2 default: use normal system DNS. The override is an opt-in
# emergency guard for RPZ-poisoned environments, configured via
# AFTERTAKE_RESOLVE_OVERRIDES="host=ip,ip;...".
DEFAULT_RESOLVE_OVERRIDES = ""
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
    asset: str = "BTC"
    qty: float = 5.0
    out_dir: Path = Path("out")
    max_daily_loss: float = 25.0
    max_open_positions: int = 1
    max_consecutive_losses: int = 5
    min_seconds_between_entries: int = 60
    live_max_account_risk_fraction: float = 0.50
    live_quantity_floor_step: float = 1.0
    dry_run_simulated_balance: float = 100.0
    resolve_overrides: str = DEFAULT_RESOLVE_OVERRIDES

    # Fixed execution mechanics, not environment strategy settings.
    state_db: Path = Path("out/aftertake.sqlite3")
    order_type: str = "FAK"
    order_ttl_s: float = 5.0
    reconcile_timeout_s: float = 45.0
    heartbeat_interval_s: float = 5.0
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
        if self.asset.upper() != "BTC":
            raise ValueError("AFTERTAKE_ASSET currently supports BTC only")
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
        parse_resolve_overrides(self.resolve_overrides)
        order_type = self.order_type.upper().strip()
        if order_type not in {"FAK", "FOK", "GTC", "GTD"}:
            raise ValueError("AFTERTAKE_ORDER_TYPE must be one of FAK, FOK, GTC, GTD")
        object.__setattr__(self, "order_type", order_type)
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
        settings = cls(
            dry_run=_bool("AFTERTAKE_DRY_RUN", True),
            asset=os.getenv("AFTERTAKE_ASSET", "BTC").strip().upper(),
            qty=_float("AFTERTAKE_QTY", 5.0),
            out_dir=out_dir,
            state_db=out_dir / "aftertake.sqlite3",
            max_daily_loss=_float("AFTERTAKE_MAX_DAILY_LOSS", 25.0),
            max_open_positions=_int("AFTERTAKE_MAX_OPEN_POSITIONS", 1),
            max_consecutive_losses=_int("AFTERTAKE_MAX_CONSECUTIVE_LOSSES", 5),
            min_seconds_between_entries=_int("AFTERTAKE_MIN_SECONDS_BETWEEN_ENTRIES", 60),
            live_max_account_risk_fraction=_float("AFTERTAKE_LIVE_MAX_ACCOUNT_RISK_FRACTION", 0.50),
            live_quantity_floor_step=_float("AFTERTAKE_LIVE_QTY_FLOOR_STEP", 1.0),
            dry_run_simulated_balance=_float("AFTERTAKE_DRY_RUN_SIM_BALANCE", 100.0),
            resolve_overrides=os.getenv("AFTERTAKE_RESOLVE_OVERRIDES", DEFAULT_RESOLVE_OVERRIDES).strip(),
            order_type=os.getenv("AFTERTAKE_ORDER_TYPE", "FAK").strip().upper(),
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
