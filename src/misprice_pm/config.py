"""Original strategy settings and the official CLOB V2 account identity."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

POLYGON_CHAIN_ID = 137
DEFAULT_CLOB_HOST = "https://clob.polymarket.com"
DEFAULT_GAMMA_HOST = "https://gamma-api.polymarket.com"
DEFAULT_GEO_ENDPOINT = "https://polymarket.com/api/geoblock"
_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
_PRIVATE_KEY_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")


def load_dotenv(path: Union[str, Path] = ".env") -> None:
    """Load a local dotenv file without overwriting process-level secrets."""

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
    aliases = {
        "EOA": 0,
        "POLY_PROXY": 1,
        "GNOSIS_SAFE": 2,
        "POLY_1271": 3,
    }
    normalized = raw.upper()
    if normalized in aliases:
        return aliases[normalized]
    try:
        return int(normalized)
    except ValueError as exc:
        raise ValueError("SIGNATURE_TYPE is invalid") from exc


@dataclass(frozen=True)
class Settings:
    # Existing strategy controls; these remain operator-owned environment values.
    dry_run: bool = True
    asset: str = "BTC"
    binance_symbol: str = "BTCUSDT"
    qty: float = 5.0
    min_entry_ask: float = 0.35
    max_entry_ask: float = 0.65
    min_transition_bp: float = 3.0
    max_pre_abs_bp: float = 2.5
    min_abs_bp: float = 3.5
    reprice_per_bp: float = 0.04
    min_lag_depth: float = 0.035
    min_elapsed_s: int = 20
    max_elapsed_s: int = 220
    ban_elapsed_start_s: int = -1
    ban_elapsed_end_s: int = -1
    loop_interval_s: float = 1.0
    out_dir: Path = Path("out")
    max_daily_loss: float = 25.0
    max_open_positions: int = 1
    max_consecutive_losses: int = 5
    min_seconds_between_entries: int = 60

    # Fixed execution mechanics; they are not strategy or account settings.
    state_db: Path = Path("out/misprice_pm.sqlite3")
    max_book_age_s: float = 2.0
    max_spot_age_s: float = 3.0
    max_open_capture_delay_s: float = 3.0
    order_ttl_s: float = 5.0
    reconcile_timeout_s: float = 8.0
    heartbeat_interval_s: float = 5.0
    builder_code: str = ""

    # Official CLOB V2 account values, loaded from the deployment contract.
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
    def has_static_api_creds(self) -> bool:
        return bool(self.clob_api_key and self.clob_api_secret and self.clob_api_passphrase)

    def validate(self) -> None:
        if self.asset.upper() != "BTC":
            raise ValueError("MISPRICE_ASSET currently supports BTC only")
        if self.qty <= 0:
            raise ValueError("MISPRICE_QTY must be > 0")
        if not 0 < self.min_entry_ask < self.max_entry_ask < 1:
            raise ValueError("entry ask bounds must satisfy 0 < min < max < 1")
        if self.min_transition_bp <= 0:
            raise ValueError("MISPRICE_MIN_TRANSITION_BP must be > 0")
        if self.max_pre_abs_bp < 0:
            raise ValueError("MISPRICE_MAX_PRE_ABS_BP must be >= 0")
        if self.min_abs_bp <= 0:
            raise ValueError("MISPRICE_MIN_ABS_BP must be > 0")
        if self.reprice_per_bp <= 0:
            raise ValueError("MISPRICE_REPRICE_PER_BP must be > 0")
        if self.min_lag_depth < 0:
            raise ValueError("MISPRICE_MIN_LAG_DEPTH must be >= 0")
        if self.min_elapsed_s < 0 or self.max_elapsed_s <= self.min_elapsed_s:
            raise ValueError("elapsed bounds must satisfy 0 <= min < max")
        if self.loop_interval_s <= 0:
            raise ValueError("MISPRICE_LOOP_INTERVAL_S must be > 0")
        if self.max_daily_loss <= 0:
            raise ValueError("MISPRICE_MAX_DAILY_LOSS must be > 0")
        if self.max_open_positions <= 0:
            raise ValueError("MISPRICE_MAX_OPEN_POSITIONS must be > 0")
        if self.max_consecutive_losses <= 0:
            raise ValueError("MISPRICE_MAX_CONSECUTIVE_LOSSES must be > 0")
        if self.min_seconds_between_entries < 0:
            raise ValueError("MISPRICE_MIN_SECONDS_BETWEEN_ENTRIES must be >= 0")
        if not 0 < self.order_ttl_s <= 10:
            raise ValueError("internal order TTL must be in (0, 10]")
        if not 0 < self.reconcile_timeout_s <= 30:
            raise ValueError("internal reconciliation timeout must be in (0, 30]")
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
        out_dir = Path(os.getenv("MISPRICE_OUT_DIR", "out"))
        settings = cls(
            dry_run=_bool("MISPRICE_DRY_RUN", True),
            asset=os.getenv("MISPRICE_ASSET", "BTC").strip().upper(),
            binance_symbol=os.getenv("MISPRICE_BINANCE_SYMBOL", "BTCUSDT").strip().upper(),
            qty=_float("MISPRICE_QTY", 5.0),
            min_entry_ask=_float("MISPRICE_MIN_ENTRY_ASK", 0.35),
            max_entry_ask=_float("MISPRICE_MAX_ENTRY_ASK", 0.65),
            min_transition_bp=_float("MISPRICE_MIN_TRANSITION_BP", 3.0),
            max_pre_abs_bp=_float("MISPRICE_MAX_PRE_ABS_BP", 2.5),
            min_abs_bp=_float("MISPRICE_MIN_ABS_BP", 3.5),
            reprice_per_bp=_float("MISPRICE_REPRICE_PER_BP", 0.04),
            min_lag_depth=_float("MISPRICE_MIN_LAG_DEPTH", 0.035),
            min_elapsed_s=_int("MISPRICE_MIN_ELAPSED_S", 20),
            max_elapsed_s=_int("MISPRICE_MAX_ELAPSED_S", 220),
            ban_elapsed_start_s=_int("MISPRICE_BAN_ELAPSED_START_S", -1),
            ban_elapsed_end_s=_int("MISPRICE_BAN_ELAPSED_END_S", -1),
            loop_interval_s=_float("MISPRICE_LOOP_INTERVAL_S", 1.0),
            out_dir=out_dir,
            state_db=out_dir / "misprice_pm.sqlite3",
            max_daily_loss=_float("MISPRICE_MAX_DAILY_LOSS", 25.0),
            max_open_positions=_int("MISPRICE_MAX_OPEN_POSITIONS", 1),
            max_consecutive_losses=_int("MISPRICE_MAX_CONSECUTIVE_LOSSES", 5),
            min_seconds_between_entries=_int("MISPRICE_MIN_SECONDS_BETWEEN_ENTRIES", 60),
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
