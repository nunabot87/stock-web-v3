from pydantic_settings import BaseSettings
from functools import lru_cache
from typing import Optional


class Settings(BaseSettings):
    """Centralized configuration for stock-web-v3."""
    
    # Application
    app_name: str = "stock-web-v3"
    debug: bool = False
    env: str = "production"
    
    # Database - TimescaleDB (Unix socket auth, no password needed for local)
    database_url: str = "postgresql:///stockdb?host=/var/run/postgresql"
    pool_size: int = 20
    max_overflow: int = 10
    pool_timeout: int = 30
    
    # Redis
    redis_url: str = "redis://localhost:6379/0"
    
    # Stockbit API config
    stockbit_base_url: str = "https://exodus.stockbit.com"
    stockbit_chartbit_url: str = "https://exodus.stockbit.com/chartbit"
    stockbit_token: Optional[str] = None
    stockbit_rate_limit: int = 10  # requests per second
    
    # Security  
    secret_key: str = "dev-secret-change-in-production"
    access_token_expire_minutes: int = 1440  # 24 hours
    
    # WebSocket
    ws_heartbeat_interval: int = 30
    
    # Scoring Engine Thresholds (v3)
    scoring_strong_buy_threshold: int = 68
    scoring_buy_threshold: int = 50
    scoring_sell_threshold: int = 35
    scoring_strong_sell_threshold: int = 30
    
    # Ingestion
    ingestion_batch_size: int = 10
    ingestion_delay_seconds: float = 0.1
    
    # Scheduler
    intraday_sync_interval: int = 5  # minutes
    daily_sync_time: str = "18:00"  # HH:MM format
    session_ttl: int = 86400  # 24 hours
    
    # Tier 1 - Blue chips (priority real-time)
    tier1_stocks: str = "BBCA,BBRI,BMRI,TLKM,UNVR,ASII,ICBP,KLBF,PGAS,BBNI"
    
    # Tier 2 - Banking, Mining, Property (3-min delay OK)
    tier2_stocks: str = "BRIS,BBTN,BBKP,ADRO,ITMG,PTBA,TINS,ANTM,MEDC,AALI,SMRA,PWON,CTRA,LPKR,BSDE,APLN"
    
    # Tier 3 - Opportunistic (on-demand)
    tier3_stocks: str = "ACES,ERAA,MAPI,INDF,CMRY,GGRM,MYOR,INCO,HRUM,MNCN,SCMA,EMTK,TKIM,JMII"
    
    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()


# Tier lists for quick lookup
TIER1_SET = set(get_settings().tier1_stocks.split(","))
TIER2_SET = set(get_settings().tier2_stocks.split(","))
TIER3_SET = set(get_settings().tier3_stocks.split(","))

# TIER_STOCKS mapping for API lookup
TIER_STOCKS: dict[int, list[str]] = {
    1: list(TIER1_SET),
    2: list(TIER2_SET),
    3: list(TIER3_SET),
}


def get_stock_tier(symbol: str) -> int:
    """Return tier level (0=None, 1=Tier1, 2=Tier2, 3=Tier3)."""
    if symbol in TIER1_SET:
        return 1
    if symbol in TIER2_SET:
        return 2
    if symbol in TIER3_SET:
        return 3
    return 0
