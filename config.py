import os


class Config:
    # OKX API Credentials
    OKX_API_KEY: str = os.getenv("OKX_API_KEY", "")
    OKX_SECRET_KEY: str = os.getenv("OKX_SECRET_KEY", "")
    OKX_PASSPHRASE: str = os.getenv("OKX_PASSPHRASE", "")

    # Mode Trading: Set True untuk Sandbox/Demo Trading OKX, False untuk Real Live Trading
    OKX_SANDBOX: bool = (
        os.getenv("OKX_SANDBOX", "false").lower() == "true"
    )

    # Security Token agar endpoint webhook tidak sembarangan di-hit orang lain
    WEBHOOK_SECRET: str = os.getenv("WEBHOOK_SECRET", "my_secret_token_123")

    # Alokasi Kapital Per-Trade (dalam USDT)
    TRADE_AMOUNT_USDT: float = float(os.getenv("TRADE_AMOUNT_USDT", "20.0"))

    # Server Port
    PORT: int = int(os.getenv("PORT", "8080"))