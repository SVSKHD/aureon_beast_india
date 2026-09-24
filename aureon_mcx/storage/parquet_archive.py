"""Optional Parquet archive of raw candles (config flag AUREON_PARQUET_ARCHIVE)."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

from aureon_mcx.market.candle import Candle
from aureon_mcx.market.timeutil import to_db


class ParquetArchive:
    def __init__(self, root: str | Path, enabled: bool):
        self.root = Path(root)
        self.enabled = enabled

    def archive(self, candles: Iterable[Candle]) -> Path | None:
        if not self.enabled:
            return None
        rows = list(candles)
        if not rows:
            return None
        import pandas as pd  # local import: pandas/pyarrow are heavy

        df = pd.DataFrame(
            [
                {
                    "symbol": c.symbol, "security_id": c.security_id, "expiry_date": c.expiry_date,
                    "timeframe": c.timeframe.value, "open_time": to_db(c.open_time), "open": c.open, "high": c.high,
                    "low": c.low, "close": c.close, "volume": c.volume, "open_interest": c.open_interest, "source": c.source,
                }
                for c in rows
            ]
        )
        first = rows[0]
        day = to_db(first.open_time)[:10]
        out = self.root / first.symbol / first.timeframe.value / f"{day}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        if out.exists():
            old = pd.read_parquet(out)
            df = pd.concat([old, df]).drop_duplicates(subset=["security_id", "timeframe", "open_time"], keep="last")
        df.sort_values("open_time").to_parquet(out, index=False)
        return out
