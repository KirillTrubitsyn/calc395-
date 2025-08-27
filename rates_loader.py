from __future__ import annotations
import datetime as dt
from typing import List, Tuple, Optional
import httpx

# Fallback table of key-rate steps (date_from, rate_percent).
# Keep empty by default; prefer providing RATES_URL for authoritative data.
DEFAULT_RATE_STEPS: List[Tuple[dt.date, float]] = []

class RatesProvider:
    def __init__(
        self,
        source_url: Optional[str] = None,
        refresh_seconds: int = 6 * 60 * 60,
    ):
        """
        source_url: Public JSON/CSV/TSV with columns:
           - date_from (YYYY-MM-DD)
           - key_rate (float, % per annum)
        """
        self.source_url = source_url
        self.refresh_seconds = refresh_seconds
        self._cache: List[Tuple[dt.date, float]] = []
        self._last_fetch: Optional[dt.datetime] = None

    async def _fetch(self) -> List[Tuple[dt.date, float]]:
        if not self.source_url:
            return DEFAULT_RATE_STEPS

        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.get(self.source_url)
            r.raise_for_status()
            text = r.text

        # Try CSV/TSV/JSON using pandas
        import pandas as pd
        from io import StringIO
        sio = StringIO(text)
        try:
            if self.source_url.endswith(".json"):
                df = pd.read_json(sio)
            else:
                df = pd.read_csv(sio)
        except Exception:
            sio.seek(0)
            df = pd.read_csv(sio, sep="\\t")

        if "date_from" not in df.columns or "key_rate" not in df.columns:
            raise ValueError("Rates file must have columns: date_from, key_rate")

        steps: List[Tuple[dt.date, float]] = []
        for _, row in df.iterrows():
            d = dt.date.fromisoformat(str(row["date_from"])[:10])
            rate = float(row["key_rate"])
            steps.append((d, rate))

        steps.sort(key=lambda x: x[0])
        return steps

    async def get_steps(self) -> List[Tuple[dt.date, float]]:
        import datetime as dtdt
        now = dtdt.datetime.utcnow()
        if (
            self._last_fetch is None
            or (now - self._last_fetch).total_seconds() > self.refresh_seconds
        ):
            self._cache = await self._fetch()
            self._last_fetch = now
        return self._cache

    def set_steps(self, steps: List[Tuple[dt.date, float]]) -> None:
        self._cache = sorted(steps, key=lambda x: x[0])
        import datetime as dtdt
        self._last_fetch = dtdt.datetime.utcnow()
