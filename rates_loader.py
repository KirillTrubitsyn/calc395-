from __future__ import annotations
import datetime as dt
from typing import List, Tuple, Optional
import httpx

DEFAULT_RATE_STEPS: List[Tuple[dt.date, float]] = []

def _normalize_headers(cols):
    return [str(c).strip().lower().replace("\ufeff", "") for c in cols]

class RatesProvider:
    def __init__(
        self,
        source_url: Optional[str] = None,
        refresh_seconds: int = 6 * 60 * 60,
    ):
        self.source_url = source_url
        self.refresh_seconds = refresh_seconds
        self._cache: List[Tuple[dt.date, float]] = []
        self._last_fetch: Optional[dt.datetime] = None

    async def _fetch(self) -> List[Tuple[dt.date, float]]:
        if not self.source_url:
            return DEFAULT_RATE_STEPS

        # Follow 3xx redirects (Google Sheets pub?output=csv returns 307 to googleusercontent.com)
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            r = await client.get(self.source_url)
            r.raise_for_status()
            text = r.text

        # Try CSV → TSV → JSON
        import pandas as pd
        from io import StringIO
        sio = StringIO(text)
        try:
            df = pd.read_csv(sio)
        except Exception:
            sio.seek(0)
            try:
                df = pd.read_csv(sio, sep="\t")
            except Exception:
                sio.seek(0)
                df = pd.read_json(sio)

        df.columns = _normalize_headers(df.columns)

        if "date_from" not in df.columns or "key_rate" not in df.columns:
            raise ValueError("Rates file must have columns: date_from, key_rate (UTF-8 CSV/JSON).")

        df["date_from"] = df["date_from"].astype(str).str.strip().str.replace("\ufeff", "", regex=False)
        df["key_rate"]  = df["key_rate"].astype(str).str.strip()
        df["key_rate"]  = df["key_rate"].str.replace("%", "", regex=False).str.replace(",", ".", regex=False)

        import pandas as pd
        df["key_rate"] = pd.to_numeric(df["key_rate"], errors="coerce")

        def parse_date_safe(s):
            try:
                return dt.date.fromisoformat(s[:10])
            except Exception:
                return None
        df["date_from"] = df["date_from"].apply(parse_date_safe)

        df = df.dropna(subset=["date_from", "key_rate"])
        steps: List[Tuple[dt.date, float]] = [(row["date_from"], float(row["key_rate"])) for _, row in df.iterrows()]
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
