from __future__ import annotations
import datetime as dt
from typing import List, Optional, Literal
from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel, Field, ConfigDict
from dateutil.relativedelta import relativedelta
import os

from rates_loader import RatesProvider

app = FastAPI(title="395-GK Calculator", version="1.0.1")

RATES_URL = os.getenv("RATES_URL")
rates = RatesProvider(source_url=RATES_URL)

DayCount = Literal["365", "ACT/365"]

class PeriodItem(BaseModel):
    start: dt.date
    end: dt.date
    rate: float = Field(..., description="Key rate, % per annum")
    days: int
    interest: float

class CalcResponse(BaseModel):
    model_config = ConfigDict(json_schema_extra={"example": {
        "periods": [
            {"start": "2024-03-01", "end": "2024-07-25", "rate": 16.0, "days": 147, "interest": 644657.53},
            {"start": "2024-07-26", "end": "2024-09-01", "rate": 15.0, "days": 38, "interest": 156164.38}
        ],
        "total": 800821.91
    }})
    periods: List[PeriodItem]
    total: float

@app.get("/health")
async def health():
    return {"ok": True, "version": app.version, "rates_url_set": bool(RATES_URL)}

@app.get("/rates")
async def get_rates():
    try:
        steps = await rates.get_steps()
        return [{"date_from": s[0].isoformat(), "key_rate": s[1]} for s in steps]
    except Exception as e:
        # Return explicit 503 with error detail to help diagnose
        raise HTTPException(status_code=503, detail=f"Failed to load/parse rates from RATES_URL. Error: {e}")

def _split_by_rate_steps(start: dt.date, end: dt.date, steps: list[tuple]) -> list[tuple]:
    if not steps:
        return []
    out = []
    cur = start
    # find index applicable at start
    applicable_idx = 0
    for i in range(len(steps)):
        if steps[i][0] <= cur:
            applicable_idx = i
        else:
            break
    i = applicable_idx
    while cur < end and i < len(steps):
        s_date, s_rate = steps[i]
        next_change = steps[i + 1][0] if i + 1 < len(steps) else None
        p_start = cur
        p_end = end if next_change is None else min(end, next_change)
        out.append((p_start, p_end, s_rate))
        cur = p_end
        i += 1
    return out

def _days_between(a: dt.date, b: dt.date) -> int:
    return (b - a).days

def _day_basis(days: int, basis: DayCount) -> float:
    return days / 365.0

@app.get("/calc395", response_model=CalcResponse)
async def calc395(
    amount: float = Query(..., gt=0, description="Principal amount (RUB)"),
    start_date: dt.date = Query(..., description="Start date inclusive (YYYY-MM-DD)"),
    end_date: dt.date = Query(..., description="End date (YYYY-MM-DD)"),
    end_inclusive: bool = Query(False, description="Include the end date in calculation"),
    day_count: DayCount = Query("365", description="Day count basis: 365 or ACT/365"),
):
    if end_inclusive:
        end_date = end_date + relativedelta(days=1)
    if end_date <= start_date:
        return CalcResponse(periods=[], total=0.0)

    try:
        steps = await rates.get_steps()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Failed to load/parse rates from RATES_URL. Error: {e}")

    if not steps:
        raise HTTPException(status_code=503, detail="No rate steps available. Check RATES_URL and CSV/JSON format.")

    pieces = _split_by_rate_steps(start_date, end_date, steps)
    periods: list[PeriodItem] = []
    total = 0.0

    if pieces == [] and steps and steps[0][0] > start_date:
        pieces = [(start_date, end_date, steps[0][1])]

    for p_start, p_end, rate in pieces:
        days = _days_between(p_start, p_end)
        if days <= 0: 
            continue
        fraction = _day_basis(days, day_count)
        interest = amount * (rate / 100.0) * fraction
        periods.append(PeriodItem(start=p_start, end=p_end, rate=rate, days=days, interest=round(interest, 2)))
        total += interest

    return CalcResponse(periods=periods, total=round(total, 2))
