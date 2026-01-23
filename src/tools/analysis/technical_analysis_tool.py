# -*- coding: utf-8 -*-
"""
Technical Analysis Tool for US Stocks using FMP API
- Indicators: MACD / KDJ / RSI / ADX / ATR / MA
- Trend System: Uptrend/Downtrend (dynamic time window + soft conditions + pending status + momentum override)
- Output: Industry/Valuation/Indicators/Trend Analysis/Key Levels/Summary/Chart
"""

import os
import uuid
import asyncio
import numpy as np
import pandas as pd
from typing import Dict, Tuple, Optional, Any, List
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import mplfinance as mpf
from src.data_client.fmp import FMPClient

# ====================== Indicator Helper Functions ======================

def rma(series: pd.Series, n: int) -> pd.Series:
    """Running Moving Average (Wilder's smoothing)"""
    return series.ewm(alpha=1.0 / max(n, 1), adjust=False).mean()


def _atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    """Average True Range"""
    high, low, close = df['high'], df['low'], df['close']
    tr = pd.concat([
        (high - low),
        (high - close.shift(1)).abs(),
        (low - close.shift(1)).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1.0 / n, adjust=False).mean()


def compute_adx(df: pd.DataFrame, n: int = 14) -> pd.DataFrame:
    """Compute ADX, +DI, -DI"""
    high, low, close = df['high'], df['low'], df['close']
    up_move = high.diff()
    down_move = low.shift(1) - low
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    tr = pd.concat([
        (high - low),
        (high - close.shift(1)).abs(),
        (low - close.shift(1)).abs()
    ], axis=1).max(axis=1)

    atr = rma(tr, n)
    plus_di = 100 * rma(pd.Series(plus_dm, index=df.index), n) / atr
    minus_di = 100 * rma(pd.Series(minus_dm, index=df.index), n) / atr
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx = rma(dx.fillna(0), n)

    out = df.copy()
    out['ADX'] = adx
    out['PLUS_DI'] = plus_di
    out['MINUS_DI'] = minus_di
    return out


# ====================== Default Configuration ======================

DEFAULT_CFG = {
    "speed_conf": {
        "lookback": 60,
        "w_pos_mean": 0.6,
        "w_atr_pct": 0.4,
        "w_neg_mean": 0.6,
        "fast_threshold": 0.009,
        "slow_threshold": 0.006,
        "bands": {"fast": [2, 20], "normal": [5, 30], "slow": [8, 30]}
    },
    "up_soft": {
        "BC_MIN": 1, "BC_MAX": 35,
        "B_OVER_D_TOL": 0.98,
        "DAY_RATIO_MAX": 1.2,
        "RISE_RATIO_MIN": 1.1,
        "AFTER_C_FLOOR": 0.94
    },
    "down_soft": {
        "BC_MIN": 3, "BC_MAX": 35,
        "B_UNDER_D_TOL": 1.02,
        "DAY_RATIO_MAX": 1.2,
        "DROP_RATIO_MIN": 1.1,
        "AFTER_C_CAP": 1.03
    },
    "momentum_override": {
        "price_break_20": 1.005,
        "price_break_55": 1.00,
        "vol_today_vs_20": 1.2,
        "vol_3_vs_20": 1.0,
        "adx_min": 22,
        "roc3_min": 0.08
    }
}


# ====================== Moving Averages ======================

def add_mas(df: pd.DataFrame) -> pd.DataFrame:
    """Add MA5/10/20/60/250"""
    df = df.copy()
    df['MA5'] = df['close'].rolling(5).mean()
    df['MA10'] = df['close'].rolling(10).mean()
    df['MA20'] = df['close'].rolling(20).mean()
    df['MA60'] = df['close'].rolling(60).mean()
    df['MA250'] = df['close'].rolling(250).mean()
    return df


# ====================== Technical Indicators ======================

def calculate_macd(df: pd.DataFrame) -> Dict[str, Any]:
    """Calculate MACD indicator with English labels"""
    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    dif_series = ema12 - ema26
    dea_series = dif_series.ewm(span=9, adjust=False).mean()
    macd_series = (dif_series - dea_series) * 2

    dif, dea, macd = dif_series.iloc[-1], dea_series.iloc[-1], macd_series.iloc[-1]
    prev_dif, prev_dea = dif_series.iloc[-2], dea_series.iloc[-2]

    out: Dict[str, Any] = {
        "dif": round(float(dif), 3),
        "dea": round(float(dea), 3),
        "macd": round(float(macd), 3),
        "signal": "",
        "trend": "",
        "explanation": ""
    }

    # Determine cross signal
    if dif > dea and prev_dif <= prev_dea:
        out["signal"] = "golden_cross"
    elif dif < dea and prev_dif >= prev_dea:
        out["signal"] = "death_cross"
    else:
        out["signal"] = "no_cross"

    # Determine trend
    out["trend"] = "bullish" if macd > 0 else ("bearish" if macd < 0 else "neutral")

    # Explanation
    explain = f"DIF={dif:.3f}, DEA={dea:.3f}, MACD bar={macd:.3f}. "
    if out["signal"] == "golden_cross" and macd > 0:
        explain += "Golden cross above zero axis indicates strong bullish momentum."
    elif out["signal"] == "death_cross" and macd < 0:
        explain += "Death cross below zero axis indicates strong bearish pressure."
    elif out["signal"] == "golden_cross":
        explain += "Golden cross below zero, may be early rebound stage."
    elif out["signal"] == "death_cross":
        explain += "Death cross above zero, watch for short-term correction."
    else:
        explain += "No clear cross signal, sideways movement."

    out["explanation"] = explain
    return out


def calculate_kdj(df: pd.DataFrame) -> Dict[str, Any]:
    """Calculate KDJ indicator with English labels"""
    low_n = df['low'].rolling(9).min()
    high_n = df['high'].rolling(9).max()
    rsv = (df['close'] - low_n) / (high_n - low_n) * 100
    k = rma(rsv, 3)
    d = rma(k, 3)
    j = 3 * k - 2 * d

    k_val, d_val, j_val = k.iloc[-1], d.iloc[-1], j.iloc[-1]

    res: Dict[str, Any] = {
        "k": round(float(k_val), 2),
        "d": round(float(d_val), 2),
        "j": round(float(j_val), 2),
        "signal": "",
        "explanation": ""
    }

    if k_val > d_val and k_val < 80:
        res["signal"] = "bullish"
    elif k_val < d_val and k_val > 20:
        res["signal"] = "bearish"
    elif k_val >= 80:
        res["signal"] = "overbought_warning"
    elif k_val <= 20:
        res["signal"] = "oversold_opportunity"

    res["explanation"] = f"K={k_val:.2f}, D={d_val:.2f}, J={j_val:.2f}. Signal: {res['signal']}."
    return res


def calculate_rsi(df: pd.DataFrame) -> Dict[str, Any]:
    """Calculate RSI indicator with English labels"""
    def _rsi(x, n):
        delta = x.diff()
        up = np.where(delta > 0, delta, 0.0)
        down = np.where(delta < 0, -delta, 0.0)
        roll_up = rma(pd.Series(up, index=x.index), n)
        roll_down = rma(pd.Series(down, index=x.index), n)
        rs = roll_up / (roll_down.replace(0, np.nan))
        return 100 - (100 / (1 + rs))

    r6 = _rsi(df['close'], 6).iloc[-1]
    r12 = _rsi(df['close'], 12).iloc[-1]
    r24 = _rsi(df['close'], 24).iloc[-1]

    if (r6 > 70) or (r12 > 70):
        sig = "overbought_risk"
    elif (r6 < 30) or (r12 < 30):
        sig = "oversold_opportunity"
    else:
        sig = "normal_range"

    return {
        "rsi_6": round(float(r6), 2),
        "rsi_12": round(float(r12), 2),
        "rsi_24": round(float(r24), 2),
        "signal": sig,
        "explanation": f"6-day RSI={r6:.2f}, 12-day RSI={r12:.2f}. Signal: {sig}."
    }


# ====================== Pattern Detection Functions ======================

def _find_abc_up(df30: pd.DataFrame) -> Optional[Tuple[pd.Timestamp, float, pd.Timestamp, float, pd.Timestamp, float]]:
    """Find A-B-C uptrend pattern: A=high → B=pullback low → C=new high"""
    if len(df30) < 15:
        return None

    head = df30.iloc[:-3]
    if head.empty:
        return None

    ia = head['high'].idxmax()
    a = float(df30.loc[ia, 'high'])

    tail_after_a = df30.loc[ia:]
    if len(tail_after_a) < 5:
        return None

    ib = tail_after_a['low'].idxmin()
    if ib <= ia:
        return None
    b = float(df30.loc[ib, 'low'])

    tail_after_b = df30.loc[ib:]
    if len(tail_after_b) < 5:
        return None

    ic = tail_after_b['high'].idxmax()
    if ic <= ib:
        return None
    c = float(df30.loc[ic, 'high'])

    return ia, a, ib, b, ic, c


def _find_d_before_a(df: pd.DataFrame, ia: pd.Timestamp) -> Optional[Tuple[pd.Timestamp, float]]:
    """Find point D (lowest point in 30 days before A)"""
    left = df.loc[:ia].iloc[:-1].tail(30)
    if left.empty:
        return None
    id_ = left['low'].idxmin()
    d = float(left.loc[id_, 'low'])
    return id_, d


def _find_cross_dates_kdj(df: pd.DataFrame, lookback: int = 10) -> Dict[str, Optional[pd.Timestamp]]:
    """Find recent KDJ golden/death cross in last N days"""
    cross_buy, cross_sell = None, None

    # Calculate KDJ if not present
    low_n = df['low'].rolling(9).min()
    high_n = df['high'].rolling(9).max()
    rsv = (df['close'] - low_n) / (high_n - low_n) * 100
    k = rma(rsv, 3)
    d = rma(k, 3)

    start = max(1, len(df) - lookback)
    for i in range(start, len(df)):
        k_val, d_val = k.iloc[i], d.iloc[i]
        pk, pd_ = k.iloc[i-1], d.iloc[i-1]
        dt = df.index[i]

        if (pk <= pd_) and (k_val > d_val):
            cross_buy = dt
        if (pk >= pd_) and (k_val < d_val):
            cross_sell = dt

    return {"kdj_buy": cross_buy, "kdj_sell": cross_sell}


# ====================== Speed Band Detection ======================

def infer_speed_band(df: pd.DataFrame, mode: str = "up") -> Tuple[Tuple[int, int], Dict[str, Any]]:
    """Infer speed band based on recent price movement and ATR%"""
    sc = DEFAULT_CFG["speed_conf"]
    lb = sc["lookback"]
    rets = df['close'].pct_change().dropna()
    last = rets.tail(lb)

    if last.empty:
        return tuple(sc["bands"]["normal"]), {"label": "unknown", "score": None}

    if mode == "up":
        base = last[last > 0]
        comp_mean = float(base.mean()) if len(base) else 0.0
    else:
        base = last[last < 0]
        comp_mean = float(abs(base.mean())) if len(base) else 0.0

    atr_pct = ((_atr(df, 14) / df['close']).tail(lb)).mean()
    atr_pct = float(atr_pct) if pd.notna(atr_pct) else 0.0

    w_mean = sc["w_pos_mean"] if mode == "up" else sc["w_neg_mean"]
    score = w_mean * comp_mean + sc["w_atr_pct"] * atr_pct

    if score >= sc["fast_threshold"]:
        band, label = tuple(sc["bands"]["fast"]), "fast"
    elif score <= sc["slow_threshold"]:
        band, label = tuple(sc["bands"]["slow"]), "slow"
    else:
        band, label = tuple(sc["bands"]["normal"]), "normal"

    return band, {"label": label, "comp_mean": round(comp_mean, 4), "atr_pct": round(atr_pct, 4), "score": round(score, 4)}


# ====================== Momentum Override ======================

def momentum_override(df: pd.DataFrame) -> Tuple[bool, dict]:
    """
    Momentum breakout override (bypasses A-B-C pattern requirement)
    All conditions must be met:
    1) Close > yesterday's 20-day high * 1.005 (strong breakout)
    2) MA10 > MA20 and MA20 rising over 5 days
    3) Volume: today >= 1.2 * 20-day avg OR 3-day avg >= 1.0 * 20-day avg
    4) MACD bar > 0
    """
    mo = DEFAULT_CFG["momentum_override"]
    df_ma = add_mas(df)
    last = df_ma.iloc[-1]

    # Recent highs (using shift(1) to exclude today)
    high20_yday = df['high'].rolling(20).max().shift(1).iloc[-1]
    high55_yday = df['high'].rolling(55).max().shift(1).iloc[-1]
    vol20 = df['vol'].rolling(20).mean().iloc[-1]
    vol3 = df['vol'].rolling(3).mean().iloc[-1]

    # Price breakout conditions
    cond_price_20 = (pd.notna(high20_yday) and last['close'] >= mo['price_break_20'] * high20_yday)
    cond_price_55 = (pd.notna(high55_yday) and last['close'] >= mo['price_break_55'] * high55_yday)

    # MA conditions
    cond_ma_core = (last['MA10'] > last['MA20']) and (df_ma['MA20'].diff().tail(5).mean() > 0)
    ma5_stack = (last['MA5'] > last['MA10'] > last['MA20'])

    # Volume conditions
    cond_vol_loose = (df['vol'].iloc[-1] >= mo['vol_today_vs_20'] * vol20) or (vol3 >= 1.0 * vol20)

    # Trend strength (ADX or ROC)
    df_adx = compute_adx(df_ma, 14)
    adx_last = float(df_adx['ADX'].iloc[-1])
    adx_rise = float(df_adx['ADX'].diff().tail(5).mean())
    cond_trend_adx = (adx_last > mo['adx_min'] and adx_rise > 0)

    roc3 = (df['close'].iloc[-1] / df['close'].iloc[-4] - 1) if len(df) >= 4 else 0.0
    cond_trend_roc = (roc3 >= mo['roc3_min'])

    # MACD must be positive
    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    macd_last = float(((dif - dea) * 2).iloc[-1])
    cond_macd = (macd_last > 0)

    hit = ((cond_price_20 or cond_price_55) and cond_ma_core and
           (cond_vol_loose or cond_trend_adx or cond_trend_roc) and cond_macd)

    info = {
        "price_20": bool(cond_price_20), "price_55": bool(cond_price_55),
        "ma10_gt_ma20": bool(cond_ma_core), "ma_stack_5_10_20": bool(ma5_stack),
        "vol_ok": bool(cond_vol_loose), "adx_ok": bool(cond_trend_adx),
        "roc3_ok": bool(cond_trend_roc), "macd_pos": bool(cond_macd)
    }

    return hit, info


# ====================== Uptrend Analysis ======================

def judge_up_trend(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Uptrend detection with A-B-C-D pattern, dynamic time window, soft conditions, and momentum override
    """
    df30 = df.tail(30)
    out: Dict[str, Any] = {
        "is_up": False, "status": "rejected",
        "base_score": 0.0, "bonus": 0.0, "total_score": 0.0,
        "a_b_c_points": {}, "conditions": {}, "supports_resist": {}, "explain": ""
    }

    abc = _find_abc_up(df30)
    if not abc:
        out["explain"] = "Insufficient data or no A-B-C pattern detected."
        # Try momentum override
        hit, mi = momentum_override(df)
        if hit:
            status = "pending"
            base_score = 4.0
            df_ma2 = add_mas(df)
            latest2 = df_ma2.iloc[-1]
            atr14_2 = _atr(df, 14).iloc[-1]
            support2 = max(
                float(latest2['MA10']) if pd.notna(latest2['MA10']) else -np.inf,
                float(latest2['MA20']) if pd.notna(latest2['MA20']) else -np.inf,
            )
            resistance2 = float(df['high'].iloc[-1])
            next_target2 = resistance2 + (float(atr14_2) if pd.notna(atr14_2) else 0.0)

            out.update({
                "is_up": True, "status": status,
                "base_score": round(base_score, 2),
                "total_score": round(base_score, 2),
                "supports_resist": {
                    "support": round(float(support2), 4),
                    "resistance": round(float(resistance2), 4),
                    "next_target": round(float(next_target2), 4)
                },
                "explain": out.get("explain", "") + f" | Momentum override triggered: {mi}"
            })
        return out

    ia, a, ib, b, ic, c = abc
    d_pair = _find_d_before_a(df, ia)
    d_val = d_pair[1] if d_pair else None

    loc = df.index.get_loc
    days_ab = (loc(ib) - loc(ia))
    days_bc = (loc(ic) - loc(ib))

    pa, pb, pc = df.loc[ia, 'close'], df.loc[ib, 'close'], df.loc[ic, 'close']
    drop_ab = float((pb - pa) / pa) if pa else 0.0
    rise_bc = float((pc - pb) / pb) if pb else 0.0

    # Dynamic time window
    (bc_min_dyn, bc_max_dyn), speed_info = infer_speed_band(df, mode="up")

    # Strict conditions
    c1_strict = (c > a) and (bc_min_dyn <= days_bc <= bc_max_dyn)
    c2_strict = (d_val is not None) and (b > d_val)
    c3_strict = (days_ab > 0 and days_bc > 0 and days_ab < days_bc)

    after_c = df.loc[ic:].iloc[1:6]
    if len(after_c) == 0:
        c4_strict = None
    else:
        c4_strict = (after_c['close'].min() >= a * 0.97)

    # Soft conditions
    up_soft = DEFAULT_CFG["up_soft"]
    c1_soft = (c >= a) and (up_soft["BC_MIN"] <= days_bc <= up_soft["BC_MAX"])
    c2_soft = (d_val is not None) and (b >= d_val * up_soft["B_OVER_D_TOL"])
    c3_soft = ((days_ab > 0 and days_bc > 0 and days_ab <= up_soft["DAY_RATIO_MAX"] * days_bc) or
               (rise_bc >= up_soft["RISE_RATIO_MIN"] * abs(drop_ab)))

    if len(after_c) == 0:
        c4_soft = None
    else:
        c4_soft = (after_c['close'].min() >= a * up_soft["AFTER_C_FLOOR"])

    # Determine status and base score
    if c1_strict and c2_strict and c3_strict:
        if c4_strict is True:
            base_score, status = 5.0, "confirmed"
        elif c4_strict is None:
            base_score, status = 4.5, "pending"
        else:
            base_score, status = 0.0, "rejected"
    elif c1_soft and c2_soft and c3_soft and c4_soft is not False:
        base_score, status = 4.0, "pending"
    else:
        base_score, status = 0.0, "rejected"

    # Bonus scoring
    bonus = 0.0
    try:
        x_vol = df.loc[ic, 'vol']
        y_vol = df.loc[:ic].iloc[-21:-1]['vol'].mean()
        vol_ok = pd.notna(x_vol) and pd.notna(y_vol) and y_vol > 0 and (x_vol > 1.2 * y_vol)
        bonus += 1.0 if vol_ok else 0.0
        vol_msg = "volume_expansion+1.0" if vol_ok else "normal_volume"
    except Exception:
        vol_msg = "volume_data_missing"

    df_ma = add_mas(df)
    latest = df_ma.iloc[-1]
    ma_basic = (latest['MA5'] > latest['MA10'] > latest['MA20'])
    mab = df_ma.loc[ib, 'MA20'] if ib in df_ma.index else np.nan
    b_support = pd.notna(mab) and (b >= 0.98 * mab)
    ma_score = (0.5 if (ma_basic and b_support) else 0.0)

    ma250_ok = (latest['close'] > latest['MA250']) and (df_ma['MA250'].diff().tail(5).mean() >= 0)
    ma_score += (0.5 if ma250_ok else 0.0)
    bonus += ma_score

    df_adx = compute_adx(df_ma, 14)
    adx_last = float(df_adx['ADX'].iloc[-1])
    adx_trend = float(df_adx['ADX'].diff().tail(5).mean())

    if adx_last > 25 and adx_trend > 0:
        bonus += 1.0
        adx_msg = "ADX>25_rising+1.0"
    elif adx_last < 20:
        bonus -= 0.5
        adx_msg = "ADX<20_-0.5"
    else:
        adx_msg = "ADX_normal"

    total_score = base_score + bonus

    # Key levels
    atr14 = _atr(df, 14).iloc[-1]
    support = max(
        b,
        float(latest['MA10']) if pd.notna(latest['MA10']) else -np.inf,
        float(latest['MA20']) if pd.notna(latest['MA20']) else -np.inf,
    )
    resistance = c
    next_target = c + (float(atr14) if pd.notna(atr14) else 0.0)

    out.update({
        "is_up": status in ("confirmed", "pending"),
        "status": status,
        "base_score": round(float(base_score), 2),
        "bonus": round(float(bonus), 2),
        "total_score": round(float(total_score), 2),
        "a_b_c_points": {
            "a": {"date": str(ia.date()), "price": round(float(a), 4)},
            "b": {"date": str(ib.date()), "price": round(float(b), 4)},
            "c": {"date": str(ic.date()), "price": round(float(c), 4)},
            "d": ({"date": str(d_pair[0].date()), "price": round(float(d_val), 4)} if d_pair else None),
        },
        "conditions": {
            "strict": {"c1": c1_strict, "c2": c2_strict, "c3": c3_strict, "c4": c4_strict},
            "soft": {"c1": c1_soft, "c2": c2_soft, "c3": c3_soft, "c4": c4_soft}
        },
        "supports_resist": {
            "support": round(float(support), 4),
            "resistance": round(float(resistance), 4),
            "next_target": round(float(next_target), 4)
        },
        "explain": (
            f"Strict conditions (c1-c4)=({c1_strict},{c2_strict},{c3_strict},{c4_strict}); "
            f"Soft conditions=({c1_soft},{c2_soft},{c3_soft},{c4_soft}); "
            f"Dynamic time window={bc_min_dyn}-{bc_max_dyn} days (speed={speed_info['label']}, "
            f"mean={speed_info['comp_mean']}, atr%={speed_info['atr_pct']}, score={speed_info['score']}); "
            f"Bonuses: [{vol_msg}, MA+{ma_score:.1f}, {adx_msg}]."
        )
    })

    # Try momentum override if rejected
    if out["status"] == "rejected" or out["base_score"] == 0:
        hit, mi = momentum_override(df)
        if hit:
            status = "pending"
            base_score = max(4.0, float(out.get("base_score", 0.0)))
            df_ma2 = add_mas(df)
            latest2 = df_ma2.iloc[-1]
            atr14_2 = _atr(df, 14).iloc[-1]
            support2 = max(
                float(latest2['MA10']) if pd.notna(latest2['MA10']) else -np.inf,
                float(latest2['MA20']) if pd.notna(latest2['MA20']) else -np.inf,
            )
            resistance2 = float(df['high'].iloc[-1])
            next_target2 = resistance2 + (float(atr14_2) if pd.notna(atr14_2) else 0.0)

            out.update({
                "is_up": True,
                "status": status,
                "base_score": round(base_score, 2),
                "total_score": round(base_score + float(out.get("bonus", 0.0)), 2),
                "supports_resist": {
                    "support": round(float(support2), 4),
                    "resistance": round(float(resistance2), 4),
                    "next_target": round(float(next_target2), 4)
                },
                "explain": out.get("explain", "") + f" | Momentum override triggered: {mi}"
            })

    return out


# ====================== Downtrend Analysis ======================

def judge_down_trend(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Downtrend detection (mirror of uptrend)
    A=stage low → B=rebound high → C=new low
    """
    df30 = df.tail(30)
    out: Dict[str, Any] = {
        "is_down": False, "status": "rejected",
        "details": "", "a_b_c_points": {}, "conditions": {}, "supports_resist": {}
    }

    if len(df30) < 15:
        out["details"] = "Insufficient data."
        return out

    head = df30.iloc[:-3]
    ia = head['low'].idxmin()
    a = float(df30.loc[ia, 'low'])

    tail_after_a = df30.loc[ia:]
    if len(tail_after_a) < 5:
        out["details"] = "Cannot construct A-B-C pattern."
        return out

    ib = tail_after_a['high'].idxmax()
    if ib <= ia:
        out["details"] = "Cannot construct A-B-C pattern."
        return out
    b = float(df30.loc[ib, 'high'])

    tail_after_b = df30.loc[ib:]
    if len(tail_after_b) < 5:
        out["details"] = "Cannot construct A-B-C pattern."
        return out

    ic = tail_after_b['low'].idxmin()
    if ic <= ib:
        out["details"] = "Cannot construct A-B-C pattern."
        return out
    c = float(df30.loc[ic, 'low'])

    loc = df.index.get_loc
    days_bc = (loc(ic) - loc(ib))
    days_ab = (loc(ib) - loc(ia))

    pa, pb, pc = df.loc[ia, 'close'], df.loc[ib, 'close'], df.loc[ic, 'close']
    rise_ab = float((pb - pa) / pa) if pa else 0.0
    drop_bc = float((pc - pb) / pb) if pb else 0.0

    # Dynamic time window
    (bc_min_dyn, bc_max_dyn), speed_info = infer_speed_band(df, mode="down")

    # Strict conditions
    c1_strict = (c < a) and (bc_min_dyn <= days_bc <= bc_max_dyn)

    left = df.loc[:ia].iloc[:-1].tail(30)
    d2 = float(left['high'].max()) if not left.empty else np.nan
    c2_strict = pd.notna(d2) and (b < d2)
    c3_strict = (days_ab > 0 and days_bc > 0 and days_ab < days_bc)

    after_c = df.loc[ic:].iloc[1:6]
    if len(after_c) == 0:
        c4_strict = None
    else:
        c4_strict = (after_c['close'].max() <= a * 1.02)

    # Soft conditions
    down_soft = DEFAULT_CFG["down_soft"]
    c1_soft = (c <= a) and (down_soft["BC_MIN"] <= days_bc <= down_soft["BC_MAX"])
    c2_soft = pd.notna(d2) and (b <= d2 * down_soft["B_UNDER_D_TOL"])
    c3_soft = ((days_ab > 0 and days_bc > 0 and days_ab <= down_soft["DAY_RATIO_MAX"] * days_bc) or
               (abs(drop_bc) >= down_soft["DROP_RATIO_MIN"] * rise_ab))

    if len(after_c) == 0:
        c4_soft = None
    else:
        c4_soft = (after_c['close'].max() <= a * down_soft["AFTER_C_CAP"])

    if c1_strict and c2_strict and c3_strict:
        status = "confirmed" if c4_strict is True else ("pending" if c4_strict is None else "rejected")
    elif c1_soft and c2_soft and c3_soft and c4_soft is not False:
        status = "pending"
    else:
        status = "rejected"

    is_down = status in ("confirmed", "pending")

    # Key levels
    ma20 = df['close'].rolling(20).mean().iloc[-1] if len(df) >= 20 else df['close'].iloc[-1]
    atr14 = _atr(df, 14).iloc[-1]
    support = min(c, float(ma20) if pd.notna(ma20) else c)
    resistance = b
    next_target = c - (float(atr14) if pd.notna(atr14) else 0.0)

    out.update({
        "is_down": is_down,
        "status": status,
        "a_b_c_points": {
            "a": {"date": str(ia.date()), "price": round(a, 4)},
            "b": {"date": str(ib.date()), "price": round(b, 4)},
            "c": {"date": str(ic.date()), "price": round(c, 4)}
        },
        "conditions": {
            "strict": {"c1": c1_strict, "c2": c2_strict, "c3": c3_strict, "c4": c4_strict},
            "soft": {"c1": c1_soft, "c2": c2_soft, "c3": c3_soft, "c4": c4_soft}
        },
        "supports_resist": {
            "support": round(float(support), 4),
            "resistance": round(float(resistance), 4),
            "next_target": round(float(next_target), 4)
        },
        "details": (
            f"Downtrend: strict=({c1_strict},{c2_strict},{c3_strict},{c4_strict}); "
            f"soft=({c1_soft},{c2_soft},{c3_soft},{c4_soft}); "
            f"Dynamic time window={bc_min_dyn}-{bc_max_dyn} days (speed={speed_info['label']}, "
            f"mean={speed_info['comp_mean']}, atr%={speed_info['atr_pct']}, score={speed_info['score']})"
        )
    })

    return out


# ====================== Main Tool Function ======================

async def technical_analyze_stock_fmp_impl(
    symbol: str,
    start_date: str,
    end_date: str,
    benchmark: str = "SPY"
) -> str:
    """
    Technical analysis for stocks using FMP API (async).

    Args:
        symbol: Stock ticker (e.g., "AAPL", "600519.SS", "0700.HK")
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        benchmark: Benchmark symbol for beta calculation (default: SPY)

    Returns:
        Markdown-formatted string with technical analysis including indicators, trend, and chart
    """
    symbol = symbol.upper()

    async with FMPClient() as fmp_client:
        # Fetch profile, price data, and metrics in parallel for HTTP/2 multiplexing
        try:
            results = await asyncio.gather(
                fmp_client.get_profile(symbol),
                fmp_client.get_stock_price(symbol, from_date=start_date, to_date=end_date),
                fmp_client.get_key_metrics_ttm(symbol),
                return_exceptions=True
            )
            profile_data, price_data, metrics_data = results
        except Exception as e:
            return f"""## Technical Analysis: {symbol}
**Status:** Error

Failed to fetch data: {str(e)}"""

        # Handle profile errors
        if isinstance(profile_data, Exception):
            return f"""## Technical Analysis: {symbol}
**Status:** Error

Failed to retrieve profile for {symbol}: {str(profile_data)}"""
        if not profile_data:
            return f"""## Technical Analysis: {symbol}
**Status:** Error

Unable to find stock profile for {symbol}. Please verify the ticker symbol is correct."""
        profile = profile_data[0]

        # Handle price data errors
        if isinstance(price_data, Exception):
            return f"""## Technical Analysis: {symbol}
**Status:** Error

Failed to retrieve price data: {str(price_data)}"""
        if not price_data:
            return f"""## Technical Analysis: {symbol}
**Status:** Error
**Period:** {start_date} to {end_date}

No price data found for {symbol} in the specified date range. Try a different date range."""

        # Convert to DataFrame
        df = pd.DataFrame(price_data)
        df['trade_date'] = pd.to_datetime(df['date'])
        df = df.rename(columns={'volume': 'vol'})
        df = df[['trade_date', 'open', 'high', 'low', 'close', 'vol']]
        df = df.dropna(subset=['close', 'open', 'high', 'low']).set_index('trade_date')
        df = df.sort_index()

        if len(df) < 30:
            return f"""## Technical Analysis: {symbol}
**Status:** Error
**Period:** {start_date} to {end_date}

Insufficient data for trend analysis. Only {len(df)} trading days available, but need at least 30 for reliable analysis. Please expand the date range."""

        # Handle metrics (optional, don't fail if unavailable)
        if isinstance(metrics_data, Exception) or not metrics_data:
            metrics = {}
        else:
            metrics = metrics_data[0] if metrics_data else {}

    # Calculate technical indicators
    macd = calculate_macd(df)
    kdj = calculate_kdj(df)
    rsi = calculate_rsi(df)

    # Trend analysis
    up = judge_up_trend(df)
    if up["is_up"]:
        down = {"is_down": False, "status": "rejected"}
    else:
        down = judge_down_trend(df)

    # Find recent KDJ crosses
    crosses = _find_cross_dates_kdj(df, lookback=10)

    # Industry info
    industry_info = {
        "sector": profile.get('sector'),
        "industry": profile.get('industry')
    }

    # Build trend conclusion
    latest_dt = df.index[-1]
    latest_close = float(df['close'].iloc[-1])
    company_name = profile.get('companyName', symbol)

    if up["is_up"]:
        trend_status = " (breakout day, pending confirmation)" if up.get("status") == "pending" else " (confirmed)"
        buy_dt = crosses.get("kdj_buy")
        signal_text = f"buy signal on {buy_dt.strftime('%m/%d')}" if buy_dt else "no clear buy signal yet"
        sup = up["supports_resist"]["support"]
        res = up["supports_resist"]["resistance"]
        nxt = up["supports_resist"]["next_target"]
        headline = f"We judge {symbol} in uptrend{trend_status}, {signal_text}, watch resistance at ${res:.2f}, next target ${nxt:.2f}, support at ${sup:.2f}."

        trend_block = {
            "conclusion": headline,
            "type": "uptrend",
            "status": up["status"],
            "score": {
                "base_score": up["base_score"],
                "bonus": up["bonus"],
                "total_score": up["total_score"],
                "conditions_met": up["conditions"],
                "key_points": up["a_b_c_points"]
            },
            "details": up["explain"],
            "key_levels": {"support": sup, "resistance": res, "next_target": nxt}
        }
    elif down["is_down"]:
        trend_status = " (breakdown day, pending confirmation)" if down.get("status") == "pending" else " (confirmed)"
        sell_dt = crosses.get("kdj_sell")
        signal_text = f"sell signal on {sell_dt.strftime('%m/%d')}" if sell_dt else "no clear sell signal yet"
        sup = down["supports_resist"]["support"]
        res = down["supports_resist"]["resistance"]
        nxt = down["supports_resist"]["next_target"]
        headline = f"We judge {symbol} in downtrend{trend_status}, {signal_text}, watch resistance at ${res:.2f}, next target ${nxt:.2f}, support at ${sup:.2f}."

        trend_block = {
            "conclusion": headline,
            "type": "downtrend",
            "status": down["status"],
            "score": {"base_score": 0, "bonus": 0, "total_score": 0, "conditions_met": down["conditions"], "key_points": down["a_b_c_points"]},
            "details": down["details"],
            "key_levels": {"support": sup, "resistance": res, "next_target": nxt}
        }
    else:
        # Sideways/no clear trend
        atr = _atr(df, 14).iloc[-1]
        sup = latest_close - 0.5 * atr
        res = latest_close + 0.5 * atr
        ma20 = df['close'].rolling(20).mean().iloc[-1] if len(df) >= 20 else latest_close
        sup = min(sup, ma20)
        res = max(res, ma20)

        headline = f"We judge {symbol} in sideways/no clear trend, no clear buy/sell signals, trading range reference [${sup:.2f}, ${res:.2f}]."
        trend_block = {
            "conclusion": headline,
            "type": "sideways",
            "status": "none",
            "score": {"base_score": 0, "bonus": 0, "total_score": 0},
            "details": "Does not meet uptrend or downtrend core conditions.",
            "key_levels": {"support": sup, "resistance": res}
        }

    # Summary
    beta = profile.get('beta', np.nan)
    beta_str = "N/A" if pd.isna(beta) else round(beta, 2)
    summary = (
        f"{symbol} ({company_name}) as of {latest_dt.strftime('%Y-%m-%d')}; "
        f"Close: ${latest_close:.2f}; "
        f"MACD: {macd['signal']} ({macd['trend']}); "
        f"KDJ: {kdj['signal']}; "
        f"RSI: {rsi['signal']}; "
        f"Beta={beta_str}; "
        f"Trend: {trend_block['type']}"
    )
    if trend_block['status'] in ('pending', 'confirmed'):
        summary += f" ({trend_block['status']})"

    # Generate chart
    chart_dir = "src/tools/temp_data"
    os.makedirs(chart_dir, exist_ok=True)

    file_uuid = str(uuid.uuid4())[:8]
    os.environ['FILE_UUID_NAME'] = file_uuid

    df_plot = df.copy()
    df_plot.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "vol": "Volume"}, inplace=True)

    try:
        fig, _ = mpf.plot(df_plot, type='candle', volume=True, mav=(5, 10, 20), returnfig=True, style='yahoo', title=f"{symbol} Technical Analysis")
        filename = f"{file_uuid}_{symbol.replace('.', '_')}_kline.png"
        filepath = os.path.join(chart_dir, filename)
        fig.savefig(filepath)
        chart_files = [filename]
    except Exception as e:
        chart_files = []
        filepath = None

    # Upload to OSS
    chart_url = filepath
    oss_upload_status = {"status": "no_charts", "uploaded_count": 0, "error": None}

    if chart_files:
        try:
            from src.tools.utils.chart_uploader import upload_and_cleanup_charts_by_prefix
            uploaded_urls = upload_and_cleanup_charts_by_prefix(chart_dir, file_uuid)
            if uploaded_urls:
                chart_files = [url.split('/')[-1] for url in uploaded_urls]
                oss_upload_status = {"status": "success", "uploaded_count": len(uploaded_urls), "error": None}
                chart_url = uploaded_urls[0]
        except ImportError:
            oss_upload_status = {"status": "failed", "uploaded_count": 0, "error": "OSS module not available"}
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"OSS upload failed: {str(e)}")
            oss_upload_status = {"status": "failed", "uploaded_count": 0, "error": str(e)[:200]}

    # Build markdown formatted output
    from datetime import datetime, timezone as dt_timezone

    output_lines = []

    # Header
    timestamp_utc = datetime.now(dt_timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    output_lines.append(f"## Technical Analysis: {symbol}")
    output_lines.append(f"**Company:** {company_name}")
    output_lines.append(f"**Retrieved:** {timestamp_utc}")
    output_lines.append(f"**Period:** {start_date} to {end_date}")
    output_lines.append(f"**Industry:** {industry_info.get('sector')} / {industry_info.get('industry')}")
    output_lines.append("")

    # Latest Quote
    output_lines.append(f"**Latest Close:** ${latest_close:.2f} ({latest_dt.strftime('%Y-%m-%d')})")
    output_lines.append(f"**Beta:** {beta_str} (vs {benchmark})")
    output_lines.append("")

    # Valuation Metrics
    output_lines.append("### Valuation Metrics")
    output_lines.append("")
    val_rows = []
    pe_ratio = metrics.get('peRatioTTM')
    pb_ratio = metrics.get('pbRatioTTM')
    ps_ratio = metrics.get('priceToSalesRatioTTM')
    mkt_cap = profile.get('mktCap', 0) / 1_000_000 if profile.get('mktCap') else None

    if pe_ratio:
        val_rows.append(("P/E Ratio (TTM)", f"{pe_ratio:.2f}"))
    if pb_ratio:
        val_rows.append(("P/B Ratio (TTM)", f"{pb_ratio:.2f}"))
    if ps_ratio:
        val_rows.append(("P/S Ratio (TTM)", f"{ps_ratio:.2f}"))
    if mkt_cap:
        val_rows.append(("Market Cap", f"${mkt_cap:,.1f}M"))

    if val_rows:
        output_lines.append("| Metric | Value |")
        output_lines.append("|--------|-------|")
        for metric, value in val_rows:
            output_lines.append(f"| {metric} | {value} |")
    else:
        output_lines.append("*No valuation data available*")
    output_lines.append("")

    # Technical Indicators
    output_lines.append("### Technical Indicators")
    output_lines.append("")
    output_lines.append("| Indicator | Value | Signal | Trend |")
    output_lines.append("|-----------|-------|--------|-------|")

    # MACD
    macd_val = macd.get('value', 'N/A')
    macd_val_str = f"{macd_val:.4f}" if isinstance(macd_val, (int, float)) else macd_val
    output_lines.append(f"| MACD | {macd_val_str} | {macd['signal']} | {macd.get('trend', 'N/A')} |")

    # KDJ
    kdj_k = kdj.get('k', 'N/A')
    kdj_d = kdj.get('d', 'N/A')
    kdj_j = kdj.get('j', 'N/A')
    kdj_str = f"K:{kdj_k:.1f}, D:{kdj_d:.1f}, J:{kdj_j:.1f}" if all(isinstance(x, (int, float)) for x in [kdj_k, kdj_d, kdj_j]) else "N/A"
    output_lines.append(f"| KDJ | {kdj_str} | {kdj['signal']} | - |")

    # RSI
    rsi_val = rsi.get('value', 'N/A')
    rsi_val_str = f"{rsi_val:.2f}" if isinstance(rsi_val, (int, float)) else rsi_val
    output_lines.append(f"| RSI (14) | {rsi_val_str} | {rsi['signal']} | - |")
    output_lines.append("")

    # Trend Analysis
    output_lines.append("### Trend Analysis")
    output_lines.append("")
    output_lines.append(f"**Conclusion:** {trend_block['conclusion']}")
    output_lines.append("")
    output_lines.append(f"**Trend Type:** {trend_block['type'].upper()}")
    if trend_block['status'] != 'none':
        output_lines.append(f"**Status:** {trend_block['status']}")
    output_lines.append("")

    # Key Levels
    output_lines.append("**Key Price Levels:**")
    output_lines.append("")
    levels_rows = []
    if 'support' in trend_block['key_levels']:
        levels_rows.append(("Support", f"${trend_block['key_levels']['support']:.2f}"))
    if 'resistance' in trend_block['key_levels']:
        levels_rows.append(("Resistance", f"${trend_block['key_levels']['resistance']:.2f}"))
    if 'next_target' in trend_block['key_levels']:
        levels_rows.append(("Next Target", f"${trend_block['key_levels']['next_target']:.2f}"))

    if levels_rows:
        output_lines.append("| Level | Price |")
        output_lines.append("|-------|-------|")
        for level, price in levels_rows:
            output_lines.append(f"| {level} | {price} |")
    output_lines.append("")

    # Trend Score (if available)
    if trend_block['score']['total_score'] > 0:
        output_lines.append(f"**Trend Score:** {trend_block['score']['total_score']} ")
        output_lines.append(f"(Base: {trend_block['score']['base_score']}, Bonus: {trend_block['score']['bonus']})")
        output_lines.append("")

    # Details
    if trend_block.get('details'):
        output_lines.append("**Analysis Details:**")
        output_lines.append("")
        output_lines.append(trend_block['details'])
        output_lines.append("")

    # Summary
    output_lines.append("### Summary")
    output_lines.append("")
    output_lines.append(summary)
    output_lines.append("")

    # Charts
    if chart_files:
        output_lines.append("### Generated Charts")
        output_lines.append("")
        if oss_upload_status.get('status') == 'success':
            output_lines.append(f"**Chart URL:** {chart_url}")
            output_lines.append(f"**Files:** {', '.join(chart_files)}")
        else:
            output_lines.append(f"**Local Files:** {', '.join(chart_files)}")
        output_lines.append("")

    return "\n".join(output_lines)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="AAPL", help="Stock symbol")
    parser.add_argument("--start", default="2024-01-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default="2024-12-31", help="End date YYYY-MM-DD")
    parser.add_argument("--benchmark", default="SPY", help="Benchmark symbol")
    args = parser.parse_args()

    res = technical_analyze_stock_fmp_impl(
        symbol=args.symbol,
        start_date=args.start,
        end_date=args.end,
        benchmark=args.benchmark
    )

    print("\n=== Technical Analysis Summary ===")
    print(res["summary"])
    print(f"\nTrend: {res['trend']['conclusion']}")
    print(f"Chart: {res.get('chart_url', 'N/A')}")
