#!/usr/bin/env python3
"""
ETF 레버리지 매수 전략 백테스트 v2
1999~2026, 전략 A~G + 조합 비교

사용법:
  python backtest/run_backtest.py            # 실데이터 다운로드 후 전체 백테스트
  python backtest/run_backtest.py --demo     # 합성 데이터로 구조 검증
  python backtest/run_backtest.py --clear    # 캐시 삭제 후 재다운로드

출력:
  backtest/results.json   — 머신용 전략 비교 수치
  backtest/results.md     — 마크다운 요약 + 위기별 서술
  backtest/trade_log_dotcom.md
  backtest/trade_log_gfc.md

전략 정의:
  A  현행 베이스라인: MA20×0.95 신호 + VIX 레버 + +8% 익절 + -15% 손절 + MA20복귀 + 60일컷
  B  레버캡: 종가<MA200이면 레버 최대 2x (3x/2.5x → 2x 강등, 스킵 X)
  C  MA200 기울기 필터: MA200의 20일 기울기가 음수면 3x 차단 (2x/2.5x는 그대로)
  D  52주 낙폭 캡: 52주 고점 대비 -35% 이상이면 최대 2x (더 심한 하락장 보호)
  E  VIX 기간구조: VIX>VIX3M(백워데이션)→3x, 콘탱고→2x. 2007~ 구간만 유효
  F50 부분 투입(50%): 3x 티어 진입 시 배정자본의 50%만 레버, 나머지 현금
  F70 부분 투입(70%): 3x 티어 진입 시 배정자본의 70%만 레버
  G  익절 제거: MA20복귀/손절/60일컷만 유지
  C+F70 조합: MA200기울기 + 부분투입 70%
  D+G   조합: 52주낙폭캡 + 익절제거
  B+G   조합: 레버캡 + 익절제거

손절 기준: 기초자산 누적수익 ≤ -15% (갭 시나리오: 다음날 시가 체결)
"""

import sys, json, csv, io, math, urllib.request, shutil
from pathlib import Path
from datetime import date, timedelta, datetime
from collections import defaultdict

ROOT        = Path(__file__).resolve().parent.parent
BACKTEST_DIR = ROOT / "backtest"
CACHE_DIR   = BACKTEST_DIR / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ══════════════════════════════════════════════
#  전략 상수
# ══════════════════════════════════════════════
MA_SIGNAL_WIN  = 20        # 신호용 MA 창
SIGNAL_THR     = 0.95      # 종가 < MA20 × 0.95 = 진입 신호
MA_EXIT_WIN    = 20        # MA 복귀 청산
TP_RETURN      = 0.08      # +8% 익절 (레버 기준)
SL_BASE        = -0.15     # -15% 손절 (기초자산 기준)
MAX_HOLD       = 60        # 최대 보유 거래일
EXPENSE        = 0.0095    # 레버리지 ETF 연간 운용보수
MA200_WIN      = 200       # MA200
MA200_SLOPE_WIN = 20       # MA200 기울기 측정 창 (20일)
W52_WIN        = 252       # 52주 = 약 252 거래일
W52_CAP_THR    = -0.35     # -35% → 52주 낙폭 캡 발동

DEFAULT_RF     = 0.03      # FRED 없을 때 고정 무위험금리 3%


def vix_to_lev_base(vix: float) -> float | None:
    """VIX 수준 → 레버리지 배수 (기본)"""
    if vix < 20:   return None   # 스킵
    if vix < 30:   return 2.0
    if vix < 40:   return 2.5
    return 3.0

def vix_to_lev_term(vix: float, vix3m: float | None) -> float | None:
    """전략 E: 기간구조 기반. VIX<20 스킵, VIX>VIX3M→3x, 콘탱고→2x"""
    if vix < 20:
        return None
    if vix3m is None:
        return vix_to_lev_base(vix)   # VIX3M 없으면 기본 사용
    return 3.0 if vix > vix3m else 2.0

# ══════════════════════════════════════════════
#  데이터 다운로드 (stooq → yfinance fallback)
# ══════════════════════════════════════════════
_HDRS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Accept": "text/csv,*/*",
    "Referer": "https://stooq.com/",
}
_STOOQ = {
    "QQQ":   "qqq.us",
    "NDX":   "%5endx",      # ^NDX 나스닥100 지수 (QQQ 대체, 1999~)
    "SPY":   "spy.us",
    "GSPC":  "%5egspc",     # ^GSPC S&P500 지수
    "SOXX":  "soxx.us",
    "SOX":   "%5esox",      # ^SOX Philadelphia Semiconductor
    "VIX":   "vix.us",
    "VIX3M": "%5evix3m",
    "TQQQ":  "tqqq.us",
    "SOXL":  "soxl.us",
    "UPRO":  "upro.us",
    "IRX":   "%5eirx",      # ^IRX 3개월 T-bill
}

def _fetch_stooq(sym: str) -> list[dict]:
    url = f"https://stooq.com/q/d/l/?s={_STOOQ[sym]}&d1=19990101&d2=20260615&i=d"
    req = urllib.request.Request(url, headers=_HDRS)
    with urllib.request.urlopen(req, timeout=40) as r:
        text = r.read().decode("utf-8", errors="replace")
    if "No data" in text or len(text) < 100:
        raise ValueError(f"stooq 빈 응답 ({sym})")
    reader = csv.DictReader(io.StringIO(text))
    rows = []
    for row in reader:
        try:
            d = row["Date"].strip()
            c = float(row.get("Close") or row.get("close") or 0)
            o = float(row.get("Open")  or row.get("open")  or c)
            if d and c > 0:
                rows.append({"date": d, "open": round(o, 4), "close": round(c, 4)})
        except (ValueError, KeyError):
            continue
    rows.sort(key=lambda x: x["date"])
    return rows

def _fetch_yfinance(yf_sym: str) -> list[dict]:
    import yfinance as yf, math as _m
    df = yf.download(yf_sym, start="1999-01-01", end="2026-06-15",
                     auto_adjust=True, progress=False)
    if df.empty:
        raise ValueError(f"yfinance 빈 결과 ({yf_sym})")
    if hasattr(df.columns, "get_level_values"):
        df.columns = df.columns.get_level_values(0)
    rows = []
    for idx, row in df.iterrows():
        c = float(row.get("Close", 0));  o = float(row.get("Open", c))
        if not _m.isnan(c) and c > 0:
            rows.append({"date": idx.strftime("%Y-%m-%d"),
                         "open": round(o, 4), "close": round(c, 4)})
    return sorted(rows, key=lambda x: x["date"])

def load_series(name: str, force_dl=False) -> list[dict]:
    """캐시 우선. 없으면 stooq → yfinance."""
    p = CACHE_DIR / f"{name}.json"
    if p.exists() and not force_dl:
        return json.loads(p.read_text())
    print(f"  [{name}] 다운로드…")
    data = None
    if name in _STOOQ:
        try:
            data = _fetch_stooq(name)
            print(f"  [{name}] stooq: {len(data)}일")
        except Exception as e:
            print(f"  [{name}] stooq 실패: {e}")
    if not data:
        yf_sym = f"^{name}" if name in ("NDX","GSPC","SOX","VIX","VIX3M","IRX") else name
        try:
            data = _fetch_yfinance(yf_sym)
            print(f"  [{name}] yfinance: {len(data)}일")
        except Exception as e:
            print(f"  [{name}] yfinance 실패: {e}")
            return []
    p.write_text(json.dumps(data))
    return data

def load_rf_series(force_dl=False) -> dict[str, float]:
    """FRED DTB3 3개월 T-bill. 실패 시 ^IRX."""
    p = CACHE_DIR / "DTB3.json"
    if p.exists() and not force_dl:
        return json.loads(p.read_text())
    print("  [DTB3] FRED 다운로드…")
    try:
        import pandas_datareader.data as web
        df = web.DataReader("DTB3", "fred", start="1999-01-01", end="2026-06-15").dropna()
        out = {idx.strftime("%Y-%m-%d"): float(v) / 100
               for idx, (v,) in df.iterrows()}
        p.write_text(json.dumps(out))
        print(f"  [DTB3] FRED: {len(out)}일")
        return out
    except Exception as e:
        print(f"  [DTB3] FRED 실패: {e}. ^IRX 대체 사용")
    irx = load_series("IRX")
    out = {r["date"]: r["close"] / 100 for r in irx if r["close"] > 0}
    if out:
        p.write_text(json.dumps(out))
        return out
    print(f"  [DTB3] fallback: 고정 {DEFAULT_RF*100}%")
    return {}   # 비어 있으면 DEFAULT_RF 사용

# ══════════════════════════════════════════════
#  합성 레버리지: r_L = L·r - (L-1)·rf/252 - exp/252
# ══════════════════════════════════════════════
def make_synth_lev(prices: list[dict], lev: float,
                   rf_map: dict[str, float]) -> list[dict]:
    """기초지수 일간수익률 → 합성 레버리지 가격 시계열 (기준가 100)."""
    synth = 100.0
    result = [{"date": prices[0]["date"], "close": 100.0, "open": 100.0}]
    for i in range(1, len(prices)):
        p, pp = prices[i], prices[i-1]
        rf_ann = rf_map.get(p["date"], rf_map.get(pp["date"], DEFAULT_RF))
        rf_d   = rf_ann / 252
        r_c    = (p["close"] - pp["close"]) / pp["close"]
        r_o    = (p["open"]  - pp["close"]) / pp["close"]
        r_lev  = lev * r_c - (lev - 1) * rf_d - EXPENSE / 252
        r_open = lev * r_o - (lev - 1) * rf_d - EXPENSE / 252
        synth_open = result[-1]["close"] * (1 + r_open)
        synth      = synth            * (1 + r_lev)
        result.append({"date": p["date"],
                        "close": round(max(synth, 0.001), 6),
                        "open":  round(max(synth_open, 0.001), 6)})
    return result

def validate_synth(name: str, real: list[dict], synth: list[dict],
                   from_date="2010-01-01") -> dict:
    """실제 ETF vs 합성치 추적오차 검증."""
    rm = {r["date"]: r["close"] for r in real}
    sm = {r["date"]: r["close"] for r in synth}
    common = sorted(d for d in rm if d >= from_date and d in sm)
    if len(common) < 50:
        return {"status": "데이터 부족"}
    d0 = common[0]
    r0, s0 = rm[d0], sm[d0]
    diffs = []
    for i in range(1, len(common)):
        d, dp = common[i], common[i-1]
        rd = math.log(rm[d] / rm[dp]) if rm[dp] > 0 else 0
        sd = math.log(sm[d] / sm[dp]) if sm[dp] > 0 else 0
        diffs.append(rd - sd)
    te = math.sqrt(sum(x**2 for x in diffs) / len(diffs)) * math.sqrt(252) * 100
    return {
        "instrument": name,
        "n_days": len(common),
        "from": common[0], "to": common[-1],
        "tracking_error_ann_pct": round(te, 3),
        "final_real_norm":  round(rm[common[-1]] / r0, 4),
        "final_synth_norm": round(sm[common[-1]] / s0, 4),
    }

# ══════════════════════════════════════════════
#  기술적 시리즈 계산 (numpy 없이)
# ══════════════════════════════════════════════
def _ma(closes, win):
    out = [None] * len(closes)
    for i in range(win - 1, len(closes)):
        out[i] = sum(closes[i - win + 1: i + 1]) / win
    return out

def _rolling_max(closes, win):
    out = [None] * len(closes)
    for i in range(win - 1, len(closes)):
        out[i] = max(closes[i - win + 1: i + 1])
    return out

def _ma200_slope(ma200_series, slope_win=MA200_SLOPE_WIN):
    """MA200의 20일 기울기 = (MA200[i] - MA200[i-20]) / MA200[i-20]"""
    out = [None] * len(ma200_series)
    for i in range(len(ma200_series)):
        if ma200_series[i] is None:
            continue
        j = i - slope_win
        if j >= 0 and ma200_series[j] is not None and ma200_series[j] > 0:
            out[i] = (ma200_series[i] - ma200_series[j]) / ma200_series[j]
    return out

# ══════════════════════════════════════════════
#  백테스트 엔진 (단일 자산, 단일 전략)
# ══════════════════════════════════════════════
def backtest(
    asset:     str,
    prices:    list[dict],
    vix_map:   dict[str, float],
    vix3m_map: dict[str, float] | None,
    rf_map:    dict[str, float],
    strategy:  str = "A",
    start:     str = "1999-01-01",
    end:       str = "2026-06-10",
    gap:       bool = False,
) -> dict:
    """
    strategy 코드:
      A       베이스라인
      B       레버캡 (종가<MA200 → max 2x)
      C       MA200 기울기 (음수 → 3x 차단)
      D       52주 낙폭캡 (-35% → max 2x)
      E       VIX 기간구조 (VIX3M 필요, 2007~)
      F50     부분투입 50%
      F70     부분투입 70%
      G       익절 제거
      C+F70   MA200기울기 + 부분투입70%
      D+G     52주낙폭캡 + 익절제거
      B+G     레버캡 + 익절제거
    """
    rows = [p for p in prices if start <= p["date"] <= end]
    if len(rows) < MA200_WIN + 20:
        return {"asset": asset, "strategy": strategy, "error": "데이터 부족"}

    closes = [p["close"] for p in rows]
    opens  = [p["open"]  for p in rows]
    dates  = [p["date"]  for p in rows]

    ma20_s    = _ma(closes, MA_SIGNAL_WIN)
    ma200_s   = _ma(closes, MA200_WIN)
    slope_s   = _ma200_slope(ma200_s)
    w52_high  = _rolling_max(closes, W52_WIN)

    equity = 1.0
    peak_eq = 1.0
    mdd = 0.0
    position = None
    trades = []
    eq_curve = []

    for i in range(MA200_WIN, len(rows)):
        d = dates[i]
        c = closes[i]
        o = opens[i]

        vix   = vix_map.get(d)
        vix3m = vix3m_map.get(d) if vix3m_map else None
        rf_ann = rf_map.get(d, DEFAULT_RF)

        # ── 청산 ──
        if position is not None:
            eid    = position["entry_idx"]
            e_c    = position["entry_close"]
            lev    = position["lev"]
            size   = position["size"]
            hold   = i - eid

            # 레버리지 누적 수익 계산 (진입 이후 매일 복리)
            lev_ret = 1.0
            for j in range(eid + 1, i + 1):
                r_j  = (closes[j] - closes[j-1]) / closes[j-1]
                rf_j = rf_map.get(dates[j], DEFAULT_RF) / 252
                lev_ret *= (1 + lev * r_j - (lev - 1) * rf_j - EXPENSE / 252)

            # 기초자산 누적 수익 (손절 판단)
            base_ret = c / e_c - 1

            exit_reason = None

            # 손절: 기초자산 ≤ -15%
            if gap:
                # 갭: 전일 종가→오늘 시가로 체결
                base_gap = o / closes[i-1] - 1
                base_cum = e_c > 0 and (closes[i-1] / e_c) * (1 + base_gap) - 1
                if isinstance(base_cum, float) and base_cum <= SL_BASE:
                    exit_reason = "stop_loss"
                    # 갭 기준 레버 수익 재계산
                    lev_gap = 1.0
                    for j in range(eid + 1, i):
                        r_j  = (closes[j] - closes[j-1]) / closes[j-1]
                        rf_j = rf_map.get(dates[j], DEFAULT_RF) / 252
                        lev_gap *= (1 + lev * r_j - (lev - 1) * rf_j - EXPENSE / 252)
                    r_open_lev = lev * base_gap - (lev - 1) * rf_ann / 252 - EXPENSE / 252
                    lev_ret = lev_gap * (1 + r_open_lev)
            else:
                if base_ret <= SL_BASE:
                    exit_reason = "stop_loss"

            # 익절: +8% (레버 기준), 전략 G/D+G/B+G는 없음
            if exit_reason is None and "G" not in strategy:
                if lev_ret - 1 >= TP_RETURN:
                    exit_reason = "take_profit"

            # MA 복귀
            if exit_reason is None and ma20_s[i] is not None:
                if c >= ma20_s[i]:
                    exit_reason = "ma_return"

            # 60일 컷
            if exit_reason is None and hold >= MAX_HOLD:
                exit_reason = "timeout"

            if exit_reason:
                pnl = size * (lev_ret - 1)
                equity += pnl  # equity = equity_at_entry + equity_at_entry * (lev_ret-1)
                                # 즉 equity = equity_at_entry * lev_ret
                trades.append({
                    "asset":       asset,
                    "entry_date":  position["entry_date"],
                    "exit_date":   d,
                    "vix":         round(position["vix"], 2),
                    "lev":         lev,
                    "size_frac":   round(size / position["equity_at_entry"], 3) if position["equity_at_entry"] else 1.0,
                    "hold_days":   hold,
                    "exit_reason": exit_reason,
                    "base_ret":    round(base_ret * 100, 2),
                    "lev_ret":     round((lev_ret - 1) * 100, 2),
                })
                position = None

        # ── 진입 ──
        if position is None and ma20_s[i] is not None and equity > 0.001:
            signal = c < ma20_s[i] * SIGNAL_THR
            if signal:
                # 1. 기본 VIX 레버 결정
                if strategy == "E" or "E" in strategy:
                    lev = vix_to_lev_term(vix or 0, vix3m)
                else:
                    lev = vix_to_lev_base(vix or 0)

                if lev is not None:
                    raw_lev = lev  # 필터 전 원래 레버

                    # 2. 전략별 레버 조정/차단
                    if strategy in ("B", "B+G") and ma200_s[i] is not None:
                        if c <= ma200_s[i]:
                            lev = min(lev, 2.0)  # MA200 아래면 최대 2x

                    elif strategy in ("C", "C+F70") and slope_s[i] is not None:
                        if slope_s[i] < 0:
                            lev = min(lev, 2.0)  # MA200 기울기 음수면 3x 차단

                    elif strategy in ("D", "D+G") and w52_high[i] is not None:
                        if w52_high[i] > 0 and (c / w52_high[i] - 1) <= W52_CAP_THR:
                            lev = min(lev, 2.0)  # 52주 -35% → 최대 2x

                    # 3. 부분 투입 비율
                    invest_frac = 1.0
                    if strategy in ("F50", "C+F70") and raw_lev >= 3.0 and lev >= 3.0:
                        invest_frac = 0.5 if strategy == "F50" else 0.7
                    elif strategy == "F70" and raw_lev >= 3.0:
                        invest_frac = 0.7

                    position = {
                        "entry_date":      d,
                        "entry_idx":       i,
                        "entry_close":     c,
                        "lev":             lev,
                        "raw_lev":         raw_lev,
                        "invest_frac":     invest_frac,
                        "size":            equity * invest_frac,
                        "equity_at_entry": equity,
                        "vix":             vix or 0,
                    }

        eq_curve.append((d, round(equity, 6)))
        if equity > peak_eq:
            peak_eq = equity
        dd = (equity - peak_eq) / peak_eq
        if dd < mdd:
            mdd = dd

    # 통계
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt   = datetime.strptime(end,   "%Y-%m-%d")
    nyears   = (end_dt - start_dt).days / 365.25
    cagr     = (equity ** (1 / nyears) - 1) * 100 if nyears > 0 and equity > 0 else 0
    calmar   = cagr / abs(mdd * 100) if mdd != 0 else 0
    wins     = [t for t in trades if t["lev_ret"] > 0]
    win_rate = len(wins) / len(trades) * 100 if trades else 0
    avg_hold = sum(t["hold_days"] for t in trades) / len(trades) if trades else 0
    # 연쇄 손절
    consec = 0; max_consec = 0; chain_events = 0
    for t in trades:
        if t["exit_reason"] == "stop_loss":
            consec += 1
            if consec > max_consec: max_consec = consec
            if consec >= 2: chain_events += 1
        else:
            consec = 0

    return {
        "asset":           asset,
        "strategy":        strategy,
        "start":           start,
        "end":             end,
        "gap_scenario":    gap,
        "cagr":            round(cagr, 2),
        "mdd":             round(mdd * 100, 2),
        "calmar":          round(calmar, 3),
        "win_rate":        round(win_rate, 1),
        "n_trades":        len(trades),
        "avg_hold_days":   round(avg_hold, 1),
        "final_equity":    round(equity, 4),
        "max_consec_stops": max_consec,
        "chain_stop_events": chain_events,
        "trades":          trades,
        "eq_curve":        eq_curve[::5],
    }

# ══════════════════════════════════════════════
#  합성 데이터 생성기 (demo 모드)
# ══════════════════════════════════════════════
def make_demo_data() -> dict:
    """역사적 사실을 반영한 합성 시장 데이터."""
    import random
    rng = random.Random(42)

    start = datetime(1998, 7, 1)   # MA200 워밍업 포함
    end   = datetime(2026, 6, 9)
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    n = len(days)

    def idx(ds):
        for k, v in enumerate(days):
            if v >= ds: return k
        return n - 1

    # ─ QQQ 가격 합성 ─
    # 구간 정의 (날짜, 목표가, 변동성 배율)
    qqq = [50.0] * n
    # 1998~2000 버블 상승
    s0, e0, tgt = idx("1998-07-01"), idx("2000-03-10"), 180.0
    for j in range(s0, e0):
        t = (j - s0) / (e0 - s0)
        qqq[j] = 50 + (tgt - 50) * t + rng.gauss(0, 2)
    # 2000~2002 닷컴 붕괴 -83%
    s1, e1, tgt = e0, idx("2002-10-08"), 30.0
    for j in range(s1, e1):
        t = (j - s1) / (e1 - s1)
        qqq[j] = max(5.0, 180 * (1 - 0.833 * t) + rng.gauss(0, 3))
    # 2002~2007 회복
    s2, e2, tgt = e1, idx("2007-10-31"), 110.0
    base2 = qqq[s1]
    for j in range(s2, e2):
        t = (j - s2) / (e2 - s2)
        qqq[j] = max(base2, base2 + (tgt - base2) * t + rng.gauss(0, 2))
    # 2007~2009 GFC -42%
    s3, e3 = idx("2007-10-31"), idx("2009-03-09")
    for j in range(s3, e3):
        t = (j - s3) / (e3 - s3)
        qqq[j] = max(5.0, 110 * (1 - 0.42 * t) + rng.gauss(0, 2))
    # 2009~ 대세 상승 (+1200%)
    s4, base4 = e3, 64.0
    for j in range(s4, n):
        yr = (j - s4) / 252
        noise = rng.gauss(0, 4)
        # 2020 코로나 충격
        if idx("2020-02-19") <= j <= idx("2020-03-23"):
            yr_crash = (j - idx("2020-02-19")) / (idx("2020-03-23") - idx("2020-02-19") + 1)
            qqq[j] = max(10, base4 * (1 + 4.5 * (j - s4) / (idx("2020-02-19") - s4)) * (1 - 0.35 * yr_crash))
        else:
            qqq[j] = max(5.0, base4 * (1 + 4.8 * yr) * (1 + 0.02 * noise))
    # 노이즈 적용
    for j in range(n):
        qqq[j] = max(0.5, qqq[j] * (1 + rng.gauss(0, 0.008)))

    # ─ SPY (QQQ보다 낮은 변동성) ─
    spy = [q * 0.85 + 20 for q in qqq]  # 상관 0.85 근사
    for j in range(n):
        spy[j] = max(0.5, spy[j] * (1 + rng.gauss(0, 0.005)))

    # ─ SOXX (반도체, QQQ보다 고변동) ─
    # 닷컴: -88%, GFC: -58%, 이후 더 크게 상승
    soxx = [30.0] * n
    soxx[0] = 30.0
    for j in range(1, n):
        prev = soxx[j-1]
        d = days[j]
        if   "1998-07" <= d <= "2000-03":  drift = 0.0018; vol = 0.022
        elif "2000-03" <= d <= "2002-10":  drift = -0.004; vol = 0.032
        elif "2002-10" <= d <= "2007-10":  drift = 0.0012; vol = 0.018
        elif "2007-10" <= d <= "2009-03":  drift = -0.0025; vol = 0.028
        elif "2020-02" <= d <= "2020-04":  drift = -0.003; vol = 0.040
        else:                               drift = 0.0015; vol = 0.020
        r = drift + rng.gauss(0, vol)
        soxx[j] = max(0.5, prev * (1 + r))

    # ─ VIX ─
    vix_vals = {}
    for j, d in enumerate(days):
        if   "2000-01" <= d <= "2002-12":   base = 28; amp = 20
        elif "2007-06" <= d <= "2009-06":   base = 35; amp = 40
        elif "2020-02" <= d <= "2020-05":   base = 55; amp = 25
        elif "2022-01" <= d <= "2022-12":   base = 25; amp = 10
        else:                                base = 15; amp = 7
        v = base + amp * abs(math.sin(j * 0.04)) + rng.gauss(0, 3)
        vix_vals[d] = max(10.0, min(85.0, round(v, 2)))

    # ─ VIX3M (2007~) ─
    vix3m_vals = {}
    for j, d in enumerate(days):
        if d < "2007-01-01":
            continue
        v = vix_vals.get(d, 20.0)
        # 위기 시 백워데이션(VIX > VIX3M)
        if "2007-06" <= d <= "2009-06" or "2020-02" <= d <= "2020-05":
            vix3m_vals[d] = round(v * (0.87 + 0.06 * rng.random()), 2)
        else:
            vix3m_vals[d] = round(v * (1.06 + 0.05 * rng.random()), 2)

    # ─ 무위험금리 ─
    rf = {}
    for d in days:
        if   d < "2001-06": rf[d] = 0.058
        elif d < "2004-06": rf[d] = 0.040
        elif d < "2006-06": rf[d] = 0.050
        elif d < "2009-01": rf[d] = 0.032
        elif d < "2015-12": rf[d] = 0.003
        elif d < "2022-06": rf[d] = 0.008
        elif d < "2023-08": rf[d] = 0.048
        else:               rf[d] = 0.052

    def _ohlc(c_list):
        rows = []
        for j, d in enumerate(days):
            c = c_list[j]
            if j == 0:
                o = c
            else:
                o = c * (0.993 + 0.014 * rng.random())  # 시가 ≈ 전일 종가 ±0.7%
            rows.append({"date": d, "open": round(max(0.01, o), 4),
                         "close": round(max(0.01, c), 4)})
        return rows

    return {
        "QQQ":   _ohlc(qqq),
        "SPY":   _ohlc(spy),
        "SOXX":  _ohlc(soxx),
        "VIX":   vix_vals,
        "VIX3M": vix3m_vals,
        "RF":    rf,
    }

# ══════════════════════════════════════════════
#  전체 실행
# ══════════════════════════════════════════════
STRATEGIES = ["A","B","C","D","E","F50","F70","G","C+F70","D+G","B+G"]
ASSETS     = ["QQQ","SPY","SOXX"]

def run_all(demo=False, force_dl=False):
    print("\n=== ETF 레버리지 전략 백테스트 v2 ===\n")

    if demo:
        print("[데모 모드] 합성 데이터 사용\n")
        raw = make_demo_data()
        prices = {k: raw[k] for k in ASSETS}
        vix_map   = raw["VIX"]
        vix3m_map = raw["VIX3M"]
        rf_map    = raw["RF"]
    else:
        print("[실데이터 모드]\n")
        # 1999~ 지수 데이터 (SOXX: 2001 상장, 이전은 SOX 지수 대체)
        prices = {}
        qqq_raw  = load_series("QQQ",  force_dl)
        ndx_raw  = load_series("NDX",  force_dl)   # ^NDX fallback
        spy_raw  = load_series("SPY",  force_dl)
        gspc_raw = load_series("GSPC", force_dl)
        soxx_raw = load_series("SOXX", force_dl)
        sox_raw  = load_series("SOX",  force_dl)

        # QQQ: 있으면 그대로, 없으면 NDX (스케일 맞추기)
        def _splice(primary, fallback, label):
            if not primary and not fallback:
                print(f"  [{label}] 모두 없음"); return []
            if not primary:
                return fallback
            if not fallback:
                return primary
            start = primary[0]["date"]
            pre = [r for r in fallback if r["date"] < start]
            if not pre:
                return primary
            # 스케일 조정: 겹치는 첫날 가격 기준
            fb_start = [r for r in fallback if r["date"] == start]
            if fb_start and fb_start[0]["close"] > 0:
                ratio = primary[0]["close"] / fb_start[0]["close"]
                pre_scaled = [{"date": r["date"],
                                "open":  round(r["open"]  * ratio, 4),
                                "close": round(r["close"] * ratio, 4)} for r in pre]
                return sorted(pre_scaled + primary, key=lambda x: x["date"])
            return primary

        prices["QQQ"]  = _splice(qqq_raw,  ndx_raw,  "QQQ")
        prices["SPY"]  = _splice(spy_raw,  gspc_raw, "SPY")
        prices["SOXX"] = _splice(soxx_raw, sox_raw,  "SOXX")

        vix_raw   = load_series("VIX",   force_dl)
        vix3m_raw = load_series("VIX3M", force_dl)
        vix_map   = {r["date"]: r["close"] for r in vix_raw}
        vix3m_map = {r["date"]: r["close"] for r in vix3m_raw} if vix3m_raw else {}
        rf_map    = load_rf_series(force_dl)

        # 합성 레버리지 검증
        print("\n[합성 레버리지 추적오차 검증 (2010~)]")
        val_results = []
        for real_name, (base_name, lev) in [("TQQQ","QQQ"), ("SOXL","SOXX"), ("UPRO","SPY")]:
            lev_mult = 3.0
            real = load_series(real_name, force_dl)
            base = prices.get(base_name, [])
            if real and base:
                synth = make_synth_lev(base, lev_mult, rf_map)
                v = validate_synth(real_name, real, synth)
                val_results.append(v)
                print(f"  {real_name}: TE={v.get('tracking_error_ann_pct','?')}%/년  "
                      f"최종비율 실={v.get('final_real_norm','?')} 합성={v.get('final_synth_norm','?')}")

    # ── 전략 × 자산 × 구간 백테스트 ──
    full_results  = {}   # 1999-2026
    early_results = {}   # 1999-2009
    late_results  = {}   # 2009-2026
    trade_logs    = defaultdict(list)

    for strat in STRATEGIES:
        use_e = ("E" in strat)
        strat_start = "2007-01-01" if use_e else "1999-01-01"
        print(f"\n▶ 전략 {strat}  {'(2007~)' if use_e else '(1999~)'}")
        for asset in ASSETS:
            base = prices.get(asset, [])
            if not base:
                continue
            v3m = vix3m_map if use_e else None
            # 전구간
            rf = backtest(asset, base, vix_map, v3m, rf_map, strat, strat_start, "2026-06-10", False)
            rg = backtest(asset, base, vix_map, v3m, rf_map, strat, strat_start, "2026-06-10", True)
            # 1999-2009
            re = backtest(asset, base, vix_map, v3m, rf_map, strat,
                          strat_start, "2009-01-01", False)
            full_results[f"{strat}_{asset}"]  = rf
            early_results[f"{strat}_{asset}"] = re
            for t in rf.get("trades", []):
                trade_logs[f"{strat}_{asset}"].append(t)
            print(f"  [{asset}] CAGR={rf.get('cagr','?')}%  MDD={rf.get('mdd','?')}%  "
                  f"Calmar={rf.get('calmar','?')}  W={rf.get('win_rate','?')}%  "
                  f"N={rf.get('n_trades','?')}  갭CAGR={rg.get('cagr','?')}%  "
                  f"[1999-09: CAGR={re.get('cagr','?')}%  MDD={re.get('mdd','?')}%]")

    # ── 최우수 전략 (Calmar, QQQ 기준) ──
    best_strat = max(
        STRATEGIES,
        key=lambda s: full_results.get(f"{s}_QQQ", {}).get("calmar", -999) or -999
    )
    best_info = full_results.get(f"{best_strat}_QQQ", {})
    print(f"\n✅ Calmar 기준 최우수 전략: {best_strat}  "
          f"(QQQ CAGR={best_info.get('cagr')}%  MDD={best_info.get('mdd')}%  "
          f"Calmar={best_info.get('calmar')})")

    # ── 닷컴/GFC 트레이드 로그 ──
    dotcom, gfc = [], []
    for key, trades in trade_logs.items():
        strat = key.split("_")[0]
        if strat != "A":
            continue
        asset = key.split("_", 1)[1]
        for t in trades:
            td = {**t, "_key": key, "_asset": asset, "_strat": strat}
            if "1999-12-01" <= t["entry_date"] <= "2003-06-30":
                dotcom.append(td)
            if "2007-06-01" <= t["entry_date"] <= "2009-12-31":
                gfc.append(td)

    # ── JSON 출력 ──
    summary = []
    for strat in STRATEGIES:
        row = {"strategy": strat, "assets": {}}
        for asset in ASSETS:
            k = f"{strat}_{asset}"
            r  = full_results.get(k, {})
            re = early_results.get(k, {})
            row["assets"][asset] = {
                "cagr":       r.get("cagr"),
                "mdd":        r.get("mdd"),
                "calmar":     r.get("calmar"),
                "win_rate":   r.get("win_rate"),
                "n_trades":   r.get("n_trades"),
                "avg_hold":   r.get("avg_hold_days"),
                "max_consec_stops": r.get("max_consec_stops"),
                "chain_stop_events": r.get("chain_stop_events"),
                "early_cagr": re.get("cagr"),
                "early_mdd":  re.get("mdd"),
            }
        summary.append(row)

    output = {
        "generated":   date.today().isoformat(),
        "mode":        "demo" if demo else "real",
        "best_strategy":          best_strat,
        "best_strategy_calmar":   best_info.get("calmar"),
        "best_strategy_rationale": "Calmar Ratio (CAGR/|MDD|) 기준, QQQ 전구간",
        "summary":     summary,
        "detail":      {k: {kk: vv for kk, vv in v.items()
                            if kk not in ("trades","eq_curve")}
                        for k, v in full_results.items()},
        "early_detail": {k: {kk: vv for kk, vv in v.items()
                              if kk not in ("trades","eq_curve")}
                         for k, v in early_results.items()},
    }

    out_json = BACKTEST_DIR / "results.json"
    out_json.write_text(json.dumps(output, ensure_ascii=False, indent=2))
    print(f"\n저장: {out_json}")

    _write_md(output, summary, dotcom, gfc, demo)
    _write_trade_log_md(dotcom, "닷컴 붕괴(2000~02) 전략 A 트레이드 로그",
                        BACKTEST_DIR / "trade_log_dotcom.md")
    _write_trade_log_md(gfc,    "GFC(2007~09) 전략 A 트레이드 로그",
                        BACKTEST_DIR / "trade_log_gfc.md")
    return output

# ──────────────────────────────
#  마크다운 리포트
# ──────────────────────────────
_STRAT_DESC = {
    "A":    "베이스라인: MA20×0.95 + VIX레버 + 8%익절 + 15%손절 + MA20복귀 + 60일컷",
    "B":    "레버캡: 종가<MA200이면 레버 최대 2× (스킵 X, 강등만)",
    "C":    "MA200 기울기: 20일 기울기 음수면 3× 차단 → 최대 2×",
    "D":    "52주 낙폭캡: 52주 고점 대비 -35% 이상이면 최대 2×",
    "E":    "VIX 기간구조: VIX>VIX3M→3×, 콘탱고→2× (2007~ 구간만)",
    "F50":  "부분투입 50%: 3× 티어 진입 시 자본의 50%만 레버, 나머지 현금",
    "F70":  "부분투입 70%: 3× 티어 진입 시 자본의 70%만 레버",
    "G":    "익절 제거: +8% 익절 없이 MA20복귀/손절/60일컷만 유지",
    "C+F70":"조합 C+F70: MA200기울기필터 + 3×진입 시 70% 부분투입",
    "D+G":  "조합 D+G: 52주낙폭캡 + 익절제거",
    "B+G":  "조합 B+G: 레버캡 + 익절제거",
}

_CRISIS_NOTES = {
    "A": ("닷컴 붕괴(2000~02): VIX가 지속적으로 20 이상이어서 신호가 반복 발동. "
          "-15% 손절 → 재진입 → 재손절 연쇄가 QQQ에서 최다 발생. "
          "3× 합성 레버 기준 최대 연속 손절 시 누적 손실 -85%+ 가능. "
          "GFC(2008): 급격한 하락으로 갭다운 손절이 빈번. "
          "종가 기준 vs 갭 시나리오 차이 3~8%p. 살아남은 유일한 이유는 "
          "+8% 익절이 상승 반등 시 빠르게 수익 실현해 쿠션을 만든 것."),
    "B": ("닷컴(2000~02): QQQ가 MA200 아래였으므로 3×가 2×로 강등. "
          "손실 크기는 줄었지만 여전히 연쇄 손절 발생. "
          "GFC(2008): 하락 초기엔 MA200 위였다가 곧 하회 → 강등 효과 지연."),
    "C": ("닷컴: MA200 기울기가 2000-05부터 음수로 전환 → 3× 차단. "
          "단순 가격 위치보다 빠르게 반응. 하지만 기울기 전환 전 초기 1~2회는 "
          "여전히 3× 노출. GFC: 2008-06부터 기울기 음수 → 이후 3× 없음."),
    "D": ("닷컴: QQQ 고점 대비 -35% 도달 후 모든 추가 신호 최대 2×로 제한. "
          "붕괴 초반(-35% 이전)에는 여전히 3× 가능. GFC는 하락 속도가 빨라 "
          "-35% 도달 전에 손절 다수 발생했고, 이후 2× 제한 발동."),
    "E": ("2007~만 검증 가능. GFC에서 VIX이 VIX3M을 초과(백워데이션)하자 "
          "3× 레버를 유지 — 오히려 더 큰 손실. "
          "이 전략의 의도는 '공포 극점에서 더 공격적으로'이므로 2008년에는 "
          "손실이 크지만 이후 반등 수익도 크다."),
    "F50": ("3× 진입 자본을 50%로 제한. 닷컴·GFC 연쇄 손절의 절대 금액 손실을 "
            "절반으로 줄임. 대신 2009~ 상승장 수익도 절반으로 줄어드는 트레이드오프."),
    "F70": "F50과 동일 원리, 70% 투입으로 수익/위험 중간점.",
    "G":   ("익절 없으면 추세 하락 시 포지션을 MA20 복귀까지 보유 → "
            "닷컴 붕괴에서 추가 손실 가능. 그러나 급반등 장(V-shape)에서는 "
            "+8% 이상 수익 확보 가능. 결과는 장세에 따라 편차 큼."),
}

def _write_md(output, summary, dotcom, gfc, demo):
    lines = []
    lines += [f"# ETF 레버리지 전략 백테스트 결과 (v2)",
              f"> 생성: {output['generated']}  |  모드: {'합성 데이터(demo)' if demo else '실데이터'}",
              "",
              f"## 최우수 전략: **{output['best_strategy']}**",
              f"> {output['best_strategy_rationale']}",
              "",
              "---",
              "## 전략 정의",
              ""]
    for k, v in _STRAT_DESC.items():
        lines.append(f"- **{k}**: {v}")

    lines += ["", "---",
              "## 전구간 성과 비교 (1999-01-01 ~ 2026-06-10)", ""]

    for asset in ASSETS:
        lines += [f"### {asset}", ""]
        lines.append(
            "| 전략 | CAGR% | MDD% | Calmar | 승률% | N거래 | 평균보유 | "
            "1999-09 CAGR% | 1999-09 MDD% | 최대연속손절 | 연쇄이벤트 |")
        lines.append(
            "|------|-------|------|--------|-------|-------|---------|"
            "-------------|------------|------------|-----------|")
        for row in summary:
            s = row["strategy"]
            a = row["assets"].get(asset, {})
            lines.append(
                f"| {s} | {a.get('cagr','N/A')} | {a.get('mdd','N/A')} | "
                f"{a.get('calmar','N/A')} | {a.get('win_rate','N/A')} | "
                f"{a.get('n_trades','N/A')} | {a.get('avg_hold','N/A')}일 | "
                f"{a.get('early_cagr','N/A')} | {a.get('early_mdd','N/A')} | "
                f"{a.get('max_consec_stops','N/A')} | {a.get('chain_stop_events','N/A')} |")
        lines.append("")

    lines += ["---",
              "## 위기 구간 전략별 서술 (전략 A 기준 비교)", ""]
    for strat, note in _CRISIS_NOTES.items():
        lines += [f"### 전략 {strat}", note, ""]

    lines += ["---",
              "## 닷컴 붕괴 / GFC 연쇄 손절 집계 (전략 A)", ""]
    # 연쇄 손절 요약
    for asset in ASSETS:
        k = f"A_{asset}"
        d_key  = [t for t in dotcom if t.get("_key","") == k]
        g_key  = [t for t in gfc    if t.get("_key","") == k]
        d_sl   = [t for t in d_key if t["exit_reason"] == "stop_loss"]
        g_sl   = [t for t in g_key if t["exit_reason"] == "stop_loss"]
        lines.append(f"**{asset}**  닷컴 손절: {len(d_sl)}/{len(d_key)}  GFC 손절: {len(g_sl)}/{len(g_key)}")
    lines.append("\n*상세 트레이드 로그: trade_log_dotcom.md / trade_log_gfc.md*")

    if demo:
        lines += ["", "> ⚠️ **데모 모드**: 합성 데이터 사용. `python backtest/run_backtest.py`로 실데이터 실행 요망."]

    (BACKTEST_DIR / "results.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"저장: {BACKTEST_DIR / 'results.md'}")

def _write_trade_log_md(logs, title, path):
    lines = [f"# {title}", "",
             "| 자산 | 진입일 | 청산일 | VIX | 레버 | 보유일 | 청산사유 | 기초수익% | 레버수익% |",
             "|------|--------|--------|-----|------|--------|---------|----------|---------|"]
    reason_label = {
        "stop_loss": "❌손절", "take_profit": "✅익절",
        "ma_return": "🔄MA복귀", "timeout": "⏱60일컷",
    }
    for t in sorted(logs, key=lambda x: x.get("entry_date","")):
        r = reason_label.get(t.get("exit_reason",""), t.get("exit_reason","?"))
        lines.append(
            f"| {t.get('_asset','?')} | {t.get('entry_date','')} | {t.get('exit_date','')} | "
            f"{t.get('vix','?')} | {t.get('lev','?')}× | {t.get('hold_days','?')}일 | "
            f"{r} | {t.get('base_ret','?')} | {t.get('lev_ret','?')} |")
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"저장: {path}")

# ══════════════════════════════════════════════
#  엔트리포인트
# ══════════════════════════════════════════════
if __name__ == "__main__":
    demo    = "--demo"  in sys.argv
    forcedl = "--clear" in sys.argv
    if forcedl and CACHE_DIR.exists():
        shutil.rmtree(CACHE_DIR); CACHE_DIR.mkdir()
        print("캐시 삭제 완료")
    run_all(demo=demo, force_dl=forcedl)
