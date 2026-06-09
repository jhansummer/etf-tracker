#!/usr/bin/env python3
"""
ETF 레버리지 매수 전략 백테스트
1999~2026, 전략 A~E 비교

사용법:
  python backtest/run_backtest.py            # 데이터 다운로드 후 전체 백테스트
  python backtest/run_backtest.py --demo     # 합성 데이터로 구조 검증

출력:
  backtest/results.json
  backtest/results.md
  backtest/trade_log_dotcom.md
  backtest/trade_log_gfc.md
"""

import sys, json, csv, io, math, urllib.request, urllib.parse
from pathlib import Path
from datetime import date, timedelta, datetime
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent
BACKTEST_DIR = ROOT / "backtest"
CACHE_DIR = BACKTEST_DIR / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ══════════════════════════════════════════════
#  파라미터 (전략 상수)
# ══════════════════════════════════════════════
MA_SIGNAL   = 20          # MA20 창
SIGNAL_THR  = 0.95        # 종가 < MA20 × 0.95 = 진입 신호
MA_EXIT     = 20          # MA20 복귀 = 청산
TP_RETURN   = 0.08        # +8% 익절
SL_RETURN   = -0.15       # -15% 손절 (레버 전 기초자산 기준)
MAX_HOLD    = 60          # 최대 보유일
EXPENSE     = 0.0095      # 레버리지 ETF 연간 운용보수 (0.95%)
MA200_WIN   = 200         # MA200 창

# VIX 레버 결정
def vix_to_lev(vix):
    if vix < 20:   return None        # 스킵
    if vix < 30:   return 2.0
    if vix < 40:   return 2.5
    return 3.0

def vix_to_lev_term(vix, vix3m):
    """전략 D: VIX > VIX3M(백워데이션)이면 3×, 아니면 2×. VIX<20은 스킵."""
    if vix < 20:   return None
    if vix3m is None or vix > vix3m:
        return 3.0   # 백워데이션 = 공포 상승 구간
    return 2.0

# ══════════════════════════════════════════════
#  데이터 다운로드 (stooq → yfinance fallback)
# ══════════════════════════════════════════════
HDRS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "text/csv,*/*",
    "Referer": "https://stooq.com/",
}

def _fetch_stooq(stooq_sym: str, start="19990101", end="20260610") -> list[dict]:
    url = f"https://stooq.com/q/d/l/?s={stooq_sym}&d1={start}&d2={end}&i=d"
    req = urllib.request.Request(url, headers=HDRS)
    with urllib.request.urlopen(req, timeout=40) as r:
        text = r.read().decode("utf-8", errors="replace")
    if "No data" in text or len(text) < 50:
        raise ValueError(f"stooq 데이터 없음 ({stooq_sym})")
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

def _fetch_yfinance(ticker: str, start="1999-01-01", end="2026-06-10") -> list[dict]:
    """yfinance 패키지 fallback"""
    import yfinance as yf
    df = yf.download(ticker, start=start, end=end, auto_adjust=True, progress=False)
    if df.empty:
        raise ValueError(f"yfinance 빈 결과 ({ticker})")
    if hasattr(df.columns, 'get_level_values'):
        df.columns = df.columns.get_level_values(0)
    rows = []
    for idx, row in df.iterrows():
        c = float(row.get("Close", 0))
        o = float(row.get("Open", c))
        if not math.isnan(c) and c > 0:
            rows.append({"date": idx.strftime("%Y-%m-%d"), "open": round(o, 4), "close": round(c, 4)})
    return sorted(rows, key=lambda x: x["date"])

STOOQ_MAP = {
    "QQQ":   "qqq.us",
    "SPY":   "spy.us",
    "SOXX":  "soxx.us",
    "SOX":   "%5esox",       # ^SOX Philadelphia Semiconductor Index
    "VIX":   "vix.us",
    "VIX3M": "%5evix3m",     # ^VIX3M
    "TQQQ":  "tqqq.us",
    "SOXL":  "soxl.us",
    "UPRO":  "upro.us",
    "IRX":   "%5eirx",       # ^IRX 3개월 T-bill
}

def load_price_series(name: str, force_dl=False) -> list[dict]:
    """캐시 우선 로드. 없으면 stooq → yfinance 다운로드."""
    cache_path = CACHE_DIR / f"{name}.json"
    if cache_path.exists() and not force_dl:
        return json.loads(cache_path.read_text())
    print(f"  [{name}] 다운로드 중...")
    prices = None
    stooq_sym = STOOQ_MAP.get(name)
    if stooq_sym:
        try:
            prices = _fetch_stooq(stooq_sym)
            print(f"  [{name}] stooq: {len(prices)}일")
        except Exception as e:
            print(f"  [{name}] stooq 실패: {e}")
    if not prices:
        try:
            yf_sym = "^" + name if name in ("VIX","VIX3M","SOX","IRX") else name
            prices = _fetch_yfinance(yf_sym)
            print(f"  [{name}] yfinance: {len(prices)}일")
        except Exception as e:
            print(f"  [{name}] yfinance 실패: {e}")
            return []
    cache_path.write_text(json.dumps(prices, ensure_ascii=False))
    return prices

def load_fred_tbill() -> dict[str, float]:
    """FRED DTB3 (3개월 T-bill 일간, %)→ {date: annualized_rate}
    pandas_datareader 사용, 없으면 ^IRX 에서 대체.
    """
    cache_path = CACHE_DIR / "DTB3.json"
    if cache_path.exists():
        return json.loads(cache_path.read_text())
    print("  [DTB3] FRED 다운로드 중...")
    try:
        import pandas_datareader.data as web
        import pandas as pd
        df = web.DataReader("DTB3", "fred", start="1999-01-01", end="2026-06-10")
        df = df.dropna()
        result = {}
        for idx, row in df.iterrows():
            result[idx.strftime("%Y-%m-%d")] = float(row["DTB3"]) / 100.0
        cache_path.write_text(json.dumps(result))
        print(f"  [DTB3] FRED: {len(result)}일")
        return result
    except Exception as e:
        print(f"  [DTB3] FRED 실패: {e}, ^IRX 대체 사용")
        irx = load_price_series("IRX")
        result = {r["date"]: r["close"] / 100.0 for r in irx if r["close"] > 0}
        cache_path.write_text(json.dumps(result))
        return result

# ══════════════════════════════════════════════
#  합성 레버리지 데이터 생성기
#  r_L = L×r − (L−1)×rf_daily − expense/252
# ══════════════════════════════════════════════
def make_synth_leveraged(prices: list[dict], lev: float,
                          rf_series: dict[str, float],
                          expense: float = EXPENSE) -> list[dict]:
    """기초지수 일간수익률로 합성 레버리지 가격 시계열 생성."""
    result = []
    synth_price = 100.0   # 기준 100으로 정규화
    for i in range(len(prices)):
        p = prices[i]
        if i == 0:
            result.append({"date": p["date"], "close": round(synth_price, 4),
                           "open": round(synth_price, 4)})
            continue
        prev = prices[i - 1]
        r_base = (p["close"] - prev["close"]) / prev["close"]
        r_open = (p["open"]  - prev["close"]) / prev["close"]  # 갭용
        rf = rf_series.get(p["date"], rf_series.get(prev["date"], 0.045)) / 252
        r_lev = lev * r_base - (lev - 1) * rf - expense / 252
        r_lev_open = lev * r_open - (lev - 1) * rf - expense / 252

        synth_price *= (1 + r_lev)
        synth_open   = result[-1]["close"] * (1 + r_lev_open)
        result.append({"date": p["date"],
                        "close": round(max(synth_price, 0.01), 4),
                        "open":  round(max(synth_open, 0.01), 4)})
    return result

def validate_synth_vs_real(name_synth: str, real_prices: list[dict],
                            synth_prices: list[dict],
                            start_date: str = "2010-01-01") -> dict:
    """실제 레버리지 ETF vs 합성치 추적오차 검증 (2010~)."""
    real_map  = {r["date"]: r["close"] for r in real_prices}
    synth_map = {r["date"]: r["close"] for r in synth_prices}
    common = sorted(k for k in real_map if k >= start_date and k in synth_map)
    if len(common) < 30:
        return {"status": "데이터 부족"}
    # 두 시계열을 첫 공통 날에 정규화
    d0 = common[0]
    real0, synth0 = real_map[d0], synth_map[d0]
    corr_data = []
    for d in common:
        r_norm = real_map[d] / real0
        s_norm = synth_map[d] / synth0
        corr_data.append((r_norm, s_norm))
    # 추적오차 = 연환산 표준편차(log 차이)
    log_diffs = []
    for i in range(1, len(common)):
        rd  = math.log(real_map[common[i]]  / real_map[common[i-1]])
        sd  = math.log(synth_map[common[i]] / synth_map[common[i-1]])
        log_diffs.append(rd - sd)
    te = math.sqrt(sum(x**2 for x in log_diffs) / len(log_diffs)) * math.sqrt(252)
    final_r = corr_data[-1][0]
    final_s = corr_data[-1][1]
    return {
        "name": name_synth,
        "n_days": len(common),
        "from": common[0],
        "to":   common[-1],
        "tracking_error_ann": round(te * 100, 3),
        "final_real_normalized":  round(final_r, 4),
        "final_synth_normalized": round(final_s, 4),
        "final_ratio": round(final_s / final_r, 4),
    }

# ══════════════════════════════════════════════
#  MA 시리즈 계산 (pandas 없이)
# ══════════════════════════════════════════════
def compute_ma(closes: list[float], window: int) -> list[float | None]:
    ma = []
    for i in range(len(closes)):
        if i < window - 1:
            ma.append(None)
        else:
            ma.append(sum(closes[i - window + 1: i + 1]) / window)
    return ma

# ══════════════════════════════════════════════
#  백테스트 엔진 (단일 자산, 단일 전략)
# ══════════════════════════════════════════════
def backtest_asset(
    name: str,
    base_prices: list[dict],    # 기초자산 종가+시가
    vix_series: dict[str, float],
    vix3m_series: dict[str, float] | None,
    rf_series: dict[str, float],
    strategy: str = "A",        # A|B|C|D|E
    start_date: str = "1999-01-01",
    end_date: str   = "2026-06-10",
    gap_scenario: bool = False,  # True=다음날 시가 체결
) -> dict:
    """
    strategy 플래그:
      A = baseline
      B = MA200 전체 필터
      C = MA200은 3× 티어만 필터
      D = VIX term structure (VIX > VIX3M → 3×, else 2×)
      E = 익절(+8%) 제거
    """
    prices = [p for p in base_prices if start_date <= p["date"] <= end_date]
    if len(prices) < MA200_WIN + 10:
        return {"error": "데이터 부족", "name": name}

    closes = [p["close"] for p in prices]
    opens  = [p["open"]  for p in prices]
    dates  = [p["date"]  for p in prices]
    ma20   = compute_ma(closes, MA_SIGNAL)
    ma200  = compute_ma(closes, MA200_WIN)

    # 포트폴리오 상태
    equity  = 1.0      # 정규화된 자본
    position = None    # None or dict
    trades   = []
    equity_curve = [(dates[0], equity)]
    max_eq = equity
    mdd    = 0.0
    consecutive_stops = 0  # 연속 손절 추적

    for i in range(MA200_WIN, len(prices)):
        d     = dates[i]
        close = closes[i]
        open_ = opens[i]
        vix   = vix_series.get(d)
        vix3m = vix3m_series.get(d) if vix3m_series else None

        if vix is None:
            equity_curve.append((d, equity))
            continue

        # ── 포지션 청산 판단 ──
        if position is not None:
            entry_close = position["entry_close"]
            lev         = position["lev"]
            entry_date  = position["entry_date"]
            entry_idx   = position["entry_idx"]
            hold_days   = i - entry_idx

            # 레버 가격 시뮬: 진입 시점부터 합성
            synth_ret = 1.0
            for j in range(entry_idx + 1, i + 1):
                r_base = (closes[j] - closes[j-1]) / closes[j-1]
                rf = rf_series.get(dates[j], 0.045) / 252
                r_lev = lev * r_base - (lev - 1) * rf - EXPENSE / 252
                synth_ret *= (1 + r_lev)

            # 갭 시나리오: 손절을 다음날 시가로 체결
            if gap_scenario:
                # 다음날 시가 기준 손익 계산
                r_gap = (open_ - closes[i-1]) / closes[i-1]
                rf_gap = rf_series.get(d, 0.045) / 252
                r_gap_lev = lev * r_gap - (lev - 1) * rf_gap - EXPENSE / 252
                gap_synth = synth_ret / (1 + (lev*(closes[i]-closes[i-1])/closes[i-1]
                                              - (lev-1)*rf_gap - EXPENSE/252)) * (1 + r_gap_lev)
            else:
                gap_synth = synth_ret

            # 기초자산 기준 손익 (손절 판단용)
            base_ret = close / entry_close - 1

            exit_reason = None
            exit_ret = synth_ret - 1

            # 1) 손절: -15% (레버 기준 → 기초자산으로 환산)
            # SL_RETURN = -15%는 기초자산 기준
            if gap_scenario:
                base_gap_ret = (open_ - closes[i-1]) / closes[i-1]
                base_cum = (close / entry_close - 1)
                # 손절 체크: 누적 기초 손익 ≤ SL_RETURN
                if base_cum <= SL_RETURN:
                    exit_reason = "stop_loss"
                    exit_ret = gap_synth - 1
            else:
                if (close / entry_close - 1) <= SL_RETURN:
                    exit_reason = "stop_loss"

            # 2) 익절: +8% (레버리지 기준)
            if exit_reason is None and strategy != "E":
                if synth_ret - 1 >= TP_RETURN:
                    exit_reason = "take_profit"

            # 3) MA20 복귀
            if exit_reason is None and ma20[i] is not None:
                if close >= ma20[i]:
                    exit_reason = "ma_return"

            # 4) 60일 컷
            if exit_reason is None and hold_days >= MAX_HOLD:
                exit_reason = "timeout"

            if exit_reason:
                pnl = exit_ret * position["size"]
                equity += pnl
                if exit_reason == "stop_loss":
                    consecutive_stops += 1
                else:
                    consecutive_stops = 0

                trades.append({
                    "asset":       name,
                    "entry_date":  entry_date,
                    "exit_date":   d,
                    "vix":         round(position["entry_vix"], 2),
                    "lev":         lev,
                    "hold_days":   hold_days,
                    "exit_reason": exit_reason,
                    "base_ret":    round((close / entry_close - 1) * 100, 2),
                    "lev_ret":     round(exit_ret * 100, 2),
                    "consec_stops": consecutive_stops if exit_reason == "stop_loss" else 0,
                })
                position = None

        # ── 진입 신호 판단 (포지션 없을 때) ──
        if position is None and ma20[i] is not None:
            signal = closes[i] < ma20[i] * SIGNAL_THR

            if signal:
                # VIX 레버 결정
                if strategy == "D":
                    lev = vix_to_lev_term(vix, vix3m)
                else:
                    lev = vix_to_lev(vix)

                if lev is None:
                    pass  # VIX < 20 스킵
                else:
                    # MA200 필터
                    skip = False
                    if strategy == "B" and ma200[i] is not None:
                        if closes[i] <= ma200[i]:
                            skip = True  # 종가 < MA200 → 스킵
                    elif strategy == "C" and lev >= 3.0 and ma200[i] is not None:
                        if closes[i] <= ma200[i]:
                            skip = True

                    if not skip:
                        position = {
                            "entry_date":  d,
                            "entry_close": closes[i],
                            "entry_idx":   i,
                            "lev":         lev,
                            "size":        1.0,
                            "entry_vix":   vix,
                        }

        # 자본 곡선 업데이트
        equity_curve.append((d, round(equity, 6)))
        if equity > max_eq:
            max_eq = equity
        dd = (equity - max_eq) / max_eq
        if dd < mdd:
            mdd = dd

    # 통계 계산
    n_years = (datetime.strptime(end_date, "%Y-%m-%d") -
               datetime.strptime(start_date, "%Y-%m-%d")).days / 365.25
    cagr = (equity ** (1 / n_years) - 1) if n_years > 0 and equity > 0 else 0
    calmar = cagr / abs(mdd) if mdd != 0 else 0
    wins = [t for t in trades if t["lev_ret"] > 0]
    win_rate = len(wins) / len(trades) * 100 if trades else 0
    avg_hold = sum(t["hold_days"] for t in trades) / len(trades) if trades else 0

    # 연쇄 손절 집계
    chain_stops = sum(1 for t in trades
                      if t["exit_reason"] == "stop_loss" and t["consec_stops"] >= 2)

    return {
        "asset":       name,
        "strategy":    strategy,
        "start":       start_date,
        "end":         end_date,
        "gap_scenario": gap_scenario,
        "final_equity": round(equity, 4),
        "cagr":         round(cagr * 100, 2),
        "mdd":          round(mdd * 100, 2),
        "calmar":       round(calmar, 3),
        "win_rate":     round(win_rate, 1),
        "n_trades":     len(trades),
        "avg_hold_days": round(avg_hold, 1),
        "chain_stop_events": chain_stops,
        "equity_curve": equity_curve[::5],  # 5일 간격으로 저장 (파일 크기 절약)
        "trades":       trades,
    }

# ══════════════════════════════════════════════
#  합성 데이터 생성기 (네트워크 없을 때 데모용)
# ══════════════════════════════════════════════
def _gbm_simulate(n_days, mu_annual, sigma_annual, start_price=100.0, seed=42):
    """기하 브라운 운동으로 일간 가격 시뮬레이션."""
    import random
    rng = random.Random(seed)
    dt = 1 / 252
    mu_d = mu_annual * dt
    sigma_d = sigma_annual * math.sqrt(dt)
    prices = [start_price]
    for _ in range(n_days - 1):
        z = rng.gauss(0, 1)
        r = mu_d - 0.5 * sigma_d**2 + sigma_d * z
        prices.append(prices[-1] * math.exp(r))
    return prices

def _crash_apply(closes, start_idx, end_idx, total_drop, recovery_idx=None):
    """인덱스 범위에 하락 + 선택적 회복 패턴 적용."""
    n = end_idx - start_idx
    if n <= 0: return closes
    peak = closes[start_idx]
    trough = peak * (1 + total_drop)
    for j in range(n):
        t = j / n
        closes[start_idx + j] = peak * (1 + total_drop * t)
    closes[end_idx - 1] = trough
    return closes

def make_demo_data():
    """
    시장 역사를 반영한 합성 시계열 생성.
    실제 데이터 없이도 전략 특성 검증 가능.

    시나리오:
    - 1999-03 ~ 2000-03: 기술주 버블 상승 (+80%)
    - 2000-03 ~ 2002-10: 닷컴 붕괴 (-83%)
    - 2002-10 ~ 2007-10: 회복 (+130%)
    - 2007-10 ~ 2009-03: GFC (-55%)
    - 2009-03 ~ 2026-06: 대세 상승 (+1000%+)
    """
    start = datetime(1999, 1, 2)
    end   = datetime(2026, 6, 9)
    total_days = (end - start).days

    # 거래일 생성 (주말 제외, 휴일 근사)
    trading_days = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            trading_days.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)

    n = len(trading_days)

    def day_idx(date_str):
        """날짜 문자열 → 인덱스"""
        try:
            return trading_days.index(date_str)
        except ValueError:
            # 가장 가까운 인덱스
            for k, t in enumerate(trading_days):
                if t >= date_str:
                    return k
            return n - 1

    # QQQ 합성 (나스닥100 대리)
    qqq_base = _gbm_simulate(n, mu_annual=0.12, sigma_annual=0.22, start_price=50.0, seed=1)
    # 닷컴 버블 위로 +80% (1999-2000-03)
    up_start = day_idx("1999-01-02");  up_end = day_idx("2000-03-10")
    for j in range(up_start, up_end):
        t = (j - up_start) / (up_end - up_start)
        qqq_base[j] = 50.0 * (1 + 0.80 * t) * (0.9 + 0.2 * j/n)
    # 닷컴 붕괴 -83%
    crash_start = day_idx("2000-03-10"); crash_end = day_idx("2002-10-09")
    peak_val = qqq_base[crash_start - 1]
    for j in range(crash_start, crash_end):
        t = (j - crash_start) / (crash_end - crash_start)
        qqq_base[j] = peak_val * (1 - 0.83 * t)
    # 회복
    rec_start = crash_end; rec_end = day_idx("2007-10-09")
    trough = qqq_base[rec_start]
    rec_target = trough * 2.6
    for j in range(rec_start, rec_end):
        t = (j - rec_start) / (rec_end - rec_start)
        qqq_base[j] = trough * (1 + 1.6 * t)
    # GFC -42%
    gfc_start = day_idx("2007-10-09"); gfc_end = day_idx("2009-03-09")
    pre_gfc = qqq_base[gfc_start - 1]
    for j in range(gfc_start, gfc_end):
        t = (j - gfc_start) / (gfc_end - gfc_start)
        qqq_base[j] = pre_gfc * (1 - 0.42 * t)
    # 2009- 대세 상승
    bull_start = gfc_end; bull_end = n
    bull_base = qqq_base[bull_start - 1]
    import random; rng = random.Random(99)
    for j in range(bull_start, bull_end):
        t = (j - bull_start) / (bull_end - bull_start)
        drift = bull_base * (1 + 5.0 * t) * (0.97 + 0.06 * rng.random())
        qqq_base[j] = drift

    # 10% 일간 노이즈 추가
    rng2 = random.Random(77)
    for j in range(n):
        qqq_base[j] *= (0.98 + 0.04 * rng2.random())
    qqq_base = [max(0.1, x) for x in qqq_base]

    # SPY 합성 (QQQ보다 낮은 변동성)
    spy_base = [q * 0.55 + 40 for q in qqq_base]  # QQQ와 상관 0.85 근사

    # SOXX 합성 (반도체, QQQ보다 고변동)
    soxx_base = _gbm_simulate(n, mu_annual=0.13, sigma_annual=0.28, start_price=30.0, seed=2)
    # SOXX에도 유사 이벤트 적용 (더 크게)
    for j in range(crash_start, crash_end):
        t = (j - crash_start) / (crash_end - crash_start)
        soxx_base[j] = soxx_base[crash_start - 1] * (1 - 0.88 * t)
    for j in range(gfc_start, gfc_end):
        t = (j - gfc_start) / (gfc_end - gfc_start)
        soxx_base[j] = soxx_base[gfc_start - 1] * (1 - 0.55 * t)
    for j in range(bull_start, bull_end):
        t = (j - bull_start) / (bull_end - bull_start)
        soxx_base[j] = soxx_base[bull_start - 1] * (1 + 6.5 * t) * (0.96 + 0.08 * rng2.random())
    soxx_base = [max(0.1, x) for x in soxx_base]

    # VIX 합성
    vix_vals = []
    for j, d in enumerate(trading_days):
        if   "2000" <= d <= "2002-10":  vix = 25 + 20 * abs(math.sin(j * 0.05)) + rng2.gauss(0, 5)
        elif "2008" <= d <= "2009-03":  vix = 35 + 40 * abs(math.sin(j * 0.1))  + rng2.gauss(0, 8)
        elif "2020-02" <= d <= "2020-05": vix = 50 + 30 * abs(math.sin(j * 0.2))
        else:                            vix = 15 + 8  * abs(math.sin(j * 0.03)) + rng2.gauss(0, 2)
        vix_vals.append(max(10, min(90, vix)))

    # VIX3M 합성 (VIX와 근접하되 위기 시 VIX < VIX3M 역전)
    vix3m_vals = []
    for j, v in enumerate(vix_vals):
        d = trading_days[j]
        if "2000" <= d <= "2002-10" or "2008" <= d <= "2009-03":
            # 백워데이션: VIX > VIX3M
            vix3m_vals.append(v * (0.88 + 0.05 * rng2.random()))
        else:
            # 컨탱고: VIX < VIX3M
            vix3m_vals.append(v * (1.05 + 0.05 * rng2.random()))

    # T-bill rate 합성 (2000년대 5%, 2008 이후 저금리, 2022+ 상승)
    rf_vals = {}
    for j, d in enumerate(trading_days):
        if   d < "2001-01": rf = 0.058
        elif d < "2004-01": rf = 0.042
        elif d < "2007-01": rf = 0.048
        elif d < "2009-01": rf = 0.035
        elif d < "2015-01": rf = 0.003
        elif d < "2022-01": rf = 0.008
        elif d < "2023-07": rf = 0.048
        else:                rf = 0.052
        rf_vals[d] = rf + rng2.gauss(0, 0.002)

    def _to_ohlc(closes_list, dates_list):
        rows = []
        for j, d in enumerate(dates_list):
            c = closes_list[j]
            o = c * (0.995 + 0.01 * rng2.random())  # 시가 = 전일 종가 ±0.5%
            rows.append({"date": d, "open": round(max(0.01, o), 4),
                         "close": round(max(0.01, c), 4)})
        return rows

    return {
        "QQQ":  _to_ohlc(qqq_base,  trading_days),
        "SPY":  _to_ohlc(spy_base,  trading_days),
        "SOXX": _to_ohlc(soxx_base, trading_days),
        "VIX":  {d: v for d, v in zip(trading_days, vix_vals)},
        "VIX3M":{d: v for d, v in zip(trading_days, vix3m_vals)},
        "RF":   rf_vals,
    }

# ══════════════════════════════════════════════
#  전체 백테스트 실행
# ══════════════════════════════════════════════
def run_all(demo_mode=False):
    print("\n=== ETF 레버리지 전략 백테스트 ===\n")

    if demo_mode:
        print("[데모 모드] 합성 데이터 사용\n")
        demo = make_demo_data()
        all_prices = {k: demo[k] for k in ("QQQ","SPY","SOXX")}
        vix_series = demo["VIX"]
        vix3m_series = demo["VIX3M"]
        rf_series = demo["RF"]
    else:
        print("[실데이터 모드] stooq/yfinance 다운로드\n")
        all_prices = {}
        for name in ("QQQ", "SPY", "SOXX"):
            all_prices[name] = load_price_series(name)
        # SOXX 2001년 이전: SOX 지수로 대체
        soxx_data = all_prices["SOXX"]
        sox_data  = load_price_series("SOX")
        if soxx_data and sox_data:
            soxx_start = soxx_data[0]["date"] if soxx_data else "2099-01-01"
            pre = [r for r in sox_data if r["date"] < soxx_start]
            # SOX를 SOXX 시작가 기준으로 스케일 조정
            if pre and soxx_data:
                # 가장 가까운 날짜의 SOX vs SOXX 비율로 조정
                overlap_sox  = [r for r in sox_data if r["date"] == soxx_start]
                if overlap_sox:
                    ratio = soxx_data[0]["close"] / overlap_sox[0]["close"]
                    pre_scaled = [{"date": r["date"],
                                   "open":  round(r["open"]  * ratio, 4),
                                   "close": round(r["close"] * ratio, 4)} for r in pre]
                    all_prices["SOXX"] = sorted(pre_scaled + soxx_data,
                                                key=lambda x: x["date"])
                    print(f"  SOXX: SOX 전처리 {len(pre)}일 추가됨")

        # VIX
        vix_list = load_price_series("VIX")
        vix_series = {r["date"]: r["close"] for r in vix_list}

        # VIX3M (가능한 날짜부터)
        vix3m_list = load_price_series("VIX3M")
        vix3m_series = {r["date"]: r["close"] for r in vix3m_list} if vix3m_list else None

        # 무위험금리
        rf_series = load_fred_tbill()
        if not rf_series:
            rf_series = {}  # fallback: 상수 4.5%

    # ── 합성 레버리지 검증 (실데이터 모드에서만) ──
    validation_results = []
    if not demo_mode:
        print("\n[합성 레버리지 추적오차 검증]")
        lev_map = {"TQQQ": ("QQQ", 3.0), "SOXL": ("SOXX", 3.0), "UPRO": ("SPY", 3.0)}
        for real_name, (base_name, lev) in lev_map.items():
            real_data = load_price_series(real_name)
            if real_data and all_prices.get(base_name):
                synth = make_synth_leveraged(all_prices[base_name], lev, rf_series)
                v = validate_synth_vs_real(real_name, real_data, synth)
                validation_results.append(v)
                print(f"  {real_name}: 추적오차 {v.get('tracking_error_ann','?')}%/년, "
                      f"최종비율 {v.get('final_ratio','?')}")

    # ── 전략 비교 백테스트 ──
    strategies = ["A","B","C","D","E"]
    assets = ["QQQ","SPY","SOXX"]

    results_full  = {}   # 1999-2026 전구간
    results_early = {}   # 1999-2009 (닷컴+GFC)
    results_late  = {}   # 2009-2026 (대세 상승)
    trade_logs    = {}

    for strategy in strategies:
        print(f"\n전략 {strategy} 백테스트 중...")
        for asset in assets:
            prices = all_prices.get(asset, [])
            if not prices:
                print(f"  [{asset}] 데이터 없음, 스킵")
                continue
            key = f"{strategy}_{asset}"

            # 전략 D는 VIX3M 필요
            v3m = vix3m_series if strategy == "D" else None

            # 전구간 (종가 기준)
            r_full = backtest_asset(asset, prices, vix_series, v3m, rf_series,
                                    strategy=strategy, start_date="1999-01-01")
            # 전구간 (갭 시나리오)
            r_gap  = backtest_asset(asset, prices, vix_series, v3m, rf_series,
                                    strategy=strategy, start_date="1999-01-01",
                                    gap_scenario=True)
            # 1999-2009
            r_early = backtest_asset(asset, prices, vix_series, v3m, rf_series,
                                     strategy=strategy,
                                     start_date="1999-01-01", end_date="2009-01-01")
            # 2009-2026
            r_late = backtest_asset(asset, prices, vix_series, v3m, rf_series,
                                    strategy=strategy,
                                    start_date="2009-01-01", end_date="2026-06-10")

            results_full[key]  = r_full
            results_early[key] = r_early
            trade_logs[key]    = r_full.get("trades", [])

            print(f"  [{asset}] CAGR={r_full.get('cagr','?')}%  MDD={r_full.get('mdd','?')}%  "
                  f"Calmar={r_full.get('calmar','?')}  W={r_full.get('win_rate','?')}%  "
                  f"N={r_full.get('n_trades','?')}  "
                  f"갭CAGR={r_gap.get('cagr','?')}%")

    # ── 결과 포맷팅 ──
    summary_rows = []
    for strategy in strategies:
        row = {"strategy": strategy}
        for asset in assets:
            key = f"{strategy}_{asset}"
            r = results_full.get(key, {})
            re = results_early.get(key, {})
            row[f"{asset}_cagr"]     = r.get("cagr", "N/A")
            row[f"{asset}_mdd"]      = r.get("mdd", "N/A")
            row[f"{asset}_calmar"]   = r.get("calmar", "N/A")
            row[f"{asset}_wr"]       = r.get("win_rate", "N/A")
            row[f"{asset}_n"]        = r.get("n_trades", "N/A")
            row[f"{asset}_hold"]     = r.get("avg_hold_days", "N/A")
            row[f"{asset}_early_cagr"] = re.get("cagr", "N/A")
            row[f"{asset}_early_mdd"]  = re.get("mdd", "N/A")
            row[f"{asset}_chain_stops"] = r.get("chain_stop_events", "N/A")
        summary_rows.append(row)

    # 최우수 전략 결정 (Calmar 기준, QQQ 기준)
    best_strat = max(strategies, key=lambda s: float(
        results_full.get(f"{s}_QQQ", {}).get("calmar", -999) or -999))
    best_row = results_full.get(f"{best_strat}_QQQ", {})
    print(f"\n✅ Calmar 기준 최우수 전략: {best_strat} (QQQ Calmar={best_row.get('calmar','?')})")

    # ── 닷컴/GFC 트레이드 로그 ──
    dotcom_logs = []
    gfc_logs = []
    for key, trades in trade_logs.items():
        strat, asset = key.split("_", 1)
        if strat != "A":
            continue   # 베이스라인만 상세 로그
        for t in trades:
            t_entry = t.get("entry_date","")
            if "1999-12-01" <= t_entry <= "2003-03-31":
                dotcom_logs.append({**t, "key": key})
            elif "2007-06-01" <= t_entry <= "2009-06-30":
                gfc_logs.append({**t, "key": key})

    # 연쇄 손절 집계
    chain_summary = {}
    for key, trades in trade_logs.items():
        if not key.startswith("A_"):
            continue
        stops = [t for t in trades if t["exit_reason"] == "stop_loss"]
        # 연속 손절 계산
        consec = 0; max_consec = 0; chains = 0
        for t in sorted(trades, key=lambda x: x["entry_date"]):
            if t["exit_reason"] == "stop_loss":
                consec += 1
                max_consec = max(max_consec, consec)
                if consec >= 2:
                    chains += 1
            else:
                consec = 0
        chain_summary[key] = {"total_stops": len(stops), "max_consecutive": max_consec, "chain_events": chains}

    # ── JSON 저장 ──
    output = {
        "generated": date.today().isoformat(),
        "mode": "demo" if demo_mode else "real",
        "best_strategy": best_strat,
        "best_strategy_rationale": "Calmar Ratio (CAGR/|MDD|) 기준, QQQ 전구간",
        "summary": summary_rows,
        "chain_stop_summary": chain_summary,
        "validation": validation_results,
        "detail": {k: {kk: vv for kk, vv in v.items() if kk != "equity_curve"}
                   for k, v in results_full.items()},
        "early_detail": {k: {kk: vv for kk, vv in v.items() if kk != "equity_curve"}
                         for k, v in results_early.items()},
    }

    out_json = BACKTEST_DIR / "results.json"
    out_json.write_text(json.dumps(output, ensure_ascii=False, indent=2))
    print(f"\n저장: {out_json}")

    # ── Markdown 보고서 ──
    _write_results_md(output, summary_rows, dotcom_logs, gfc_logs, chain_summary, demo_mode)
    _write_trade_log_md(dotcom_logs, "닷컴 붕괴(2000~2003) 트레이드 로그", BACKTEST_DIR / "trade_log_dotcom.md")
    _write_trade_log_md(gfc_logs,    "GFC(2007~2009) 트레이드 로그",       BACKTEST_DIR / "trade_log_gfc.md")
    return output

def _write_results_md(output, summary_rows, dotcom_logs, gfc_logs, chain_summary, demo_mode):
    lines = []
    lines.append("# ETF 레버리지 전략 백테스트 결과 (1999~2026)")
    lines.append(f"\n> 생성: {output['generated']} | 모드: {'합성 데이터(데모)' if demo_mode else '실데이터'}")
    lines.append(f"\n**최우수 전략: {output['best_strategy']}** — {output['best_strategy_rationale']}")
    lines.append("\n---\n")

    strat_desc = {
        "A": "현행 전략 (베이스라인): MA20×0.95 신호 + VIX 레버 + +8% 익절 + -15% 손절 + MA20복귀 + 60일컷",
        "B": "현행 + MA200 전체 필터: 종가>MA200일 때만 레버리지 진입 허용",
        "C": "현행 + 3× 티어만 MA200 필터 (2×/2.5×는 그대로)",
        "D": "VIX 레벨 대신 VIX>VIX3M(백워데이션) 여부로 2×/3× 결정",
        "E": "+8% 익절 제거 (MA20복귀/손절/60일컷만 유지)",
    }

    lines.append("## 전략 정의\n")
    for k, v in strat_desc.items():
        lines.append(f"- **{k}**: {v}")

    lines.append("\n---\n")
    lines.append("## 전구간 성과 비교 (1999-01-01 ~ 2026-06-10)\n")

    for asset in ["QQQ","SPY","SOXX"]:
        lines.append(f"### {asset}\n")
        lines.append("| 전략 | CAGR | MDD | Calmar | 승률 | 거래수 | 평균보유일 | 1999-2009 CAGR | 1999-2009 MDD | 연쇄손절수 |")
        lines.append("|------|------|-----|--------|------|--------|-----------|---------------|--------------|-----------|")
        for row in summary_rows:
            s = row["strategy"]
            lines.append(
                f"| {s} | {row[f'{asset}_cagr']}% | {row[f'{asset}_mdd']}% | "
                f"{row[f'{asset}_calmar']} | {row[f'{asset}_wr']}% | "
                f"{row[f'{asset}_n']} | {row[f'{asset}_hold']}일 | "
                f"{row[f'{asset}_early_cagr']}% | {row[f'{asset}_early_mdd']}% | "
                f"{row[f'{asset}_chain_stops']} |"
            )
        lines.append("")

    lines.append("---\n")
    lines.append("## 닷컴 붕괴(2000~2003) 연쇄 손절 요약\n")
    for key, cs in chain_summary.items():
        lines.append(f"- **{key}**: 총 손절 {cs['total_stops']}회, 최대 연속 {cs['max_consecutive']}회, 연쇄이벤트 {cs['chain_events']}회")

    lines.append("\n*자세한 트레이드 로그는 trade_log_dotcom.md, trade_log_gfc.md 참조*")

    (BACKTEST_DIR / "results.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"저장: {BACKTEST_DIR / 'results.md'}")

def _write_trade_log_md(logs, title, path):
    lines = [f"# {title}\n",
             "| 자산 | 전략 | 진입일 | 청산일 | VIX | 레버 | 보유일 | 청산사유 | 기초수익 | 레버수익 | 연속손절 |",
             "|------|------|--------|--------|-----|------|--------|---------|---------|---------|---------|"]
    for t in sorted(logs, key=lambda x: x.get("entry_date","")):
        key = t.pop("key", "?")
        strat, asset = key.split("_", 1) if "_" in key else ("?", "?")
        reason_map = {
            "stop_loss":   "❌손절",
            "take_profit": "✅익절",
            "ma_return":   "🔄MA복귀",
            "timeout":     "⏱60일컷",
        }
        reason = reason_map.get(t.get("exit_reason",""), t.get("exit_reason",""))
        lines.append(
            f"| {asset} | {strat} | {t.get('entry_date','')} | {t.get('exit_date','')} | "
            f"{t.get('vix','?')} | {t.get('lev','?')}× | {t.get('hold_days','?')}일 | "
            f"{reason} | {t.get('base_ret','?')}% | {t.get('lev_ret','?')}% | "
            f"{t.get('consec_stops',0)} |"
        )
        t["key"] = key
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"저장: {path}")

# ══════════════════════════════════════════════
#  메인
# ══════════════════════════════════════════════
if __name__ == "__main__":
    demo_mode = "--demo" in sys.argv
    result = run_all(demo_mode=demo_mode)
