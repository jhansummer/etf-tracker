#!/usr/bin/env python3
"""
ETF 포트폴리오 통합 빌드
data/*.json → docs/data/combined.json

매매 vs 주가효과 판별:
  - 수량 변화 > 3% → 매수/매도 (의도적 리밸런싱)
  - 수량 동일 + 비중 변화 → 주가효과
  - 이전에 없던 종목 → 신규편입
  - 이전에 있었는데 사라진 종목 → 제외
"""
import json, re
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
OUT = ROOT / "docs" / "data" / "combined.json"

SHARE_THRESHOLD = 0.03  # 3%

def load_snapshots():
    """data/ 에서 time_*.json, koact_*.json, kosdaq_*.json 로드"""
    snapshots = {"time": [], "koact": [], "kosdaq": []}
    for f in sorted(DATA.glob("*.json")):
        if f.name == "sector_map.json":
            continue
        m = re.match(r"(time|koact|kosdaq)_(\d{4}-\d{2}-\d{2})\.json", f.name)
        if not m:
            continue
        key = m.group(1)
        data = json.loads(f.read_text(encoding="utf-8"))
        snapshots[key].append(data)
    # 날짜순 정렬
    for key in snapshots:
        snapshots[key].sort(key=lambda x: x["date"])
    return snapshots

def compute_changes(current, previous):
    """이전 주 대비 변화 계산"""
    prev_map = {}
    if previous:
        for h in previous.get("holdings", []):
            prev_map[h["ticker"]] = h

    curr_map = {}
    for h in current.get("holdings", []):
        curr_map[h["ticker"]] = h

    results = []
    for h in current["holdings"]:
        ticker = h["ticker"]
        entry = dict(h)
        prev = prev_map.get(ticker)

        if prev is None:
            entry["signal"] = "new"  # 신규편입
            entry["prev_shares"] = None
            entry["prev_weight"] = None
            entry["shares_chg_pct"] = None
            entry["weight_chg"] = None
        else:
            entry["prev_shares"] = prev.get("shares")
            entry["prev_weight"] = prev.get("weight")

            cur_shares = h.get("shares")
            prv_shares = prev.get("shares")
            cur_weight = h.get("weight") or 0
            prv_weight = prev.get("weight") or 0
            entry["weight_chg"] = round(cur_weight - prv_weight, 2)

            # 수량 기반 매매 판별
            if cur_shares is not None and prv_shares is not None and prv_shares > 0:
                chg_pct = (cur_shares - prv_shares) / prv_shares
                entry["shares_chg_pct"] = round(chg_pct * 100, 2)

                if abs(chg_pct) > SHARE_THRESHOLD:
                    entry["signal"] = "buy" if chg_pct > 0 else "sell"
                else:
                    entry["signal"] = "price_effect"
            elif cur_shares is None and prv_shares is None:
                # 둘 다 수량 없음 (KoAct) — 비중으로만 판단
                weight_diff = abs(cur_weight - prv_weight)
                if weight_diff > 1.0:
                    entry["signal"] = "buy" if cur_weight > prv_weight else "sell"
                else:
                    entry["signal"] = "price_effect"
                entry["shares_chg_pct"] = None
            else:
                entry["signal"] = "price_effect"
                entry["shares_chg_pct"] = None

        results.append(entry)

    # 제외된 종목
    for ticker, prev in prev_map.items():
        if ticker not in curr_map:
            entry = dict(prev)
            entry["signal"] = "removed"
            entry["weight"] = 0
            entry["prev_weight"] = prev.get("weight")
            entry["weight_chg"] = -(prev.get("weight") or 0)
            entry["shares_chg_pct"] = None
            entry["prev_shares"] = prev.get("shares")
            entry["shares"] = 0
            entry["value_krw"] = 0
            results.append(entry)

    return results

def compute_sector_breakdown(holdings):
    """섹터별 비중 합계"""
    sectors = defaultdict(float)
    for h in holdings:
        if h.get("signal") == "removed":
            continue
        sectors[h.get("sector", "미분류")] += h.get("weight", 0) or 0
    result = [{"sector": k, "weight": round(v, 2)} for k, v in sectors.items()]
    result.sort(key=lambda x: -x["weight"])
    return result

def build_etf_data(snapshots):
    """한 ETF의 전체 주차별 데이터 생성"""
    weeks = []
    for i, snap in enumerate(snapshots):
        prev = snapshots[i - 1] if i > 0 else None
        holdings_with_changes = compute_changes(snap, prev)
        sectors = compute_sector_breakdown(holdings_with_changes)

        week = {
            "date": snap["date"],
            "etf": snap["etf"],
            "code": snap["code"],
            "nav": snap.get("nav"),
            "aum_billion": snap.get("aum_billion"),
            "holdings": holdings_with_changes,
            "sectors": sectors,
            "total_holdings": len([h for h in holdings_with_changes if h.get("signal") != "removed"]),
        }

        if prev is None:
            # 첫 주 — 모든 종목이 "신규"가 아닌 "기존"으로 표시
            for h in week["holdings"]:
                h["signal"] = "hold"
        weeks.append(week)
    return weeks

def build_ticker_history(all_weeks, etf_key):
    """종목별 주간 비중 히스토리"""
    history = defaultdict(list)
    for week in all_weeks:
        for h in week["holdings"]:
            if h.get("signal") == "removed":
                continue
            history[h["ticker"]].append({
                "date": week["date"],
                "weight": h.get("weight"),
                "shares": h.get("shares"),
            })
    return dict(history)

def build_overlap(time_weeks, koact_weeks):
    """두 ETF 최신 주의 공통 종목 비교"""
    if not time_weeks or not koact_weeks:
        return []
    time_latest = time_weeks[-1]
    koact_latest = koact_weeks[-1]

    time_map = {h["ticker"]: h for h in time_latest["holdings"] if h.get("signal") != "removed"}
    koact_map = {h["ticker"]: h for h in koact_latest["holdings"] if h.get("signal") != "removed"}

    all_tickers = set(time_map.keys()) | set(koact_map.keys())
    overlap = []
    for ticker in sorted(all_tickers):
        t = time_map.get(ticker)
        k = koact_map.get(ticker)
        overlap.append({
            "ticker": ticker,
            "name": (t or k)["name"],
            "sector": (t or k).get("sector", "미분류"),
            "time_weight": t["weight"] if t else None,
            "koact_weight": k["weight"] if k else None,
            "both": ticker in time_map and ticker in koact_map,
        })
    overlap.sort(key=lambda x: -(x.get("time_weight") or 0) - (x.get("koact_weight") or 0))
    return overlap

def main():
    snapshots = load_snapshots()

    etf_keys = ["time", "koact", "kosdaq"]
    all_weeks = {}
    all_history = {}
    dates = []

    for key in etf_keys:
        weeks = build_etf_data(snapshots[key])
        all_weeks[key] = weeks
        all_history[key] = build_ticker_history(weeks, key)
        if weeks:
            dates.append(weeks[-1]["date"])

    overlap = build_overlap(all_weeks.get("time", []), all_weeks.get("koact", []))

    combined = {
        "generated": max(dates) if dates else "",
        "etf_keys": etf_keys,
    }
    for key in etf_keys:
        combined[key] = {
            "weeks": all_weeks[key],
            "history": all_history[key],
        }
    combined["overlap"] = overlap

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(
        json.dumps(combined, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"빌드 완료: {OUT}")
    for key in etf_keys:
        w = all_weeks[key]
        if w:
            latest = w[-1]
            signals = defaultdict(int)
            for h in latest["holdings"]:
                signals[h["signal"]] += 1
            print(f"{latest['etf']} ({latest['date']}): {len(w)}일, {dict(signals)}")

if __name__ == "__main__":
    main()
