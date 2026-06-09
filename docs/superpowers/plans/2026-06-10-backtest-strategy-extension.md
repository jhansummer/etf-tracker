# Backtest Strategy Extension + UI Signal Enhancement

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add user-spec strategies B2 (MA200 skip) and C2 (MA200 skip-3x-only) to the backtest engine, regenerate results, and update the UI to show "skip" semantics + gray filtered signals.

**Architecture:** The existing `backtest/run_backtest.py` already implements A/E/G (user's A/D/E). Only two new strategy codes need adding. `docs/index.html` already has the VIX3M banner, MA200 display, and backtest summary table — only the skip-semantics signal display is missing.

**Tech Stack:** Python 3 (backtest), vanilla JS (dashboard), Chart.js 4, stooq/yfinance data, GitHub Pages static hosting.

---

## Pre-work: Codebase map

| File | What to touch |
|------|--------------|
| `backtest/run_backtest.py:619` | Add `"B2","C2"` to `STRATEGIES` list |
| `backtest/run_backtest.py:401-420` | Add B2/C2 branch in `backtest()` entry logic |
| `backtest/run_backtest.py:794-806` | Add B2/C2 to `_STRAT_DESC` |
| `backtest/run_backtest.py:836` | Add `user_comparison_table` to JSON output |
| `docs/index.html:1534-1578` | Update signal action logic for skip semantics |
| `docs/index.html:1767-1829` | Extend `buildBacktestSummaryTable` with user-labels A–E |

## Strategy mapping (user label → code)

| User | Code | Description |
|------|------|-------------|
| A | `A` | Baseline (already exists) |
| B | `B2` | Skip entry entirely when price < MA200 |
| C | `C2` | Skip only 3× entries when price < MA200; allow 2×/2.5× |
| D | `E` | VIX term structure: VIX>VIX3M→3×, contango→2× (already exists) |
| E | `G` | No +8% profit-taking (already exists) |

---

## Task 1: Add B2 strategy to `run_backtest.py`

**Files:**
- Modify: `backtest/run_backtest.py:401-420` (entry logic inside `backtest()`)
- Modify: `backtest/run_backtest.py:619` (STRATEGIES list)
- Modify: `backtest/run_backtest.py:794-806` (_STRAT_DESC)

- [ ] **Step 1: Add B2 to STRATEGIES list (line 619)**

Open `backtest/run_backtest.py`. Find line 619:
```python
STRATEGIES = ["A","B","C","D","E","F50","F70","G","C+F70","D+G","B+G"]
```
Change to:
```python
STRATEGIES = ["A","B","B2","C","C2","D","E","F50","F70","G","C+F70","D+G","B+G"]
```

- [ ] **Step 2: Add B2/C2 entry logic**

In `backtest()`, find the block at line ~401 that starts with:
```python
                    # 2. 전략별 레버 조정/차단
                    if strategy in ("B", "B+G") and ma200_s[i] is not None:
                        if c <= ma200_s[i]:
                            lev = min(lev, 2.0)  # MA200 아래면 최대 2x
```

Add two new `elif` branches immediately after the `D`/`D+G` block (after line ~411):
```python
                    elif strategy == "B2" and ma200_s[i] is not None:
                        # 사용자 전략B: MA200 하방이면 진입 완전 스킵
                        if c <= ma200_s[i]:
                            lev = None

                    elif strategy == "C2" and ma200_s[i] is not None:
                        # 사용자 전략C: MA200 하방 + VIX 3x 티어면 스킵; 2x/2.5x는 허용
                        if c <= ma200_s[i] and lev == 3.0:
                            lev = None
```

The `if lev is not None:` check at line ~397 means setting `lev = None` correctly prevents entry.

- [ ] **Step 3: Add B2/C2 to _STRAT_DESC (line ~794)**

Find `_STRAT_DESC` dict. Add entries:
```python
    "B2":   "MA200 스킵: 종가<MA200이면 진입 완전 스킵 (레버 강등 X, 거래 없음)",
    "C2":   "MA200 3x스킵: 종가<MA200 + VIX 3x 티어면 스킵; 2x/2.5x는 그대로 진입",
```

- [ ] **Step 4: Add B2/C2 to _CRISIS_NOTES (line ~808)**

```python
    "B2":  ("닷컴(2000~02): MA200 아래 구간에서 모든 신호 스킵. 3x 손실 원천 차단. "
            "단점: 붕괴 중반 이후 저점 매수 기회를 완전히 포기. "
            "GFC(2008): 하락 초기엔 MA200 위였으므로 초기 1~2회 진입 가능, 이후 스킵."),
    "C2":  ("닷컴: 3x 진입만 차단. VIX 20~30(2x) 구간 신호는 MA200 하방에서도 허용. "
            "닷컴 붕괴 초기 VIX 구간이 20~30에 머물었으므로 2x 진입은 계속 발생. "
            "GFC: VIX 급등 시 3x 스킵되나 2x는 허용 → B2보다 트레이드 수 많음."),
```

- [ ] **Step 5: Add `user_comparison_table` to JSON output (in `run_all()`)**

In the `output` dict construction (around line 765), add:
```python
    # 사용자 지정 A-E 비교 (user_label → internal_code)
    USER_STRAT_MAP = [
        ("A", "A",  "베이스라인"),
        ("B", "B2", "MA200 스킵"),
        ("C", "C2", "MA200 3x스킵"),
        ("D", "E",  "VIX 기간구조"),
        ("E", "G",  "익절 제거"),
    ]
    user_comparison = []
    for user_lbl, code, desc in USER_STRAT_MAP:
        row = {"user_label": user_lbl, "code": code, "desc": desc, "assets": {}}
        for asset in ASSETS:
            k  = f"{code}_{asset}"
            r  = full_results.get(k, {})
            re = early_results.get(k, {})
            row["assets"][asset] = {
                "cagr":       r.get("cagr"),
                "mdd":        r.get("mdd"),
                "calmar":     r.get("calmar"),
                "win_rate":   r.get("win_rate"),
                "n_trades":   r.get("n_trades"),
                "avg_hold":   r.get("avg_hold_days"),
                "early_cagr": re.get("cagr"),
                "early_mdd":  re.get("mdd"),
            }
        user_comparison.append(row)
    output["user_comparison"] = user_comparison
```

- [ ] **Step 6: Add early gap scenario backtest for all strategies**

In `run_all()`, find the existing early_results loop. After line:
```python
            re = backtest(asset, base, vix_map, v3m, rf_map, strat,
                          strat_start, "2009-01-01", False)
```
Add a gap version:
```python
            re_gap = backtest(asset, base, vix_map, v3m, rf_map, strat,
                              strat_start, "2009-01-01", True)
            early_gap_results[f"{strat}_{asset}"] = re_gap
```

Also declare `early_gap_results = {}` at the top of the loop. Include in `output`:
```python
    "early_gap_detail": {k: {kk: vv for kk, vv in v.items()
                             if kk not in ("trades","eq_curve")}
                        for k, v in early_gap_results.items()},
```

- [ ] **Step 7: Commit Task 1**

```bash
cd /Users/hanjin/etf-tracker
git add backtest/run_backtest.py
git commit -m "feat(backtest): add B2/C2 strategy variants (MA200 skip semantics)"
```

---

## Task 2: Run backtest and regenerate results

**Files:**
- Read/write: `backtest/results.json`, `backtest/results.md`, `backtest/trade_log_*.md`

- [ ] **Step 1: Run the backtest (real data)**

```bash
cd /Users/hanjin/etf-tracker
python backtest/run_backtest.py
```

Expected: downloads/uses cached data, runs A+B+B2+C+C2+D+E+F50+F70+G+C+F70+D+G+B+G strategies × 3 assets × 2 periods, writes results.json/results.md.

If real data fails, run demo mode first to verify logic:
```bash
python backtest/run_backtest.py --demo
```

- [ ] **Step 2: Verify results.json contains B2/C2 and user_comparison**

```bash
python -c "
import json
bt = json.load(open('backtest/results.json'))
strats = [r['strategy'] for r in bt['summary']]
print('Strategies:', strats)
print('Has B2:', 'B2' in strats)
print('Has C2:', 'C2' in strats)
print('Has user_comparison:', 'user_comparison' in bt)
uc = bt.get('user_comparison',[])
for row in uc:
    qqq = row['assets'].get('QQQ',{})
    print(f\"  {row['user_label']} ({row['code']}): CAGR={qqq.get('cagr')} MDD={qqq.get('mdd')} Calmar={qqq.get('calmar')}\")
"
```

Expected: B2 and C2 in strategies list, user_comparison has 5 rows with numeric values.

- [ ] **Step 3: Check best strategy result**

```bash
python -c "
import json
bt = json.load(open('backtest/results.json'))
print('Best strategy:', bt['best_strategy'], 'Calmar:', bt['best_strategy_calmar'])
# Print B2 vs A comparison for QQQ
for row in bt['summary']:
    if row['strategy'] in ('A','B2','C2','E','G'):
        q = row['assets'].get('QQQ',{})
        print(f\"{row['strategy']:5} CAGR={q.get('cagr'):8.2f} MDD={q.get('mdd'):7.2f} Calmar={q.get('calmar'):6.3f}\")
"
```

- [ ] **Step 4: Commit results**

```bash
cd /Users/hanjin/etf-tracker
git add backtest/results.json backtest/results.md backtest/trade_log_dotcom.md backtest/trade_log_gfc.md
git commit -m "feat(backtest): regenerate results with B2/C2 strategies (1999-2026)"
```

---

## Task 3: Update `index.html` — skip semantics + user A–E comparison table

**Files:**
- Modify: `docs/index.html:1534-1578` (renderMAAlertCards action logic)
- Modify: `docs/index.html:1767-1830` (buildBacktestSummaryTable)

### Part A: Skip semantics in signal cards

- [ ] **Step 1: Read current action logic (lines 1534–1578)**

The block reads:
```javascript
    if (d.alert && recLev) {
      // B필터: close < MA200 → 최대 2×
      if (d.ma200 && d.current < d.ma200 && recLev > 2) {
        appliedLev = 2.0;
        downgrades.push("MA200 하방 → " + recLev + "×→2× (B)");
      }
      // C필터: MA200 기울기 하락 → 3× 차단
      if (d.ma200_slope === "falling" && (appliedLev||recLev) >= 3) {
        appliedLev = Math.min(appliedLev||recLev, 2.0);
        if (!downgrades.length || downgrades[0].indexOf("(B)") < 0)
          downgrades.push("MA200 기울기↓ → 3×→2× (C)");
      }
      // D필터: 52주 낙폭 ≤ -35% → 최대 2×
      if (d.w52_drawdown != null && d.w52_drawdown <= -35 && (appliedLev||recLev) > 2) {
        appliedLev = Math.min(appliedLev||recLev, 2.0);
        downgrades.push("52주낙폭 " + d.w52_drawdown.toFixed(1) + "% → 2× (D)");
      }
    }
```

- [ ] **Step 2: Add skip-semantics for B2/C2 best strategy**

Replace the above block with the following (adds skip logic before the cap logic):
```javascript
    // ── 채택된 최우수 전략에서 파생된 필터 ──
    var bestStrat = (DATA.backtest_summary && DATA.backtest_summary.best_strategy) || '';
    var signalSkipped = false;
    var skipReason = '';

    if (d.alert && recLev) {
      // B2 전략이 최우수 → MA200 하방이면 진입 완전 스킵
      if (bestStrat === 'B2' && d.ma200 && d.current < d.ma200) {
        signalSkipped = true;
        skipReason = 'MA200 하방 — 진입 스킵 (전략 B)';
      }
      // C2 전략이 최우수 → MA200 하방 + 3x 티어이면 스킵
      else if (bestStrat === 'C2' && d.ma200 && d.current < d.ma200 && recLev >= 3) {
        signalSkipped = true;
        skipReason = 'MA200 하방 + 3× → 진입 스킵 (전략 C)';
      }
    }

    if (!signalSkipped && d.alert && recLev) {
      // B필터(기존): close < MA200 → 최대 2×
      if (d.ma200 && d.current < d.ma200 && recLev > 2) {
        appliedLev = 2.0;
        downgrades.push("MA200 하방 → " + recLev + "×→2× (B)");
      }
      // C필터: MA200 기울기 하락 → 3× 차단
      if (d.ma200_slope === "falling" && (appliedLev||recLev) >= 3) {
        appliedLev = Math.min(appliedLev||recLev, 2.0);
        if (!downgrades.length || downgrades[0].indexOf("(B)") < 0)
          downgrades.push("MA200 기울기↓ → 3×→2× (C)");
      }
      // D필터: 52주 낙폭 ≤ -35% → 최대 2×
      if (d.w52_drawdown != null && d.w52_drawdown <= -35 && (appliedLev||recLev) > 2) {
        appliedLev = Math.min(appliedLev||recLev, 2.0);
        downgrades.push("52주낙폭 " + d.w52_drawdown.toFixed(1) + "% → 2× (D)");
      }
    }
```

- [ ] **Step 3: Update action string generation to handle skip case**

Find the block starting at line ~1559:
```javascript
    if (!d.alert) {
      ...
    } else if (!recLev) {
      ...
    } else {
      ...
    }
```

Add a `else if (signalSkipped)` branch before the final `else`:
```javascript
    } else if (signalSkipped) {
      action = "⚪ " + skipReason;
      actionColor = "var(--muted)";
```

- [ ] **Step 4: Update the action tile UI to render skip state differently**

Find the "추천 액션" tile HTML (~line 1636):
```javascript
      + '<div style="flex:1.5;background:'+(d.alert?'rgba(239,68,68,0.08)':'var(--surface2)')+';border-radius:7px;padding:8px 12px;border:1px solid '+(d.alert?'rgba(239,68,68,0.3)':'var(--border)')+';">'
      + '<div style="font-size:9px;color:var(--muted);margin-bottom:3px">추천 액션</div>'
      + '<div style="font-size:11px;font-weight:700;color:' + actionColor + '">' + action + '</div></div>'
```

Change to:
```javascript
      + '<div style="flex:1.5;background:'+(signalSkipped?'rgba(139,143,163,0.07)':(d.alert?'rgba(239,68,68,0.08)':'var(--surface2)'))+';border-radius:7px;padding:8px 12px;border:1px solid '+(signalSkipped?'var(--border)':(d.alert?'rgba(239,68,68,0.3)':'var(--border)')+'')+';">'
      + '<div style="font-size:9px;color:var(--muted);margin-bottom:3px">추천 액션</div>'
      + '<div style="font-size:11px;font-weight:700;color:' + actionColor + '">' + action + '</div></div>'
```

Also update card border to not show red when skipped:
```javascript
    var cardBorder = (!signalSkipped && d.alert) ? "border-color:rgba(239,68,68,0.5);" : "";
```
(Find `var cardBorder = d.alert ?` around line 1614 and change `d.alert` to `(!signalSkipped && d.alert)`.)

- [ ] **Step 5: Add "skipped" badge to symbol header**

Find the symbol badge `신호` display (~line 1620):
```javascript
      + (d.alert ? ' <span style="font-size:10px;padding:2px 7px;border-radius:5px;background:rgba(239,68,68,0.2);color:#ef4444;vertical-align:middle">신호</span>' : '')
```

Change to:
```javascript
      + (signalSkipped ? ' <span style="font-size:10px;padding:2px 7px;border-radius:5px;background:rgba(139,143,163,0.15);color:var(--muted);vertical-align:middle">스킵됨</span>'
         : d.alert ? ' <span style="font-size:10px;padding:2px 7px;border-radius:5px;background:rgba(239,68,68,0.2);color:#ef4444;vertical-align:middle">신호</span>' : '')
```

### Part B: Add user A–E comparison section to backtest summary table

- [ ] **Step 6: Extend `buildBacktestSummaryTable()` with user_comparison table**

Find the end of `buildBacktestSummaryTable()` (around line 1840, before `html += '</div>'; return html;`). Add:

```javascript
  // ── 사용자 지정 A-E 전략 비교표 ──
  var uc = bt.user_comparison;
  if (uc && uc.length) {
    html += '<div style="font-size:11px;font-weight:700;color:var(--ink);margin:20px 0 8px">📋 전략 A–E 비교 (사용자 지정)</div>';
    html += '<div style="font-size:10px;color:var(--muted);margin-bottom:10px">'
      + 'A=기본 · B=MA200스킵 · C=3×스킵 · D=VIX기간구조 · E=익절제거</div>';

    var ucAssets = ["QQQ","SPY","SOXX"];
    ucAssets.forEach(function(asset) {
      html += '<div style="font-size:11px;font-weight:700;color:var(--ink);margin:12px 0 4px">' + asset + '</div>';
      html += '<div style="overflow-x:auto"><table class="tbl" style="font-size:10px;min-width:480px">';
      html += '<thead><tr>'
        + '<th style="text-align:left;padding:4px 8px;color:var(--muted);font-weight:600;border-bottom:1px solid var(--border)">전략</th>'
        + '<th style="text-align:right;padding:4px 8px;color:var(--muted);font-weight:600;border-bottom:1px solid var(--border)">CAGR%</th>'
        + '<th style="text-align:right;padding:4px 8px;color:var(--muted);font-weight:600;border-bottom:1px solid var(--border)">MDD%</th>'
        + '<th style="text-align:right;padding:4px 8px;color:var(--muted);font-weight:600;border-bottom:1px solid var(--border)">칼마</th>'
        + '<th style="text-align:right;padding:4px 8px;color:var(--muted);font-weight:600;border-bottom:1px solid var(--border)">승률%</th>'
        + '<th style="text-align:right;padding:4px 8px;color:var(--muted);font-weight:600;border-bottom:1px solid var(--border)">99–09 CAGR</th>'
        + '</tr></thead><tbody>';

      var ucBest = uc.reduce(function(best, row) {
        var cal = (row.assets[asset] || {}).calmar || -999;
        return cal > ((best.assets[asset] || {}).calmar || -999) ? row : best;
      }, uc[0]);

      uc.forEach(function(row) {
        var m = row.assets[asset] || {};
        var isBest = row === ucBest;
        var rowBg = isBest ? 'background:rgba(99,102,241,0.1);' : '';
        var cagrClr   = (m.cagr  || 0) > 0 ? '#22c55e' : '#ef4444';
        var mddClr    = (m.mdd   || 0) < -50 ? '#ef4444' : (m.mdd||0) < -30 ? '#f59e0b' : 'var(--muted)';
        var calmarClr = (m.calmar|| 0) > 1   ? '#22c55e' : (m.calmar||0) > 0.3 ? '#f59e0b' : 'var(--muted)';
        html += '<tr style="' + rowBg + 'border-bottom:1px solid var(--border)">'
          + '<td style="padding:4px 8px;font-weight:' + (isBest?'700':'400') + ';color:' + (isBest?'var(--accent2)':'var(--ink)') + '">'
          + row.user_label + ' ' + row.desc + (isBest ? ' ★' : '') + '</td>'
          + '<td style="text-align:right;padding:4px 8px;color:'+cagrClr+';font-weight:700">'
          + (m.cagr != null ? (m.cagr > 0 ? '+' : '') + m.cagr.toFixed(1) : '—') + '</td>'
          + '<td style="text-align:right;padding:4px 8px;color:'+mddClr+'">'
          + (m.mdd != null ? m.mdd.toFixed(1) : '—') + '</td>'
          + '<td style="text-align:right;padding:4px 8px;color:'+calmarClr+';font-weight:700">'
          + (m.calmar != null ? m.calmar.toFixed(3) : '—') + '</td>'
          + '<td style="text-align:right;padding:4px 8px">'
          + (m.win_rate != null ? m.win_rate.toFixed(1) : '—') + '</td>'
          + '<td style="text-align:right;padding:4px 8px;color:' + ((m.early_cagr||0)>0?'#22c55e':'#ef4444') + '">'
          + (m.early_cagr != null ? (m.early_cagr>0?'+':'')+m.early_cagr.toFixed(1) : '—') + '</td>'
          + '</tr>';
      });

      html += '</tbody></table></div>';
    });
  }
```

- [ ] **Step 7: Test in browser**

Run the local build and open the page:
```bash
cd /Users/hanjin/etf-tracker
python scripts/build.py
python -m http.server 8080 --directory docs
```
Open `http://localhost:8080`. Navigate to 매수알림 tab.

Verify:
1. VIX3M spread shows in the VIX banner (should already work, pre-existing)
2. MA200 status shows for each ETF card (should already work, pre-existing)
3. If best strategy is B2/C2, skipped signals show with gray "스킵됨" badge
4. Backtest summary table at bottom shows both full strategy list and user A–E table
5. Mobile layout not broken (resize to 375px width)

- [ ] **Step 8: Commit Task 3**

```bash
cd /Users/hanjin/etf-tracker
git add docs/index.html docs/data/combined.json
git commit -m "feat(ui): add skip-semantics for MA200 filter, user A-E comparison table"
```

---

## Self-Review

### Spec coverage check

| Requirement | Covered by |
|-------------|------------|
| 합성 레버 데이터 생성기 | Pre-existing in run_backtest.py — no change needed |
| 실제 ETF(TQQQ/SOXL/UPRO) 추적오차 검증 | Pre-existing validate_synth() — no change needed |
| 1999~2009 닷컴/GFC 트레이드 로그 | Pre-existing trade_log_*.md output — no change needed |
| 갭 시나리오 손절 계산 | Pre-existing gap=True/False — added early gap in Task 1 Step 6 |
| 전략 A | A (existing) |
| 전략 B (MA200 skip) | B2 (Task 1) |
| 전략 C (3x MA200 skip) | C2 (Task 1) |
| 전략 D (VIX term struct) | E (existing) |
| 전략 E (no profit-taking) | G (existing) |
| results.md + results.json | Task 2 (regenerate) |
| VIX 카드에 VIX3M 스프레드 | Pre-existing (build.py line 325, index.html line 1446) |
| 시그널 패널에 MA200 상태 | Pre-existing (build.py line 398, index.html line 1580) |
| MA200 필터 시그널 반영 + 스킵 회색 표시 | Task 3 Part A |
| 백테스트 요약 수치 미니 테이블 | Pre-existing buildBacktestSummaryTable() + Task 3 Part B adds user A-E view |
| crawl.py에 VIX3M + MA200 계산 추가 | Pre-existing (crawl.py line 818, build.py line 398) |
| 다크 테마 / CSS variables 유지 | Uses existing CSS vars throughout |
| 모바일 레이아웃 유지 | Uses existing flex/overflow-x:auto patterns |

### Gaps / placeholders: None found

### Type / name consistency:
- `signalSkipped` (boolean) used in 4 places: filter logic, action string, action tile bg, symbol badge — consistent
- `skipReason` (string) used in filter logic and action string — consistent
- `user_comparison` key in results.json matches `bt.user_comparison` in JS — consistent
- `early_gap_results` dict declared before loop, added to `output` — consistent

---

**Plan complete and saved to `docs/superpowers/plans/2026-06-10-backtest-strategy-extension.md`.**
