#!/usr/bin/env python3
"""
ETF 포트폴리오 자동 크롤링
사용법:
  python scripts/crawl.py                    # 오늘 날짜로 둘 다 크롤링
  python scripts/crawl.py 2026-04-04         # 특정 날짜
  python scripts/crawl.py 2026-04-04 time    # TIME만
  python scripts/crawl.py 2026-04-04 koact   # KoAct만
"""
import sys, json, re, os, time as _time
from pathlib import Path
from datetime import date, datetime
from html.parser import HTMLParser
import urllib.request, urllib.parse

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
SECTOR_MAP_PATH = DATA / "sector_map.json"

# ── 섹터맵 ──
def load_sector_map():
    if SECTOR_MAP_PATH.exists():
        return json.loads(SECTOR_MAP_PATH.read_text(encoding="utf-8"))
    return {}

def save_sector_map(smap):
    SECTOR_MAP_PATH.write_text(
        json.dumps(smap, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

# ── HTTP 유틸 ──
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}

def fetch_url(url, data=None, extra_headers=None):
    hdrs = dict(HEADERS)
    if extra_headers:
        hdrs.update(extra_headers)
    if data and isinstance(data, dict):
        data = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=hdrs)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read()

# ══════════════════════════════════════════════
#  TIME ETF — timeetf.co.kr 크롤링
# ══════════════════════════════════════════════

def strip_html(s):
    return re.sub(r"<[^>]+>", "", s).strip()


def parse_number_str(s):
    """'68,478,101' → 68478101"""
    s = s.replace(",", "").replace(" ", "").strip()
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None

def parse_float_str(s):
    s = s.replace(",", "").replace("%", "").replace(" ", "").strip()
    try:
        return round(float(s), 2)
    except (ValueError, TypeError):
        return None


def crawl_time(date_str):
    print("[TIME] timeetf.co.kr 크롤링 중...")
    url = "https://www.timeetf.co.kr/m11_view.php?idx=2"
    html = fetch_url(url).decode("utf-8", errors="replace")

    # NAV, AUM 파싱 시도
    nav = None
    aum = None
    nav_match = re.search(r"기준가[^\d]*?([\d,]+\.?\d*)", html)
    if nav_match:
        nav = parse_float_str(nav_match.group(1))
    aum_match = re.search(r"순자산[^\d]*?([\d,]+)", html)
    if aum_match:
        aum = parse_number_str(aum_match.group(1))

    # 정규식으로 종목 테이블 파싱 (헤더에 "종목코드"가 있는 첫 번째 테이블)
    tables = re.findall(r"<table[^>]*>(.*?)</table>", html, re.DOTALL)
    holdings_table = None
    for t in tables:
        if "종목코드" in t and "종목명" in t and "비중" in t:
            holdings_table = t
            break

    if not holdings_table:
        print("[TIME] 종목 테이블을 찾지 못했습니다.")
        return None

    # <tr> 안의 <td> 파싱
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", holdings_table, re.DOTALL)

    sector_map = load_sector_map()
    holdings = []

    for row_html in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, re.DOTALL)
        cells = [strip_html(c) for c in cells]
        if len(cells) < 5:
            continue

        raw_ticker = cells[0]  # "SNDK US EQUITY" 형태
        name = cells[1]
        shares = parse_number_str(cells[2])
        value_krw = parse_number_str(cells[3])
        weight = parse_float_str(cells[4])

        if not raw_ticker or not name:
            continue
        if weight is None or weight == 0:
            continue

        # "SNDK US EQUITY" → "SNDK", 선물/현금은 그대로
        ticker = raw_ticker.split()[0] if " " in raw_ticker else raw_ticker

        # 현금 처리
        if "현금" in name or "CASH" in raw_ticker.upper():
            ticker = "CASH"
            name = "현금"
        # 선물 처리 (NQM6 등)
        elif "E-MINI" in name.upper() or "NASDAQ 100" in name.upper() or "INDEX" in name.upper():
            # 티커 유지
            pass

        sector = sector_map.get(ticker, "미분류")
        holdings.append({
            "ticker": ticker,
            "name": name,
            "shares": shares,
            "value_krw": value_krw,
            "weight": weight,
            "sector": sector,
        })
        if ticker not in sector_map:
            sector_map[ticker] = "미분류"

    if not holdings:
        print("[TIME] 종목 데이터를 파싱하지 못했습니다.")
        return None

    save_sector_map(sector_map)

    snapshot = {
        "etf": "TIME",
        "code": "426030",
        "date": date_str,
        "nav": nav,
        "aum_billion": aum,
        "holdings": holdings,
    }

    out_path = DATA / f"time_{date_str}.json"
    out_path.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"[TIME] 저장: {out_path} ({len(holdings)}종목)")
    unmapped = [h["ticker"] for h in holdings if h["sector"] == "미분류"]
    if unmapped:
        print(f"[TIME] 미분류 섹터: {', '.join(unmapped)}")
    return snapshot


# ══════════════════════════════════════════════
#  삼성액티브 ETF — Playwright (samsungactive.co.kr)
# ══════════════════════════════════════════════

SAMSUNG_ETFS = {
    "koact": {
        "etf": "KoAct", "code": "0015B0",
        "url": "https://www.samsungactive.co.kr/etf/view.do?id=2ETFQ1",
        "file_prefix": "koact",
    },
    "kosdaq": {
        "etf": "코스닥", "code": "0163Y0",
        "url": "https://www.samsungactive.co.kr/etf/view.do?id=2ETFU6",
        "file_prefix": "kosdaq",
    },
}


def crawl_samsung(date_str, key="koact"):
    """Playwright로 samsungactive.co.kr 구성종목(PDF) 테이블 크롤링"""
    meta = SAMSUNG_ETFS[key]
    label = meta["etf"]
    print(f"[{label}] samsungactive.co.kr Playwright 크롤링 중...")

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print(f"[{label}] playwright 미설치: pip install playwright && playwright install chromium")
        return None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(meta["url"], timeout=30000)
            page.wait_for_load_state("networkidle", timeout=15000)

            # '구성종목(PDF)' 탭 클릭
            page.get_by_role("link", name="구성종목(PDF)").click()
            page.wait_for_timeout(2000)
            page.wait_for_load_state("networkidle", timeout=10000)

            # '더보기' 버튼 반복 클릭해서 전체 종목 로드
            for _ in range(10):
                btns = page.locator('button:has-text("더보기")')
                clicked = False
                for j in range(btns.count()):
                    btn = btns.nth(j)
                    if btn.is_visible() and "/" in (btn.text_content() or ""):
                        btn.click()
                        page.wait_for_timeout(1000)
                        clicked = True
                        break
                if not clicked:
                    break

            html = page.content()
            browser.close()
    except Exception as e:
        print(f"[{label}] Playwright 크롤링 실패: {e}")
        return None

    # 가장 큰 테이블 (종목명+종목코드+비중 포함) 파싱
    tables = re.findall(r"<table[^>]*>(.*?)</table>", html, re.DOTALL)
    holdings_table = None
    max_rows = 0
    for t in tables:
        if "종목코드" in t and "비중" in t:
            row_count = len(re.findall(r"<tr", t))
            if row_count > max_rows:
                max_rows = row_count
                holdings_table = t

    if not holdings_table:
        print(f"[{label}] 구성종목 테이블을 찾지 못했습니다.")
        return None

    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", holdings_table, re.DOTALL)
    sector_map = load_sector_map()
    holdings = []

    for row_html in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, re.DOTALL)
        cells = [strip_html(c) for c in cells]
        if len(cells) < 4:
            continue

        # 컬럼: 종목명, 종목코드, 수량, 비중(%), 평가금액(원), ...
        name = cells[0]
        raw_code = cells[1]   # "ARM US Equity" 또는 "CASH00000001"
        shares = parse_number_str(cells[2])
        weight = parse_float_str(cells[3])
        value_krw = parse_number_str(cells[4]) if len(cells) > 4 else None

        if not name or not raw_code:
            continue

        # 현금/원화 건너뛰기
        if "CASH" in raw_code or "KRD" in raw_code or "설정현금" in name or "원화현금" in name:
            continue
        # 비중 없는 행 건너뛰기
        if weight is None or weight == 0:
            continue

        # "ARM US Equity" → "ARM", 한국종목 "A005930" 그대로
        ticker = raw_code.split()[0] if " " in raw_code else raw_code

        sector = sector_map.get(ticker, "미분류")
        holdings.append({
            "ticker": ticker,
            "name": name,
            "shares": shares,
            "value_krw": value_krw,
            "weight": weight,
            "sector": sector,
        })
        if ticker not in sector_map:
            sector_map[ticker] = "미분류"

    if not holdings:
        print(f"[{label}] 종목 데이터를 파싱하지 못했습니다.")
        return None

    save_sector_map(sector_map)

    snapshot = {
        "etf": meta["etf"],
        "code": meta["code"],
        "date": date_str,
        "nav": None,
        "aum_billion": None,
        "holdings": holdings,
    }

    out_path = DATA / f"{meta['file_prefix']}_{date_str}.json"
    out_path.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"[{label}] 저장: {out_path} ({len(holdings)}종목)")
    unmapped = [h["ticker"] for h in holdings if h["sector"] == "미분류"]
    if unmapped:
        print(f"[{label}] 미분류 섹터: {', '.join(unmapped)}")
    return snapshot


# ══════════════════════════════════════════════
#  메인
# ══════════════════════════════════════════════

def main():
    today = date.today().isoformat()
    date_str = sys.argv[1] if len(sys.argv) > 1 else today
    target = sys.argv[2].lower() if len(sys.argv) > 2 else "all"

    print(f"=== ETF 크롤링 시작 ({date_str}) ===\n")

    if target in ("all", "time"):
        crawl_time(date_str)
        print()

    if target in ("all", "koact"):
        crawl_samsung(date_str, "koact")
        print()

    if target in ("all", "kosdaq"):
        crawl_samsung(date_str, "kosdaq")
        print()

    # 자동 빌드
    print("=== combined.json 빌드 ===")
    import subprocess
    subprocess.run([sys.executable, str(ROOT / "scripts" / "build.py")], check=True)

    print("\n완료! git push하면 대시보드에 반영됩니다.")

if __name__ == "__main__":
    main()
