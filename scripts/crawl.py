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
#  KoAct ETF — KRX API 크롤링
# ══════════════════════════════════════════════

KRX_OTP_URL = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
KRX_DOWNLOAD_URL = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
KOACT_ISIN = "KR7458360004"  # KoAct 미국나스닥성장기업액티브
KOACT_SHORT_CODE = "458360"


def crawl_koact_krx(date_str):
    """KRX PDF(포트폴리오 구성종목) 데이터 가져오기"""
    print("[KoAct] KRX API 크롤링 중...")

    trd_dd = date_str.replace("-", "")

    # OTP 발급
    otp_params = {
        "locale": "ko_KR",
        "isuCd": KOACT_ISIN,
        "isuCd2": KOACT_ISIN,
        "strtDd": trd_dd,
        "endDd": trd_dd,
        "share": "1",
        "money": "1",
        "csvxls_isNo": "false",
        "name": "fileDown",
        "url": "dbms/MDC/STAT/standard/MDCSTAT05901",
    }

    try:
        otp = fetch_url(KRX_OTP_URL, data=otp_params, extra_headers={
            "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201030105",
        }).decode("utf-8")
    except Exception as e:
        print(f"[KoAct] KRX OTP 발급 실패: {e}")
        return None

    # CSV 다운로드
    _time.sleep(1)
    try:
        csv_data = fetch_url(KRX_DOWNLOAD_URL, data={"code": otp}, extra_headers={
            "Referer": "http://data.krx.co.kr/",
        }).decode("utf-8-sig")
    except Exception as e:
        print(f"[KoAct] KRX 데이터 다운로드 실패: {e}")
        return None

    if not csv_data or "구성종목" not in csv_data and len(csv_data) < 100:
        print(f"[KoAct] KRX 응답이 비정상적입니다 (길이: {len(csv_data)})")
        print(f"  응답 시작: {csv_data[:200]}")
        return None

    return parse_koact_csv(csv_data, date_str)


def crawl_koact_samsungactive(date_str):
    """samsungactive.co.kr에서 크롤링 시도 (fallback)"""
    print("[KoAct] samsungactive.co.kr 크롤링 시도...")
    url = "https://www.samsungactive.co.kr/etf/view.do?id=2ETFQ1"

    try:
        html = fetch_url(url).decode("utf-8", errors="replace")
    except Exception as e:
        print(f"[KoAct] samsungactive 접속 실패: {e}")
        return None

    # JSON 데이터가 페이지에 임베딩되어 있는지 확인
    # 일반적으로 SPA는 __NEXT_DATA__ 또는 유사한 스크립트 태그에 데이터를 넣음
    json_match = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if json_match:
        try:
            next_data = json.loads(json_match.group(1))
            print("[KoAct] __NEXT_DATA__ 발견, 파싱 시도...")
            return parse_samsungactive_nextdata(next_data, date_str)
        except json.JSONDecodeError:
            pass

    # API 엔드포인트 패턴 탐색
    api_matches = re.findall(r'["\'](/api/[^"\']+)["\']', html)
    if api_matches:
        print(f"[KoAct] API 엔드포인트 발견: {api_matches}")

    print("[KoAct] samsungactive.co.kr에서 구성종목 데이터를 찾지 못했습니다.")
    return None


def parse_samsungactive_nextdata(data, date_str):
    """Next.js __NEXT_DATA__에서 종목 데이터 추출"""
    # 구조를 탐색하여 holdings 데이터를 찾음
    def find_holdings(obj, depth=0):
        if depth > 10:
            return None
        if isinstance(obj, list):
            # 리스트의 아이템이 종목 데이터인지 확인
            if len(obj) > 0 and isinstance(obj[0], dict):
                keys = set(obj[0].keys())
                # 비중/weight 관련 키가 있으면 holdings일 가능성
                if keys & {"weight", "비중", "ratio", "percent", "wght"}:
                    return obj
            for item in obj:
                result = find_holdings(item, depth + 1)
                if result:
                    return result
        elif isinstance(obj, dict):
            for v in obj.values():
                result = find_holdings(v, depth + 1)
                if result:
                    return result
        return None

    holdings_raw = find_holdings(data)
    if not holdings_raw:
        return None

    sector_map = load_sector_map()
    holdings = []
    for item in holdings_raw:
        ticker = item.get("ticker") or item.get("종목코드") or item.get("code") or ""
        name = item.get("name") or item.get("종목명") or ""
        weight = item.get("weight") or item.get("비중") or item.get("ratio") or 0
        if isinstance(weight, str):
            weight = parse_float_str(weight)
        sector = sector_map.get(ticker, "미분류")
        holdings.append({
            "ticker": ticker, "name": name,
            "shares": None, "value_krw": None,
            "weight": round(float(weight), 2) if weight else None,
            "sector": sector,
        })
        if ticker and ticker not in sector_map:
            sector_map[ticker] = "미분류"

    if holdings:
        save_sector_map(sector_map)
    return build_koact_snapshot(holdings, date_str)


def parse_koact_csv(csv_data, date_str):
    """KRX CSV 데이터에서 KoAct 종목 파싱"""
    import csv, io

    reader = csv.reader(io.StringIO(csv_data))
    header = next(reader, None)
    if not header:
        print("[KoAct] CSV 헤더가 없습니다.")
        return None

    # 컬럼 인덱스 찾기
    col_map = {}
    for i, h in enumerate(header):
        h = h.strip()
        if "종목코드" in h or "종목" in h and "코드" in h:
            col_map["ticker"] = i
        elif "종목명" in h:
            col_map["name"] = i
        elif "비중" in h:
            col_map["weight"] = i
        elif "수량" in h:
            col_map["shares"] = i
        elif "평가금액" in h or "금액" in h:
            col_map["value"] = i

    print(f"[KoAct] CSV 헤더: {header}")
    print(f"[KoAct] 컬럼 매핑: {col_map}")

    sector_map = load_sector_map()
    holdings = []

    for row in reader:
        if not row or len(row) < 3:
            continue
        ticker = row[col_map.get("ticker", 0)].strip() if "ticker" in col_map else ""
        name = row[col_map.get("name", 1)].strip() if "name" in col_map else ""
        weight = parse_float_str(row[col_map.get("weight", -1)]) if "weight" in col_map else None
        shares = parse_number_str(row[col_map.get("shares", -1)]) if "shares" in col_map else None
        value_krw = parse_number_str(row[col_map.get("value", -1)]) if "value" in col_map else None

        if not name:
            continue

        # KRX에서 가져온 데이터는 종목명이 한글일 수 있음 → 티커 매핑 필요
        # 미국 종목은 종목코드가 ISIN 형태일 수 있음
        sector = sector_map.get(ticker, "미분류")
        holdings.append({
            "ticker": ticker, "name": name,
            "shares": shares, "value_krw": value_krw,
            "weight": weight, "sector": sector,
        })
        if ticker and ticker not in sector_map:
            sector_map[ticker] = "미분류"

    if holdings:
        save_sector_map(sector_map)

    return build_koact_snapshot(holdings, date_str)


def build_koact_snapshot(holdings, date_str):
    if not holdings:
        return None
    snapshot = {
        "etf": "KoAct",
        "code": "0015B0",
        "date": date_str,
        "nav": None,
        "aum_billion": None,
        "holdings": holdings,
    }
    out_path = DATA / f"koact_{date_str}.json"
    out_path.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"[KoAct] 저장: {out_path} ({len(holdings)}종목)")
    unmapped = [h["ticker"] for h in holdings if h.get("sector") == "미분류"]
    if unmapped:
        print(f"[KoAct] 미분류 섹터: {', '.join(unmapped[:10])}")
    return snapshot


def crawl_koact(date_str):
    """KoAct 크롤링 (KRX 우선, samsungactive fallback)"""
    result = crawl_koact_krx(date_str)
    if result:
        return result
    result = crawl_koact_samsungactive(date_str)
    if result:
        return result
    print("[KoAct] 모든 크롤링 방법 실패. 수동 입력이 필요합니다.")
    return None


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
        crawl_koact(date_str)
        print()

    # 자동 빌드
    print("=== combined.json 빌드 ===")
    import subprocess
    subprocess.run([sys.executable, str(ROOT / "scripts" / "build.py")], check=True)

    print("\n완료! git push하면 대시보드에 반영됩니다.")

if __name__ == "__main__":
    main()
