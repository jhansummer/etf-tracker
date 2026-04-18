#!/usr/bin/env python3
"""
ETF 포트폴리오 자동 크롤링
사용법:
  python scripts/crawl.py                         # 오늘 날짜로 전체 크롤링
  python scripts/crawl.py 2026-04-04              # 특정 날짜
  python scripts/crawl.py 2026-04-04 time         # TIME 미국나스닥100만
  python scripts/crawl.py 2026-04-04 koact        # KoAct만
  python scripts/crawl.py 2026-04-04 time_kosdaq  # TIME 코스닥액티브만
  python scripts/crawl.py 2026-04-04 kosdaq       # KoAct 코스닥만
  python scripts/crawl.py 2026-04-04 arkk         # ARK Innovation ETF만
  python scripts/crawl.py 2026-04-04 berkshire    # 버핏 13F
  python scripts/crawl.py 2026-04-04 13f          # 13F 전체 (4명)
"""
import sys, json, re, os, time as _time, csv, io
import xml.etree.ElementTree as ET
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

def fetch_url(url, data=None, extra_headers=None, retries=3, timeout=60):
    hdrs = dict(HEADERS)
    if extra_headers:
        hdrs.update(extra_headers)
    if data and isinstance(data, dict):
        data = urllib.parse.urlencode(data).encode("utf-8")
    last_err = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, data=data, headers=hdrs)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                wait = 5 * (attempt + 1)
                print(f"  fetch 실패 (attempt {attempt+1}/{retries}): {e} → {wait}s 후 재시도")
                _time.sleep(wait)
    raise last_err

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


def crawl_time(date_str, idx=2, etf_label="TIME", code="426030", file_prefix="time"):
    print(f"[{etf_label}] timeetf.co.kr 크롤링 중...")
    url = f"https://www.timeetf.co.kr/m11_view.php?idx={idx}"
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
        print(f"[{etf_label}] 종목 테이블을 찾지 못했습니다.")
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
        print(f"[{etf_label}] 종목 데이터를 파싱하지 못했습니다.")
        return None

    save_sector_map(sector_map)

    snapshot = {
        "etf": etf_label,
        "code": code,
        "date": date_str,
        "nav": nav,
        "aum_billion": aum,
        "holdings": holdings,
    }

    out_path = DATA / f"{file_prefix}_{date_str}.json"
    out_path.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"[{etf_label}] 저장: {out_path} ({len(holdings)}종목)")
    unmapped = [h["ticker"] for h in holdings if h["sector"] == "미분류"]
    if unmapped:
        print(f"[{etf_label}] 미분류 섹터: {', '.join(unmapped)}")
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
#  ARK Invest ETF — 일간 CSV (assets.ark-funds.com)
# ══════════════════════════════════════════════

ARK_ETFS = {
    "arkk": {
        "etf": "ARKK",
        "code": "ARKK",
        "label": "ARK Innovation",
        "file_prefix": "arkk",
        "url": "https://assets.ark-funds.com/fund-documents/funds-etf-csv/ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv",
    },
    "arkq": {
        "etf": "ARKQ",
        "code": "ARKQ",
        "label": "ARK Autonomous Tech",
        "file_prefix": "arkq",
        # ARK 실제 파일명 패턴: TECH.(약어+점), & 리터럴 사용
        "url": "https://assets.ark-funds.com/fund-documents/funds-etf-csv/ARK_AUTONOMOUS_TECH._&_ROBOTICS_ETF_ARKQ_HOLDINGS.csv",
        "url_fallbacks": [
            "https://assets.ark-funds.com/fund-documents/funds-etf-csv/ARK_AUTONOMOUS_TECHNOLOGY_&_ROBOTICS_ETF_ARKQ_HOLDINGS.csv",
            "https://assets.ark-funds.com/fund-documents/funds-etf-csv/ARK_AUTONOMOUS_TECHNOLOGY_AND_ROBOTICS_ETF_ARKQ_HOLDINGS.csv",
        ],
    },
    "arkw": {
        "etf": "ARKW",
        "code": "ARKW",
        "label": "ARK Next Gen Internet",
        "file_prefix": "arkw",
        "url": "https://assets.ark-funds.com/fund-documents/funds-etf-csv/ARK_NEXT_GENERATION_INTERNET_ETF_ARKW_HOLDINGS.csv",
    },
    "arkg": {
        "etf": "ARKG",
        "code": "ARKG",
        "label": "ARK Genomic Revolution",
        "file_prefix": "arkg",
        "url": "https://assets.ark-funds.com/fund-documents/funds-etf-csv/ARK_GENOMIC_REVOLUTION_ETF_ARKG_HOLDINGS.csv",
    },
}

# ── 13F 투자거장 ──
INVESTORS_13F = {
    "berkshire": {"cik": "0001067983", "label": "Berkshire (버핏)",       "file_prefix": "berkshire"},
    "pershing":  {"cik": "0001336528", "label": "Pershing Sq (애크먼)",   "file_prefix": "pershing"},
    "scion":     {"cik": "0001649339", "label": "Scion (버리)",           "file_prefix": "scion"},
    "duquesne":  {"cik": "0001536411", "label": "Duquesne (드러켄밀러)", "file_prefix": "duquesne"},
}

SEC_UA = {"User-Agent": "ETF-Tracker/1.0 jin.han226@gmail.com", "Accept-Encoding": "gzip, deflate"}

def _sec_get(url):
    return fetch_url(url, extra_headers=SEC_UA)

def get_latest_13f_info(cik_raw):
    """SEC EDGAR submissions에서 최신 13F-HR 정보 반환"""
    cik10 = cik_raw.lstrip("0").zfill(10)
    data = json.loads(_sec_get(f"https://data.sec.gov/submissions/CIK{cik10}.json").decode("utf-8"))
    recent = data.get("filings", {}).get("recent", {})
    forms   = recent.get("form", [])
    dates   = recent.get("filingDate", [])
    accs    = recent.get("accessionNumber", [])
    for i, f in enumerate(forms):
        if f == "13F-HR":
            return {"cik": cik_raw.lstrip("0"), "accession": accs[i], "filing_date": dates[i]}
    return None

def get_13f_xml_url(cik, accession):
    """13F 제출에서 information table XML URL 찾기"""
    acc_nd = accession.replace("-", "")
    # 인덱스 JSON 시도
    try:
        idx = json.loads(_sec_get(
            f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nd}/{accession}-index.json"
        ).decode("utf-8"))
        for doc in idx.get("documents", []):
            dtype = (doc.get("type") or "").upper()
            dname = (doc.get("document") or "").lower()
            if "INFORMATION TABLE" in dtype or "infotable" in dname or dname.endswith("infotable.xml"):
                return f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nd}/{doc['document']}"
    except Exception as e:
        print(f"  index.json 실패: {e}")
    # fallback 후보
    for name in ["infotable.xml", "form13fInfoTable.xml", "informationtable.xml",
                 f"{accession}-infotable.xml"]:
        url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nd}/{name}"
        try:
            _sec_get(url)
            return url
        except Exception:
            continue
    return None

def parse_13f_xml(xml_bytes):
    """13F information table XML → holdings 리스트"""
    text = xml_bytes.decode("utf-8", errors="replace")
    # 네임스페이스 제거 (ElementTree 단순화)
    text = re.sub(r'\sxmlns[^=]*="[^"]*"', "", text)
    try:
        root = ET.fromstring(text)
    except ET.ParseError as e:
        print(f"  XML 파싱 오류: {e}")
        return []

    holdings = []
    for info in root.iter("infoTable"):
        name      = (info.findtext("nameOfIssuer") or "").strip()
        cusip     = (info.findtext("cusip") or "").strip()
        val_str   = (info.findtext("value") or "0").strip().replace(",", "")
        sh_el     = info.find("shrsOrPrnAmt")
        sh_str    = (sh_el.findtext("sshPrnamt") if sh_el is not None else "0") or "0"
        sh_str    = sh_str.strip().replace(",", "")
        try:
            value  = float(val_str) * 1000   # 천달러 → 달러
            shares = int(float(sh_str))
        except (ValueError, TypeError):
            continue
        if name and value > 0:
            holdings.append({"name": name, "cusip": cusip, "ticker": "",
                              "shares": shares, "value_usd": value, "weight": 0.0, "sector": "미분류"})

    total = sum(h["value_usd"] for h in holdings)
    if total > 0:
        for h in holdings:
            h["weight"] = round(h["value_usd"] / total * 100, 4)
    holdings.sort(key=lambda x: x["weight"], reverse=True)
    return holdings

def lookup_cusip_tickers(cusips):
    """OpenFIGI API (무료, 무인증) CUSIP→ticker 일괄 조회"""
    ticker_map = {}
    batch_size = 10
    for i in range(0, len(cusips), batch_size):
        batch = cusips[i:i+batch_size]
        payload = json.dumps([{"idType": "ID_CUSIP", "idValue": c} for c in batch]).encode("utf-8")
        try:
            raw = fetch_url("https://api.openfigi.com/v3/mapping", data=payload,
                            extra_headers={"Content-Type": "application/json",
                                           "User-Agent": "ETF-Tracker/1.0"})
            results = json.loads(raw.decode("utf-8"))
            for j, res in enumerate(results):
                if "data" in res and res["data"]:
                    for item in res["data"]:
                        if item.get("exchCode") in ("US", "UN", "UQ", "UA", "UP"):
                            ticker_map[batch[j]] = item.get("ticker", "")
                            break
                    if batch[j] not in ticker_map:
                        ticker_map[batch[j]] = res["data"][0].get("ticker", "")
        except Exception as e:
            print(f"  OpenFIGI 배치 오류: {e}")
        _time.sleep(0.6)
    return ticker_map

def crawl_13f(date_str, key):
    """SEC EDGAR 13F 포트폴리오 크롤링"""
    meta  = INVESTORS_13F[key]
    label = meta["label"]
    print(f"[{label}] 13F 크롤링 중...")

    filing = get_latest_13f_info(meta["cik"])
    if not filing:
        print(f"[{label}] 13F 제출 없음"); return None

    filing_date = filing["filing_date"]
    cik         = filing["cik"]
    accession   = filing["accession"]
    print(f"[{label}] 최신 13F: {filing_date}  acc={accession}")

    out_file = DATA / f"{meta['file_prefix']}_{filing_date}.json"
    if out_file.exists():
        print(f"[{label}] 이미 있음, 스킵"); return True

    xml_url = get_13f_xml_url(cik, accession)
    if not xml_url:
        print(f"[{label}] XML URL 찾기 실패"); return None
    print(f"[{label}] XML: {xml_url}")

    try:
        xml_bytes = _sec_get(xml_url)
    except Exception as e:
        print(f"[{label}] XML 다운로드 실패: {e}"); return None

    holdings = parse_13f_xml(xml_bytes)
    if not holdings:
        print(f"[{label}] holdings 파싱 실패"); return None
    print(f"[{label}] {len(holdings)}개 포지션 파싱")

    # CUSIP→ticker
    cusips = [h["cusip"] for h in holdings if h.get("cusip")]
    print(f"[{label}] ticker 조회 중 ({len(cusips)}개)...")
    tmap = lookup_cusip_tickers(cusips)
    sector_map = load_sector_map()
    for h in holdings:
        h["ticker"] = tmap.get(h.get("cusip", ""), "")
        t = h["ticker"]
        h["sector"] = sector_map.get(t, "미분류") if t else "미분류"

    payload = {"investor": key, "label": label, "filing_date": filing_date, "holdings": holdings}
    out_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[{label}] 저장: {out_file.name}")
    return True


def crawl_ark(date_str, key="arkk"):
    meta = ARK_ETFS[key]
    label = meta["label"]
    print(f"[{label}] ARK Holdings CSV 크롤링 중...")

    urls_to_try = [meta["url"]] + meta.get("url_fallbacks", [])
    text = None
    for url in urls_to_try:
        try:
            raw = fetch_url(url, extra_headers={"Accept": "text/csv,*/*"})
            text = raw.decode("utf-8", errors="replace")
            if text and "ticker" in text.lower():
                print(f"[{label}] URL 성공: {url}")
                break
            else:
                print(f"[{label}] 응답 이상 (CSV 아님), 다음 URL 시도...")
                text = None
        except Exception as e:
            print(f"[{label}] URL 실패 ({url}): {e}")
            text = None

    if text is None:
        print(f"[{label}] 모든 URL 실패")
        return None

    # CSV 파싱
    reader = csv.DictReader(io.StringIO(text))
    sector_map = load_sector_map()
    holdings = []
    csv_date = None

    def _s(v):
        """None-safe strip"""
        return (v or "").strip()

    for row in reader:
        # 열 이름 정규화 (대소문자 무관, None-safe)
        row_lower = {k.strip().lower(): v for k, v in row.items() if k and k.strip()}
        ticker = _s(row_lower.get("ticker"))
        name   = _s(row_lower.get("company"))
        shares_str = _s(row_lower.get("shares"))
        weight_str = _s(row_lower.get("weight (%)")) or _s(row_lower.get("weight"))
        value_str  = _s(row_lower.get("market value ($)")) or _s(row_lower.get("market value"))
        date_val   = _s(row_lower.get("date"))

        if not ticker or ticker in ("-", "USD"):
            continue
        if "CASH" in ticker.upper() or not name:
            continue

        # CSV 날짜 파싱 (MM/DD/YYYY)
        if date_val and not csv_date:
            try:
                csv_date = datetime.strptime(date_val.strip(), "%m/%d/%Y").strftime("%Y-%m-%d")
            except Exception:
                pass

        shares    = parse_number_str(shares_str)
        weight    = parse_float_str(weight_str)
        value_usd = parse_float_str(value_str)

        if weight is None or weight == 0:
            continue

        sector = sector_map.get(ticker, "미분류")
        holdings.append({
            "ticker":    ticker,
            "name":      name,
            "shares":    shares,
            "value_krw": None,        # USD 펀드 — KRW 없음
            "value_usd": value_usd,
            "weight":    weight,
            "sector":    sector,
        })
        if ticker not in sector_map:
            sector_map[ticker] = "미분류"

    if not holdings:
        print(f"[{label}] 종목 데이터를 파싱하지 못했습니다.")
        return None

    effective_date = csv_date or date_str
    save_sector_map(sector_map)

    snapshot = {
        "etf":          meta["etf"],
        "code":         meta["code"],
        "date":         effective_date,
        "nav":          None,
        "aum_billion":  None,
        "holdings":     holdings,
    }

    out_path = DATA / f"{meta['file_prefix']}_{effective_date}.json"
    out_path.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"[{label}] 저장: {out_path} ({len(holdings)}종목, 날짜:{effective_date})")
    unmapped = [h["ticker"] for h in holdings if h["sector"] == "미분류"]
    if unmapped:
        print(f"[{label}] 미분류 섹터: {', '.join(unmapped[:20])}{'...' if len(unmapped) > 20 else ''}")
    return snapshot


# ══════════════════════════════════════════════
#  메인
# ══════════════════════════════════════════════

def main():
    today = date.today().isoformat()
    date_str = sys.argv[1] if len(sys.argv) > 1 else today
    target = sys.argv[2].lower() if len(sys.argv) > 2 else "all"

    print(f"=== ETF 크롤링 시작 ({date_str}) ===\n")

    results = {}

    if target in ("all", "time"):
        try:
            results["time"] = crawl_time(date_str, idx=2, etf_label="TIME", code="426030", file_prefix="time") is not None
        except Exception as e:
            print(f"[TIME] 크롤링 실패: {e}")
            results["time"] = False
        print()

    if target in ("all", "time_kosdaq"):
        try:
            results["time_kosdaq"] = crawl_time(date_str, idx=24, etf_label="TIME 코스닥", code="0162Y0", file_prefix="time_kosdaq") is not None
        except Exception as e:
            print(f"[TIME 코스닥] 크롤링 실패: {e}")
            results["time_kosdaq"] = False
        print()

    if target in ("all", "koact"):
        try:
            results["koact"] = crawl_samsung(date_str, "koact") is not None
        except Exception as e:
            print(f"[KoAct] 크롤링 실패: {e}")
            results["koact"] = False
        print()

    if target in ("all", "kosdaq"):
        try:
            results["kosdaq"] = crawl_samsung(date_str, "kosdaq") is not None
        except Exception as e:
            print(f"[코스닥] 크롤링 실패: {e}")
            results["kosdaq"] = False
        print()

    for ark_key in ("arkk", "arkq", "arkw", "arkg"):
        if target in ("all", ark_key):
            try:
                results[ark_key] = crawl_ark(date_str, ark_key) is not None
            except Exception as e:
                print(f"[{ark_key.upper()}] 크롤링 실패: {e}")
                results[ark_key] = False
            print()

    for inv_key in ("berkshire", "pershing", "scion", "duquesne"):
        if target in ("all", "13f", inv_key):
            try:
                results[inv_key] = crawl_13f(date_str, inv_key) is not None
            except Exception as e:
                print(f"[{inv_key}] 13F 크롤링 실패: {e}")
                results[inv_key] = False
            print()

    # 하나라도 성공하면 빌드
    if any(results.values()):
        print("=== combined.json 빌드 ===")
        import subprocess
        subprocess.run([sys.executable, str(ROOT / "scripts" / "build.py")], check=True)
        print("\n완료! git push하면 대시보드에 반영됩니다.")
    else:
        print("모든 크롤링 실패. 빌드 건너뜀.")
        sys.exit(1)

    # 일부만 실패해도 종료 코드는 0 (workflow는 계속 진행)
    failed = [k for k, v in results.items() if not v]
    if failed:
        print(f"\n경고: {', '.join(failed)} 크롤링 실패 (다른 ETF는 정상)")

if __name__ == "__main__":
    main()
