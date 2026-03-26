#!/usr/bin/env python3
"""
ETF 포트폴리오 데이터 임포트
사용법:
  python scripts/import.py time 2026-03-28          # data/raw/ 에서 자동 탐색
  python scripts/import.py time 2026-03-28 file.xlsx  # 파일 지정
  python scripts/import.py koact 2026-03-28           # KoAct도 동일
"""
import sys, json, csv, os, re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
RAW = DATA / "raw"
SECTOR_MAP_PATH = DATA / "sector_map.json"

ETF_META = {
    "time": {"etf": "TIME", "code": "426030"},
    "koact": {"etf": "KoAct", "code": "0015B0"},
}

def load_sector_map():
    if SECTOR_MAP_PATH.exists():
        return json.loads(SECTOR_MAP_PATH.read_text(encoding="utf-8"))
    return {}

def save_sector_map(smap):
    SECTOR_MAP_PATH.write_text(
        json.dumps(smap, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

def find_raw_file(etf_key, date_str):
    """data/raw/ 에서 etf_key와 date_str이 포함된 파일 탐색"""
    patterns = [
        f"{etf_key}*{date_str}*",
        f"*{etf_key}*{date_str}*",
        f"*{date_str}*{etf_key}*",
    ]
    for pat in patterns:
        matches = list(RAW.glob(pat))
        if matches:
            return matches[0]
    # 날짜만으로 탐색
    for f in sorted(RAW.iterdir()):
        if date_str in f.name and etf_key in f.name.lower():
            return f
    return None

def read_csv_rows(filepath):
    """CSV/TSV 파일 읽기"""
    with open(filepath, encoding="utf-8-sig") as f:
        # 탭 또는 콤마 구분 자동 감지
        sample = f.read(4096)
        f.seek(0)
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t")
        reader = csv.DictReader(f, dialect=dialect)
        return list(reader)

def read_excel_rows(filepath):
    """엑셀 파일 읽기 (openpyxl 필요)"""
    try:
        import openpyxl
    except ImportError:
        print("openpyxl 필요: pip install openpyxl")
        sys.exit(1)
    wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return []
    headers = [str(h).strip() if h else f"col{i}" for i, h in enumerate(rows[0])]
    return [dict(zip(headers, row)) for row in rows[1:]]

def normalize_column(row):
    """다양한 컬럼명을 통일"""
    mapping = {}
    for k, v in row.items():
        kl = k.strip().lower().replace(" ", "")
        if kl in ("종목코드", "티커", "ticker", "code", "종목코드(ticker)"):
            mapping["ticker"] = str(v).strip() if v else ""
        elif kl in ("종목명", "name", "종목", "종목명(한글)"):
            mapping["name"] = str(v).strip() if v else ""
        elif kl in ("수량", "shares", "보유수량"):
            mapping["shares"] = parse_number(v)
        elif kl in ("평가금액", "평가금액(원)", "value", "value_krw", "평가액"):
            mapping["value_krw"] = parse_number(v)
        elif kl in ("비중", "비중(%)", "weight", "비중(%)"):
            mapping["weight"] = parse_float(v)
    return mapping

def parse_number(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return int(v)
    s = re.sub(r"[,\s]", "", str(v))
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None

def parse_float(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return round(float(v), 2)
    s = re.sub(r"[,%\s]", "", str(v))
    try:
        return round(float(s), 2)
    except (ValueError, TypeError):
        return None

def import_etf(etf_key, date_str, filepath=None):
    meta = ETF_META.get(etf_key)
    if not meta:
        print(f"알 수 없는 ETF: {etf_key} (time 또는 koact)")
        sys.exit(1)

    if filepath:
        fpath = Path(filepath)
    else:
        fpath = find_raw_file(etf_key, date_str)
        if not fpath:
            print(f"data/raw/ 에서 {etf_key} {date_str} 파일을 찾을 수 없습니다.")
            print(f"사용법: python scripts/import.py {etf_key} {date_str} [파일경로]")
            sys.exit(1)

    print(f"파일 읽는 중: {fpath}")

    ext = fpath.suffix.lower()
    if ext in (".xlsx", ".xls"):
        rows = read_excel_rows(fpath)
    else:
        rows = read_csv_rows(fpath)

    sector_map = load_sector_map()
    holdings = []
    for row in rows:
        norm = normalize_column(row)
        ticker = norm.get("ticker", "")
        if not ticker:
            continue
        sector = sector_map.get(ticker, "미분류")
        holdings.append({
            "ticker": ticker,
            "name": norm.get("name", ""),
            "shares": norm.get("shares"),
            "value_krw": norm.get("value_krw"),
            "weight": norm.get("weight"),
            "sector": sector,
        })
        # 섹터맵에 없으면 추가
        if ticker not in sector_map:
            sector_map[ticker] = "미분류"

    snapshot = {
        "etf": meta["etf"],
        "code": meta["code"],
        "date": date_str,
        "nav": None,
        "aum_billion": None,
        "holdings": holdings,
    }

    out_path = DATA / f"{etf_key}_{date_str}.json"
    out_path.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    save_sector_map(sector_map)

    print(f"저장 완료: {out_path}")
    print(f"종목 수: {len(holdings)}")
    unmapped = [h["ticker"] for h in holdings if h["sector"] == "미분류"]
    if unmapped:
        print(f"미분류 섹터: {', '.join(unmapped)}")
        print(f"→ data/sector_map.json 에서 섹터를 지정해주세요.")

def main():
    if len(sys.argv) < 3:
        print("사용법: python scripts/import.py <time|koact> <YYYY-MM-DD> [파일경로]")
        sys.exit(1)
    etf_key = sys.argv[1].lower()
    date_str = sys.argv[2]
    filepath = sys.argv[3] if len(sys.argv) > 3 else None
    import_etf(etf_key, date_str, filepath)

if __name__ == "__main__":
    main()
