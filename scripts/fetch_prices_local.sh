#!/bin/bash
# 매일 미국장 마감 후 가격 데이터 로컬 fetch → git push
# crontab: 0 7 * * 2-6 /Users/hanjin/Desktop/etf-tracker/scripts/fetch_prices_local.sh

set -e

REPO_DIR="/Users/hanjin/etf-tracker"
LOG="$REPO_DIR/scripts/fetch_prices.log"
DATE=$(TZ=Asia/Seoul date +%F)
PYTHON="/opt/homebrew/bin/python3"

echo "[$DATE $(TZ=Asia/Seoul date +%T)] 가격 fetch 시작" >> "$LOG"

cd "$REPO_DIR"

# us_price 크롤링
$PYTHON scripts/crawl.py "$DATE" us_price >> "$LOG" 2>&1

# build
$PYTHON scripts/build.py >> "$LOG" 2>&1

# git push
git add -A
if git diff --staged --quiet; then
  echo "[$DATE] 변경 없음 (장 휴장?)" >> "$LOG"
else
  git commit -m "daily prices $DATE"
  git pull --rebase origin main >> "$LOG" 2>&1 || true
  git push >> "$LOG" 2>&1
  echo "[$DATE] push 완료" >> "$LOG"
fi
