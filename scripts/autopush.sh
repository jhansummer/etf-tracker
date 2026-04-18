#!/bin/bash
REPO=~/etf-tracker
LOG=~/Library/Logs/etf-autopush.log
cd "$REPO" || exit 1

# 오래된 락 파일 정리 (5분 이상)
for lock in .git/index.lock .git/HEAD.lock; do
    if [ -f "$lock" ]; then
        age=$(( $(date +%s) - $(stat -f %m "$lock") ))
        if [ "$age" -gt 300 ]; then rm -f "$lock"; fi
    fi
done

# 락 있으면 스킵
if [ -f .git/index.lock ] || [ -f .git/HEAD.lock ]; then
    echo "$(date): lock exists, skipping" >> "$LOG"
    exit 0
fi

# 미커밋 변경사항 (로그파일 제외) 있으면 커밋
if ! git diff --quiet 2>/dev/null || \
   ! git diff --staged --quiet 2>/dev/null || \
   [ -n "$(git ls-files --others --exclude-standard 2>/dev/null)" ]; then
    git add -A
    git -c user.email="jin.han226@gmail.com" -c user.name="hanjin" \
        commit -m "auto: $(TZ=Asia/Seoul date '+%Y-%m-%d %H:%M')" \
        >> "$LOG" 2>&1
fi

# 미push 커밋 있으면 rebase 후 push
if git log origin/main..HEAD --oneline 2>/dev/null | grep -q .; then
    git pull --rebase origin main >> "$LOG" 2>&1
    git push >> "$LOG" 2>&1
fi
