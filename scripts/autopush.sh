#!/bin/bash
REPO=~/etf-tracker
cd "$REPO" || exit 1

# 오래된 락 파일 정리 (5분 이상 된 것만)
for lock in .git/index.lock .git/HEAD.lock .git/COMMIT_EDITMSG.lock; do
    if [ -f "$lock" ]; then
        age=$(( $(date +%s) - $(stat -f %m "$lock") ))
        if [ "$age" -gt 300 ]; then
            rm -f "$lock"
        fi
    fi
done

# 락 있으면 이번 라운드 스킵
if [ -f .git/index.lock ] || [ -f .git/HEAD.lock ]; then
    echo "$(date): lock exists, skipping" >> "$REPO/scripts/autopush.log"
    exit 0
fi

# 미커밋 변경사항 있으면 자동 커밋
if ! git diff --quiet 2>/dev/null || \
   ! git diff --staged --quiet 2>/dev/null || \
   [ -n "$(git ls-files --others --exclude-standard 2>/dev/null)" ]; then
    git add -A
    git -c user.email="jin.han226@gmail.com" -c user.name="hanjin" \
        commit -m "auto: $(TZ=Asia/Seoul date '+%Y-%m-%d %H:%M')" \
        >> "$REPO/scripts/autopush.log" 2>&1
fi

# 미push 커밋 있으면 push
if git log origin/main..HEAD --oneline 2>/dev/null | grep -q .; then
    git push >> "$REPO/scripts/autopush.log" 2>&1
fi
