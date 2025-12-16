# chmod +x refresh.sh
#!/bin/bash

echo "🚀 Starting Airflow environment update..."

# 1. 拉取最新程式碼
git pull origin main

# 2. 檢查是否有 Dockerfile 或 requirements.txt 的變更
# 這裡用簡單的邏輯：如果 git pull 有更新到這兩個檔案，就重建
if git diff --name-only HEAD@{1} HEAD | grep -E "Dockerfile|requirements.txt"; then
    echo "📦 Environment changes detected. Rebuilding Docker image..."
    docker-compose up -d --build
else
    echo "✅ Code changes only. No Docker restart required."
fi

echo "🎉 Update complete!"