
# config.py — パス定義と共通定数（5ファイル版）
from __future__ import annotations
import os

APP_TITLE = "🧹 ハウスクリーニング完了画像チェック"

# ルート
ROOT_DIR  = "storage"
SAVE_ROOT = os.path.join(ROOT_DIR, "jobs")
AUTH_DIR  = os.path.join(ROOT_DIR, "auth")
PROPS_DIR = os.path.join(ROOT_DIR, "props")
LOG_ROOT  = os.path.join(ROOT_DIR, "logs")

# 設定ファイル
GLOBAL_OKWL   = os.path.join(ROOT_DIR, "global_ok_whitelist.json")
GLOBAL_RECHECKWL = os.path.join(ROOT_DIR, "global_recheck_whitelist.json")
NOTICE_MD     = os.path.join(ROOT_DIR, "notice.md")
CONFIG_JSON   = os.path.join(ROOT_DIR, "config.json")  # {"openai_api_key":"sk-..."}
USERS_JSON    = os.path.join(AUTH_DIR, "users.json")

# DB
DATA_DIR      = os.path.join(ROOT_DIR, "data")
USAGE_DB_PATH = os.path.join(DATA_DIR, "usage.db")

# 初期ディレクトリ作成
for d in (SAVE_ROOT, AUTH_DIR, PROPS_DIR, LOG_ROOT, DATA_DIR, os.path.dirname(GLOBAL_OKWL), os.path.dirname(NOTICE_MD)):
    os.makedirs(d, exist_ok=True)
