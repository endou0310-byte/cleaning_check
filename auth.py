
# auth.py — ログイン/ユーザー管理（現状はJSONバックエンドのまま・最小変更）
from __future__ import annotations
import json, os
from typing import Dict, Any

from config import USERS_JSON

def _load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _dump_json(path: str, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def users_db() -> Dict[str, Any]:
    db = _load_json(USERS_JSON, {})
    if not db:
        db = {"admin": {"password": "admin", "role": "admin", "must_change": False}}
        _dump_json(USERS_JSON, db)
    return db

def save_users_db(db: Dict[str, Any]):
    _dump_json(USERS_JSON, db)
