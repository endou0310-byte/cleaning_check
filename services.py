# services.py — 解析実行・ログ保存・使用量加算 + ★SQLへKPI記録
from __future__ import annotations
import os, json
from datetime import datetime
from typing import Any, Dict, List, Tuple

from config import LOG_ROOT
from db import add_monthly_usage, month_key, write_job, write_job_results
from chatgpt_cleaning_check import analyze_headless

def save_run_log(uid: str, prop: str, job_id: str, summary: Dict[str,Any], images: int):
    yymm = datetime.now().strftime("%Y-%m")
    path = os.path.join(LOG_ROOT, uid, yymm, prop)
    os.makedirs(path, exist_ok=True)
    rec = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "user": uid, "prop": prop, "job_id": job_id,
        "images": int(images),
        "ok": int((summary or {}).get("ok",0)),
        "ng": int((summary or {}).get("ng",0)),
        "unknown": int((summary or {}).get("unknown",0)),
        "stage_counts": (summary or {}).get("counts_by_stage", {}),
        "presence_evidence": {k: len(v) for k,v in ((summary or {}).get("presence_evidence") or {}).items()},
    }
    with open(os.path.join(path, f"{job_id}.json"), "w", encoding="utf-8") as f:
        json.dump(rec, f, ensure_ascii=False, indent=2)

def run_analysis_and_record(
    uid: str,
    prop_name: str,
    image_blobs: List[Tuple[str, bytes]],
    openai_key: str,
    thresholds: Dict[str, Any],
    defaults: Dict[str, Any],
):
    """analyze → usage加算 → JSON軽量ログ保存 → ★SQLへKPI記録"""
    ts_start = datetime.now().isoformat(timespec="seconds")

    summary, results, base_dir, job_id = analyze_headless(
        files=image_blobs,
        property_name=prop_name,
        api_key=openai_key,
        thresholds=thresholds,
        defaults=defaults,
    )

    ts_end = datetime.now().isoformat(timespec="seconds")
    ym = month_key()

    # 1) 月次使用量（DB加算）
    add_monthly_usage(uid, prop_name, ym, add_images=len(image_blobs), add_runs=1)

    # 2) 軽量ログ（JSON、互換維持）
    save_run_log(uid, prop_name, job_id, summary, images=len(image_blobs))

    # 3) ★SQLへKPI記録（jobs / job_results）
    ok = int((summary or {}).get("ok", 0))
    ng = int((summary or {}).get("ng", 0))
    unknown = int((summary or {}).get("unknown", 0))
    write_job(uid, prop_name, job_id, ym, ts_start, ts_end, len(image_blobs), ok, ng, unknown)
    write_job_results(job_id, results)

    return summary, results, base_dir, job_id

