# db.py — SQLite (usage_monthly + jobs / job_results + KPI集計)
from __future__ import annotations
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Tuple

from config import USAGE_DB_PATH

@contextmanager
def _conn():
    con = sqlite3.connect(USAGE_DB_PATH, timeout=30, isolation_level=None)
    try:
        yield con
    finally:
        con.close()

def init_usage_db():
    with _conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS usage_monthly (
              tenant TEXT NOT NULL,
              property TEXT NOT NULL,
              ym TEXT NOT NULL,
              images_used INTEGER NOT NULL DEFAULT 0,
              runs_used   INTEGER NOT NULL DEFAULT 0,
              updated_at  TEXT NOT NULL,
              PRIMARY KEY (tenant, property, ym)
            )
        """)

def month_key(dt: datetime | None = None) -> str:
    dt = dt or datetime.now()
    return f"{dt.year:04d}{dt.month:02d}"

def get_monthly_usage(tenant: str, prop: str, ym: str) -> Tuple[int, int]:
    """(images_used, runs_used)"""
    init_usage_db()
    with _conn() as con:
        cur = con.execute(
            "SELECT images_used, runs_used FROM usage_monthly WHERE tenant=? AND property=? AND ym=?",
            (tenant, prop, ym)
        )
        row = cur.fetchone()
        if row:
            return int(row[0]), int(row[1])
        now = datetime.now().isoformat(timespec="seconds")
        con.execute(
            "INSERT OR IGNORE INTO usage_monthly(tenant, property, ym, images_used, runs_used, updated_at) VALUES (?,?,?,?,?,?)",
            (tenant, prop, ym, 0, 0, now)
        )
        return 0, 0

def add_monthly_usage(tenant: str, prop: str, ym: str, add_images: int, add_runs: int):
    init_usage_db()
    now = datetime.now().isoformat(timespec="seconds")
    with _conn() as con:
        con.execute("""
            INSERT INTO usage_monthly(tenant, property, ym, images_used, runs_used, updated_at)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(tenant, property, ym) DO UPDATE SET
              images_used = images_used + excluded.images_used,
              runs_used   = runs_used   + excluded.runs_used,
              updated_at  = excluded.updated_at
        """, (tenant, prop, ym, max(0, add_images), max(0, add_runs), now))

# === KPI用のテーブル（jobs / job_results） ===
def init_reporting_db():
    init_usage_db()
    with _conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
              job_id   TEXT PRIMARY KEY,
              tenant   TEXT NOT NULL,
              property TEXT NOT NULL,
              ym       TEXT NOT NULL,
              ts_start TEXT NOT NULL,
              ts_end   TEXT NOT NULL,
              images   INTEGER NOT NULL,
              ok       INTEGER NOT NULL,
              ng       INTEGER NOT NULL,
              unknown  INTEGER NOT NULL
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS job_results (
              id       INTEGER PRIMARY KEY AUTOINCREMENT,
              job_id   TEXT NOT NULL,
              idx      INTEGER NOT NULL,
              file     TEXT,
              verdict  TEXT,
              stage    TEXT,
              FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
            )
        """)

def write_job(tenant: str, prop: str, job_id: str, ym: str,
              ts_start: str, ts_end: str, images: int, ok: int, ng: int, unknown: int):
    init_reporting_db()
    with _conn() as con:
        con.execute("""
            INSERT OR REPLACE INTO jobs(job_id,tenant,property,ym,ts_start,ts_end,images,ok,ng,unknown)
            VALUES(?,?,?,?,?,?,?,?,?,?)
        """, (job_id, tenant, prop, ym, ts_start, ts_end, int(images), int(ok), int(ng), int(unknown)))

def write_job_results(job_id: str, results: list[dict]):
    if not results:
        return
    init_reporting_db()
    rows = []
    for r in results:
        rows.append((
            job_id,
            int(r.get("index", 0)),
            str(r.get("file","")),
            str(r.get("verdict","")),
            str(r.get("stage","")),
        ))
    with _conn() as con:
        con.executemany("""
            INSERT INTO job_results(job_id, idx, file, verdict, stage)
            VALUES(?,?,?,?,?)
        """, rows)

def query_monthly_kpi(tenant: str, ym: str) -> list[dict]:
    """物件別KPI: property, jobs, images, ok, ng, unknown, ng_rate"""
    init_reporting_db()
    with _conn() as con:
        cur = con.execute("""
            SELECT property,
                   COUNT(*) AS jobs,
                   SUM(images) AS images,
                   SUM(ok) AS ok,
                   SUM(ng) AS ng,
                   SUM(unknown) AS unknown
              FROM jobs
             WHERE tenant=? AND ym=?
          GROUP BY property
          ORDER BY property
        """, (tenant, ym))
        out = []
        for p, jobs, images, ok, ng, unknown in cur.fetchall():
            total = (ok or 0) + (ng or 0) + (unknown or 0)
            ng_rate = (float(ng or 0) / total) if total else 0.0
            out.append({
                "property": p,
                "jobs": int(jobs or 0),
                "images": int(images or 0),
                "ok": int(ok or 0),
                "ng": int(ng or 0),
                "unknown": int(unknown or 0),
                "ng_rate": round(ng_rate, 4),
            })
        return out

def query_monthly_jobs_detail(tenant: str, ym: str, prop: str | None = None) -> list[dict]:
    """ジョブ単位の明細（請求証跡向け）"""
    init_reporting_db()
    cond = "WHERE tenant=? AND ym=?"
    params = [tenant, ym]
    if prop:
        cond += " AND property=?"
        params.append(prop)
    sql = f"""
        SELECT job_id, property, ts_start, ts_end, images, ok, ng, unknown
          FROM jobs
         {cond}
      ORDER BY ts_start
    """
    with _conn() as con:
        cur = con.execute(sql, tuple(params))
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
