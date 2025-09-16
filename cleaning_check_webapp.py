# cleaning_check_webapp.py — 5ファイル版 UI（SQL KPI対応）
from __future__ import annotations
import os, io, json, shutil, zipfile
from datetime import datetime
from typing import Any, Dict, List, Tuple

import streamlit as st

from config import (
    APP_TITLE, ROOT_DIR, SAVE_ROOT, AUTH_DIR, PROPS_DIR, LOG_ROOT,
    GLOBAL_OKWL, GLOBAL_RECHECKWL, NOTICE_MD, CONFIG_JSON
)
from auth import users_db, save_users_db
from db import get_monthly_usage, month_key, query_monthly_kpi, query_monthly_jobs_detail
from services import run_analysis_and_record
from chatgpt_cleaning_check import export_csv, export_json, export_zip_ng, export_zip_ok, render_image_grid

# === ユーティリティ ===
def _load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _dump_json(path: str, data: Any):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _prop_conf_path(uid: str, prop: str) -> str:
    return os.path.join(PROPS_DIR, uid, f"{prop}.json")

def _load_prop(uid: str, prop: str) -> Dict[str, Any]:
    return _load_json(_prop_conf_path(uid, prop), {
        "has_tv": True,
        "has_heater_panel": True,
        "conf_th": 0.80,
        "speed_mode": True,
        "quota": {"images": 3000, "runs": 20},
    })

def _save_prop(uid: str, prop: str, cfg: Dict[str, Any]):
    _dump_json(_prop_conf_path(uid, prop), cfg)

def _load_text(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""

def _save_text(path: str, text: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def _load_global_whitelist() -> List[str]:
    return _load_json(GLOBAL_OKWL, [])

def _save_global_whitelist(items: List[str]):
    _dump_json(GLOBAL_OKWL, items)

def _load_global_recheck() -> List[str]:
    return _load_json(GLOBAL_RECHECKWL, [])

def _save_global_recheck(items: List[str]):
    _dump_json(GLOBAL_RECHECKWL, items)

def _load_api_key_from_config() -> str:
    cfg = _load_json(CONFIG_JSON, {})
    return (cfg or {}).get("openai_api_key", "").strip()

def _jobs_dir(uid: str, prop: str) -> str:
    d = os.path.join(SAVE_ROOT, uid, prop)
    os.makedirs(d, exist_ok=True)
    return d

def _check_free_space_or_stop(bytes_needed: int, root_dir: str = SAVE_ROOT):
    try:
        os.makedirs(root_dir, exist_ok=True)
        free = shutil.disk_usage(root_dir).free
    except Exception:
        free = shutil.disk_usage("/")
    buffer = 300 * 1024 * 1024
    if free < bytes_needed + buffer:
        st.error("空き容量が不足しています。古いジョブを削除してから再実行してください。")
        st.stop()

# ========== UI開始 ==========
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)

# 初回お知らせファイル
if not os.path.exists(NOTICE_MD):
    _save_text(NOTICE_MD, "# お知らせ\n\n- 初期メッセージです。ここにアップデートやメンテ告知をどうぞ。")

# ログイン
with st.sidebar:
    st.subheader("ログイン")
    if "auth" not in st.session_state:
        st.session_state.auth = {"uid": None, "role": None, "logged_in": False, "must_change": False}
    if not st.session_state.auth["logged_in"]:
        uid_input = st.text_input("ユーザーID", value=st.session_state.auth.get("uid") or "")
        pw_input  = st.text_input("パスワード", type="password")
        c1, c2 = st.columns(2)
        if c1.button("ログイン"):
            db = users_db()
            if uid_input in db and db[uid_input]["password"] == pw_input:
                must_change = bool(db[uid_input].get("must_change")) or (db[uid_input]["password"] == "password")
                st.session_state.auth = {
                    "uid": uid_input,
                    "role": db[uid_input].get("role","user"),
                    "logged_in": True,
                    "must_change": must_change,
                }
                st.rerun()
            else:
                st.error("ユーザーID または パスワードが違います。")
        if c2.button("ログアウト", disabled=True):
            pass
    else:
        st.success(f"ログイン中: {st.session_state.auth['uid']}")
        with st.expander("🔐 パスワード変更", expanded=False):
            new1 = st.text_input("新しいパスワード", type="password", key="pw1_side")
            new2 = st.text_input("新しいパスワード（確認）", type="password", key="pw2_side")
            if st.button("変更を保存", key="pw_change_side"):
                if not new1 or len(new1) < 8:
                    st.warning("パスワードは8文字以上にしてください。")
                elif new1 != new2:
                    st.warning("確認用と一致しません。")
                else:
                    db = users_db()
                    uid = st.session_state.auth["uid"]
                    if uid in db:
                        db[uid]["password"] = new1
                        db[uid]["must_change"] = False
                        save_users_db(db)
                        st.session_state.auth["must_change"] = False
                        st.success("パスワードを更新しました。")
        if st.button("ログアウト"):
            st.session_state.auth = {"uid": None, "role": None, "logged_in": False, "must_change": False}
            st.rerun()

auth = st.session_state.auth
if not auth["logged_in"]:
    st.info("左のサイドバーからログインしてください。")
    with st.container(border=True):
        st.markdown("### 📢 お知らせ（ログイン前でも閲覧可）")
        st.markdown(_load_text(NOTICE_MD))
    st.stop()

# 初回強制パスワード変更
if auth.get("must_change"):
    st.warning("初回ログインのため、パスワードの再設定が必要です。")
    new1 = st.text_input("新しいパスワード", type="password", key="pw1_force")
    new2 = st.text_input("新しいパスワード（確認）", type="password", key="pw2_force")
    if st.button("変更を保存して続行", key="pw_change_force"):
        if not new1 or len(new1) < 8:
            st.error("パスワードは8文字以上にしてください。")
        elif new1 != new2:
            st.error("確認用と一致しません。")
        else:
            db = users_db()
            uid = auth["uid"]
            if uid in db:
                db[uid]["password"] = new1
                db[uid]["must_change"] = False
                save_users_db(db)
                st.session_state.auth["must_change"] = False
                st.success("パスワードを更新しました。")
                st.rerun()
    st.stop()

current_user = auth["uid"]
current_role = auth["role"]

# 物件プルダウン
with st.sidebar:
    st.subheader("物件")
    user_prop_dir = os.path.join(PROPS_DIR, current_user)
    os.makedirs(user_prop_dir, exist_ok=True)
    prop_files = [f[:-5] for f in os.listdir(user_prop_dir) if f.endswith(".json")]
    prop_options = ["— 物件を選択 —"] + sorted(prop_files)
    prop_name = st.selectbox("物件を選択", options=prop_options, index=0)
    st.session_state.prop_name = prop_name

    st.subheader("物件仕様")
    if prop_name != "— 物件を選択 —":
        cfg = _load_prop(current_user, prop_name)
        has_tv     = st.checkbox("TV あり", value=cfg.get("has_tv", True))
        has_heater = st.checkbox("給湯パネルあり", value=cfg.get("has_heater_panel", True))

        is_admin = (current_role == "admin")
        if is_admin:
            conf_th    = st.slider("NG判定しきい値 (conf_th)", 0.30, 0.95, float(cfg.get("conf_th", 0.80)), 0.01)
            speed_mode = st.checkbox("スピードモード（nano のみ・明確NGのみ）", value=cfg.get("speed_mode", True))
        else:
            conf_th    = float(cfg.get("conf_th", 0.80))
            speed_mode = bool(cfg.get("speed_mode", True))

        # 利用状況（SQLiteから取得）
        ym = month_key()
        images_used, runs_used = get_monthly_usage(current_user, prop_name, ym)
        quota = cfg.get("quota", {"images":3000, "runs":20})
        qi, qr = int(quota.get("images",3000)), int(quota.get("runs",20))
        st.markdown("#### 今月の利用状況（この物件）")
        st.progress(min(images_used/qi,1.0), text=f"画像 {images_used} / {qi}")
        st.progress(min(runs_used/qr,1.0), text=f"実行 {runs_used} / {qr}")

        if st.button("物件設定を保存"):
            cfg["has_tv"] = has_tv
            cfg["has_heater_panel"] = has_heater
            if is_admin:
                cfg["conf_th"] = float(conf_th)
                cfg["speed_mode"] = bool(speed_mode)
            _save_prop(current_user, prop_name, cfg)
            st.success("保存しました。")
    else:
        st.info("物件が未選択です。先に右の管理者コンソールで物件を作成するか、既存物件を選択してください。")

# アップロード
st.subheader("画像の入力（先にアップロード → [チェック開始]）")
uploaded_files = st.file_uploader(
    "画像またはZIPをアップロード（複数可）",
    accept_multiple_files=True,
    type=["jpg", "jpeg", "png", "webp", "heic", "zip"]
)

def _is_image_filename(name: str) -> bool:
    n = (name or "").lower()
    return n.endswith((".jpg", ".jpeg", ".png", ".webp", ".heic"))

image_blobs_preview: List[Tuple[str, bytes]] = []
total_bytes = 0
if uploaded_files:
    for fu in uploaded_files:
        name = fu.name or "uploaded"
        if name.lower().endswith(".zip"):
            try:
                z = zipfile.ZipFile(io.BytesIO(fu.getvalue()))
                for info in z.infolist():
                    if info.is_dir(): continue
                    if not _is_image_filename(info.filename): continue
                    data = z.read(info)
                    total_bytes += len(data)
                    image_blobs_preview.append((os.path.basename(info.filename), data))
            except zipfile.BadZipFile:
                st.warning(f"ZIPの展開に失敗: {name}")
        else:
            data = fu.getvalue()
            total_bytes += len(data)
            image_blobs_preview.append((name, data))
    if st.session_state.prop_name != "— 物件を選択 —":
        _check_free_space_or_stop(total_bytes, root_dir=os.path.join(SAVE_ROOT, current_user, st.session_state.prop_name))

st.info(f"アップロード合計: {len(image_blobs_preview)} 枚")
run_btn = st.button("✅ チェック開始", disabled=(len(image_blobs_preview) == 0))
st.divider()

# 実行
if run_btn:
    if st.session_state.prop_name == "— 物件を選択 —":
        st.error("物件を選択してください。")
        st.stop()
    prop_name = st.session_state.prop_name
    cfg = _load_prop(current_user, prop_name)

    # クオタ（DB値と照合）
    ym = month_key()
    images_used, runs_used = get_monthly_usage(current_user, prop_name, ym)
    quota = cfg.get("quota", {"images": 3000, "runs": 20})
    if runs_used + 1 > int(quota.get("runs", 20)):
        st.error("この物件の今月の実行回数上限を超えています。")
        st.stop()
    if images_used + len(image_blobs_preview) > int(quota.get("images", 3000)):
        st.error("この物件の今月の画像上限を超えています。")
        st.stop()

    openai_key = _load_api_key_from_config()
    if not openai_key:
        st.error("APIキーが未設定です。storage/config.json に {\"openai_api_key\":\"sk-...\"} を保存してください。")
        st.stop()

    thresholds = {
        "NANO_OK_TH": 0.20,
        "NANO_NG_TH": 0.90,
        "FULL_MAX": 3,
        "OK_WHITELIST": _load_global_whitelist(),
        "RECHECK_WHITELIST": _load_global_recheck(),
        "conf_th": float(cfg.get("conf_th", 0.80)),
    }

    with st.spinner("解析中..."):
        summary, results, base_dir, job_id = run_analysis_and_record(
            uid=current_user,
            prop_name=prop_name,
            image_blobs=image_blobs_preview,
            openai_key=openai_key,
            thresholds=thresholds,
            defaults={"conf_th": float(cfg.get("conf_th", 0.80)), "speed_mode": bool(cfg.get("speed_mode", True))},
        )
        st.session_state["last_run"] = {
            "summary": summary, "results": results, "base_dir": base_dir, "job_id": job_id, "prop_name": prop_name
        }

    s = summary or {}
    vc_ok = int(s.get("ok", 0)); vc_ng = int(s.get("ng", 0)); vc_re = int(s.get("unknown", 0))
    top_cols = st.columns(5)
    with top_cols[0]: st.metric("OK", vc_ok)
    with top_cols[1]: st.metric("NG", vc_ng)
    with top_cols[2]: st.metric("要確認", vc_re)
    with top_cols[3]: st.metric("段階:nano", int((s.get("counts_by_stage") or {}).get("nano", 0)))
    with top_cols[4]: st.metric("保存先", job_id)

# 結果ビュー
st.subheader("🖼️ 画像結果ビュー")
last = st.session_state.get("last_run")
if not last:
    st.info("まだ解析結果がありません。画像をアップロードして『✅ チェック開始』を実行してください。")
else:
    summary = last.get("summary") or {}
    results = last.get("results") or []
    base_dir = last.get("base_dir")
    job_id   = last.get("job_id")

    st.subheader("🔎 設備チェック（証跡サマリ）")
    evid = (summary.get("presence_evidence") or {})

    def _evi_card(title: str, key: str, icon: str, max_thumbs: int = 4):
        idxs = list(evid.get(key) or [])
        with st.container(border=True):
            hdr = st.columns([1, 1])
            with hdr[0]: st.markdown(f"**{icon} {title}**")
            with hdr[1]: st.markdown(f"<div style='text-align:right'><span style='display:inline-block;padding:2px 10px;border-radius:999px;background:#333;color:#fff;font-size:12px'>{len(idxs)} 枚</span></div>", unsafe_allow_html=True)
            if not idxs:
                st.info(f"{title} の画像が確認できませんでした。"); return
            paths, caps = [], []
            for i in idxs[:max_thumbs]:
                if 0 <= i < len(results):
                    r = results[i]
                    p = os.path.join(base_dir, r.get("file",""))
                    if os.path.exists(p):
                        paths.append(p); caps.append(f"#{int(r.get('index', i)):04d}")
            if paths:
                thumb_cols = st.columns(min(len(paths), max_thumbs))
                for j, p in enumerate(paths):
                    with thumb_cols[j]: st.image(p, caption=caps[j], use_container_width=True)
            with st.expander("すべて表示"):
                full_paths, full_caps = [], []
                for i in idxs:
                    if 0 <= i < len(results):
                        r = results[i]; p = os.path.join(base_dir, r.get("file",""))
                        if os.path.exists(p): full_paths.append(p); full_caps.append(f"#{int(r.get('index', i)):04d}")
                if full_paths: render_image_grid(full_paths, full_caps)
                else: st.info("画像は見つかりませんでした。")

    c1, c2 = st.columns(2)
    with c1: _evi_card("鍵（必須）", "key", "🔑")
    with c2: _evi_card("Wi-Fi（必須）", "wifi", "📶")
    c3, c4 = st.columns(2)
    with c3: _evi_card("給湯パネル（任意）", "heater", "♨️")
    with c4: _evi_card("TV（任意）", "tv", "📺")

    def _count_verdicts(_results):
        ok = sum(1 for r in _results if r.get("verdict") == "ok")
        ng = sum(1 for r in _results if r.get("verdict") == "ng")
        re = sum(1 for r in _results if r.get("verdict") in ("unknown", "recheck"))
        return ok, ng, re

    def _render_cards(_results, _base_dir, verdicts: tuple[str, ...]):
        for r in _results:
            if r.get("verdict") not in verdicts: continue
            cols = st.columns([2, 3])
            with cols[0]:
                path = os.path.join(_base_dir, r.get("file",""))
                if os.path.exists(path): st.image(path, use_container_width=True)
            with cols[1]:
                st.markdown(
                    f"**判定:** {r.get('verdict')} / **段階:** {r.get('stage')}  \n"
                    f"<div style='padding:8px 10px;border:1px solid rgba(0,0,0,.12);border-radius:6px;background:#f7f7f7;color:#222'>{' / '.join(r.get('comments') or [])}</div>",
                    unsafe_allow_html=True
                )
            st.markdown('---')

    ok_n, ng_n, re_n = _count_verdicts(results)
    tab_ok, tab_ng, tab_re = st.tabs([f"✅ OK（{ok_n}）", f"❌ NG（{ng_n}）", f"🟨 要確認（{re_n}）"])
    with tab_ok: _render_cards(results, base_dir, ("ok",))
    with tab_ng: _render_cards(results, base_dir, ("ng",))
    with tab_re: _render_cards(results, base_dir, ("unknown", "recheck"))

    st.markdown("### ⬇️エクスポート")
    c = st.columns(4)
    c[0].download_button("CSV をダウンロード",  export_csv(job_id,  results), file_name=f"{job_id}_result.csv")
    c[1].download_button("JSON をダウンロード", export_json(job_id, summary, results), file_name=f"{job_id}_result.json")
    c[2].download_button("NG だけZIP",         export_zip_ng(job_id, base_dir, results), file_name=f"{job_id}_NG.zip")
    c[3].download_button("OK だけZIP",         export_zip_ok(job_id, base_dir, results), file_name=f"{job_id}_OK.zip")

# 管理者タブ（SQL KPI対応）
if current_role == "admin":
    st.divider()
    st.header("👤 管理者コンソール")
    tabs = st.tabs(["ユーザー追加/削除/リセット", "ユーザー設定（物件・しきい値・クオタ）", "全体設定（OK/RECHECKホワイトリスト・APIキー）", "お知らせ", "📊 月次レポート(SQL)"])

    # ユーザー管理
    with tabs[0]:
        st.subheader("ユーザー追加")
        new_uid = st.text_input("新規ユーザーID", key="new_uid")
        if st.button("追加"):
            dbu = users_db()
            if new_uid in dbu:
                st.warning("そのユーザーIDは既に存在します。")
            else:
                dbu[new_uid] = {"password": "password", "role": "user", "must_change": True}
                save_users_db(dbu)
                st.success(f"ユーザー {new_uid} を作成しました。初期パスワードは 'password' です。")
        st.subheader("ユーザー削除 / パスワード初期化")
        target_uid = st.text_input("対象ユーザーID", key="del_uid")
        c1, c2 = st.columns(2)
        if c1.button("削除"):
            dbu = users_db()
            if target_uid in dbu and target_uid != "admin":
                dbu.pop(target_uid, None); save_users_db(dbu); st.success(f"{target_uid} を削除しました。")
            else:
                st.warning("削除できません。")
        if c2.button("パスワード初期化"):
            dbu = users_db()
            if target_uid in dbu:
                dbu[target_uid]["password"]    = "password"
                dbu[target_uid]["must_change"] = True
                save_users_db(dbu)
                st.success(f"{target_uid} のパスワードを 'password' に初期化しました。")

    # 物件設定
    with tabs[1]:
        st.subheader("ユーザーの物件設定・しきい値・クオタ")
        dbu = users_db()
        user_list = sorted(dbu.keys())
        sel_uid = st.selectbox("ユーザーIDを選択", options=user_list,
                               index=max(0, user_list.index(current_user)) if current_user in user_list else 0)
        user_prop_dir = os.path.join(PROPS_DIR, sel_uid)
        os.makedirs(user_prop_dir, exist_ok=True)
        prop_files = [f[:-5] for f in os.listdir(user_prop_dir) if f.endswith(".json")]
        prop_options = ["（新規作成）"] + sorted(prop_files)

        sel_prop = st.selectbox("物件名を選択 / 新規", options=prop_options)
        target_prop = (st.text_input("新規物件名", value="") if sel_prop == "（新規作成）" else sel_prop).strip()
        if not target_prop:
            st.info("物件名を入力すると編集フォームが表示されます。")
        else:
            cfg = _load_prop(sel_uid, target_prop)
            st.markdown("### 物件設定")
            cfg["has_tv"]           = st.checkbox("TVあり", value=cfg.get("has_tv", True), key="edit_tv")
            cfg["has_heater_panel"] = st.checkbox("給湯パネルあり", value=cfg.get("has_heater_panel", True), key="edit_heater")
            cfg["conf_th"]          = st.slider("conf_th", 0.30, 0.95, float(cfg.get("conf_th", 0.80)), 0.01, key="edit_conf")
            cfg["speed_mode"]       = st.checkbox("スピードモード", value=cfg.get("speed_mode", True), key="edit_speed")
            st.markdown("### クオタ（この物件に適用）")
            q = cfg.get("quota", {"images":3000, "runs":20})
            q_images = st.number_input("今月の画像上限", 100, 100000, int(q.get("images",3000)), 50, key="edit_q_img")
            q_runs   = st.number_input("今月の実行回数上限", 1, 9999,     int(q.get("runs",20)),   1, key="edit_q_runs")
            cfg["quota"] = {"images": int(q_images), "runs": int(q_runs)}
            if st.button("保存", key="save_prop_cfg"):
                _save_prop(sel_uid, target_prop, cfg); st.success(f"保存しました（ユーザー:{sel_uid} / 物件:{target_prop}）。")

    # ホワイトリスト/APIキー/お知らせ
    with tabs[2]:
        st.subheader("全ユーザー共通 OK_WHITELIST")
        cur = _load_global_whitelist() or []
        txt = st.text_area("改行区切りで入力", value="\n".join(cur), height=220)
        if st.button("保存（ホワイトリスト）", key="save_global_okwl"):
            items = [x.strip() for x in txt.splitlines() if x.strip()]
            _save_global_whitelist(items); st.success("全体OK_WHITELISTを保存しました。")

        st.subheader("全ユーザー共通 RECHECK_WHITELIST（要確認へ分類）")
        cur_r = _load_global_recheck() or []
        txt_r = st.text_area("改行区切りで入力", value="\n".join(cur_r) if cur_r else "", height=200, key="recheck_ta")
        if st.button("保存（リチェック）", key="save_global_recheck"):
            items = [x.strip() for x in txt_r.splitlines() if x.strip()]
            _save_global_recheck(items); st.success("RECHECK_WHITELISTを保存しました。")

        st.subheader("OpenAI APIキー（storage/config.json）")
        st.code('{\n  "openai_api_key": "sk-ここにキー"\n}', language="json")
        show_key = _load_api_key_from_config()
        masked = ("****" + show_key[-6:]) if show_key else "(未設定)"
        st.text(f"現在の設定: {masked}")

    with tabs[3]:
        st.subheader("お知らせ（Markdown）")
        cur_notice = _load_text(NOTICE_MD)
        txt_notice = st.text_area("内容を編集", value=cur_notice, height=260)
        if st.button("保存（お知らせ）"):
            _save_text(NOTICE_MD, txt_notice); st.success("お知らせを保存しました。")

    # 📊 月次レポート(SQL)
    with tabs[4]:
        st.subheader("月次KPI（SQL集計）")
        dbu = users_db()
        users = sorted(dbu.keys())
        cols = st.columns(3)
        sel_user = cols[0].selectbox("ユーザー", options=users, index=max(0, users.index(current_user)) if current_user in users else 0)
        ym_input = cols[1].text_input("対象年月 (YYYYMM または YYYY-MM)", value=datetime.now().strftime("%Y-%m"))
        ym = ym_input.replace("-", "")
        if len(ym) != 6 or not ym.isdigit():
            st.warning("YYYYMM 形式で入力してください。例: 2025-09 または 202509")
        else:
            # 物件別サマリ
            kpi = query_monthly_kpi(sel_user, ym)
            if not kpi:
                st.info("該当データがありません。実行後に再度お試しください。")
            else:
                import pandas as pd
                df = pd.DataFrame(kpi)
                df["ng_rate(%)"] = (df["ng_rate"] * 100).round(2)
                st.dataframe(df[["property","jobs","images","ok","ng","unknown","ng_rate(%)"]], use_container_width=True)
                # CSV
                st.download_button(
                    "物件別サマリCSVをダウンロード",
                    data=df.to_csv(index=False).encode("utf-8"),
                    file_name=f"{sel_user}_{ym}_kpi_summary.csv",
                    mime="text/csv"
                )
                # 物件選択→ジョブ明細
                prop_opts = ["(すべて)"] + [r["property"] for r in kpi]
                sel_prop = cols[2].selectbox("物件（任意）", options=prop_opts)
                detail = query_monthly_jobs_detail(sel_user, ym, None if sel_prop=="(すべて)" else sel_prop)
                if detail:
                    df2 = pd.DataFrame(detail)
                    st.markdown("#### ジョブ明細")
                    st.dataframe(df2, use_container_width=True, height=280)
                    st.download_button(
                        "ジョブ明細CSVをダウンロード",
                        data=df2.to_csv(index=False).encode("utf-8"),
                        file_name=f"{sel_user}_{ym}_jobs_detail.csv",
                        mime="text/csv"
                    )
