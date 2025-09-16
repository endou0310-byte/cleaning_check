# -*- coding: utf-8 -*-
from __future__ import annotations
import os, io, json, csv, zipfile, hashlib, base64, tempfile, re
from datetime import datetime
from dataclasses import dataclass, asdict, is_dataclass
from typing import List, Dict, Any, Optional, Tuple

import numpy as np
from PIL import Image, ImageOps

# HEIC 読み込み（インストール済みなら自動有効）
try:
    import pillow_heif  # type: ignore
    pillow_heif.register_heif_opener()
except Exception:
    pass

# OpenCV があれば画質フラグに利用
try:
    import cv2  # type: ignore
    _HAS_CV2 = True
except Exception:
    _HAS_CV2 = False

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from concurrent.futures import ThreadPoolExecutor, as_completed


# ===== ラベル定義 =====
LABELS = ["hair_dust", "clutter", "quality", "unnatural"]
TYPE_JA = {
    "hair_dust": "髪/ホコリ",
    "clutter":   "乱雑",
    "quality":   "低品質",
    "unnatural": "不自然",
}

# ===== コメント判定（日本語） =====
KW = r"(髪の毛|毛髪|抜け毛|ほこり|ホコリ|埃)"
NEG = (
    r"(?:ない|ありません|無し|なし|見当たらない|見受けられません|"
    r"ほぼない|ほとんどない|少ない|目立たない|見られません|認められません|"
    r"見受けにくい|確認できず|確認できません)"
)
POS = r"(?:清潔|清掃状態は良好|概ね良好|問題なし|良好|綺麗|きれい|整頓|整えられている|清潔感がある|不自然な点はない|不自然さは感じられない)"

NEG_AFTER  = re.compile(KW + r".{0,12}" + NEG)
NEG_BEFORE = re.compile(NEG + r".{0,12}" + KW)
POS_ANY    = re.compile(POS)
POS_NEAR   = re.compile(KW + r".{0,12}" + POS + "|" + POS + r".{0,12}" + KW)
ANY_KW     = re.compile(KW)


@dataclass
class ImageResult:
    index: int
    file: str
    labels: List[str]
    scores: Dict[str, float]
    comments: List[str]          # 日本語コメント
    quality_flags: List[str]
    verdict: str                 # "ok" | "ng" | "unknown"
    stage: str                   # "nano" | "mini" | "full"
    presence: Dict[str, Optional[bool]]  # {"key":true/false, "wifi":..., "heater":..., "tv":...}


# ===== ユーティリティ =====
def image_to_jpeg_bytes(img: Image.Image, quality: int = 80) -> bytes:
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=quality)
    return buf.getvalue()

def load_and_resize(raw: bytes, max_long_edge: int = 960) -> Tuple[bytes, Tuple[int, int]]:
    img = Image.open(io.BytesIO(raw)).convert("RGB")
    if max(img.size) > max_long_edge:
        img = ImageOps.contain(img, (max_long_edge, max_long_edge))
    return image_to_jpeg_bytes(img, 80), img.size

def _calc_basic_quality_flags(img: Image.Image) -> List[str]:
    flags: List[str] = []
    try:
        if _HAS_CV2:
            ar = np.array(img.convert("L"))
            var = cv2.Laplacian(ar, cv2.CV_64F).var()
            if var < 80.0:
                flags.append("blur")
        mean = np.array(img.convert("L")).mean()
        if mean < 60:
            flags.append("dark")
        over = (np.array(img.convert("L")) > 240).mean()
        if over > 0.40:
            flags.append("overexpose")
    except Exception:
        pass
    return flags

def _rv(obj, key, default=None):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)

# --- 簡易グリッド表示（Streamlitがある場合のみ機能） ---
def render_image_grid(paths, captions=None):
    try:
        import streamlit as st
    except Exception:
        return
    if not paths:
        st.info("画像はありません。")
        return
    captions = captions or [""] * len(paths)
    cols = st.columns(2)
    for i, p in enumerate(paths):
        with cols[i % 2]:
            if os.path.exists(p):
                st.image(p, caption=captions[i], use_container_width=True)


# ===== コメントに基づく判定補正 =====
def refine_verdict_by_text(verdict: str, comments: List[str], ok_whitelist: List[str]) -> str:
    txt = " ".join(comments)
    for w in ok_whitelist:
        if w and w in txt:
            return "ok"
    if NEG_AFTER.search(txt) or NEG_BEFORE.search(txt) or POS_NEAR.search(txt):
        return "ok"
    if POS_ANY.search(txt):
        return "ok"
    if ANY_KW.search(txt):
        return "ng"
    return verdict

# ★追加: コメント語句で強制的に「要確認(unknown)」へ
def force_recheck_by_text(verdict: str, comments: List[str], recheck_words: List[str]) -> str:
    txt = " ".join(comments)
    for w in recheck_words or []:
        if w and w in txt:
            return "unknown"
    return verdict

# ===== OpenAI クライアント =====
class OpenAIClient:
    def __init__(self, model_name: str, api_key: Optional[str]):
        from openai import OpenAI
        self.model_name = (model_name or "").strip()

        # projキーでも動くように環境変数から org / project を拾う
        api_key = (api_key or "").strip()
        org  = (os.getenv("OPENAI_ORG") or os.getenv("OPENAI_ORGANIZATION") or "").strip()
        proj = (os.getenv("OPENAI_PROJECT") or "").strip()

        kwargs = {}
        if api_key:
            kwargs["api_key"] = api_key
        if org:
            kwargs["organization"] = org
        if proj:
            kwargs["project"] = proj

        self._client = OpenAI(**kwargs) if api_key else None

    def available(self) -> bool:
        return (self._client is not None) and bool(self.model_name)

    def _supports_temperature(self) -> bool:
        m = (self.model_name or "").lower()
        return not ("nano" in m or "mini" in m)

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(Exception),
    )
    def analyze_one(self, data: bytes) -> Dict[str, Any]:
        if not self.available():
            raise RuntimeError("OpenAIClient not available")

        # 日本語指示 + presenceを厳密JSONで返す（設備定義を具体化）
        msg = [
            {"role": "system", "content": "あなたはホテル・民泊の清掃チェック補助AIです。日本語で簡潔に返答してください。"},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": (
                        "この画像を清掃観点で解析し、必ず次のJSONだけを返してください。"
                        " keys: "
                        "  - labels: string[],"
                        "  - scores: object {hair_dust, clutter, quality, unnatural ∈ [0,1]},"
                        "  - comments: string[] (日本語・短文・最大5件),"
                        "  - presence: object {key:boolean|null, wifi:boolean|null, heater:boolean|null, tv:boolean|null}."
                        "\n\n"
                        "【presence 判定基準（厳密）】\n"
                        "- 共通: はっきり確認できるときだけ true。写っていない/判別困難なら null。明確に無いと分かる場合のみ false。\n"
                        "- key（鍵）: 物理的な鍵・キーボックス・スマートロックの実機や鍵束が写っている。鍵のイラストや単なるドアノブのみは不可。\n"
                        "- wifi（Wi-Fi）: ルーター/SSID・パスワードが書かれた紙/カード/ラベル、または"
                        " スマホ等のスクリーンショットで SSID/パスワード/接続画面が明瞭な場合も true。"
                        " 単なるケーブルやコンセントのみは不可。\n"
                        "- heater（給湯パネル）: 給湯器リモコン・温度設定パネル（風呂/台所等）。"
                        " ディスプレイに温度(℃)表示、湯/お湯/風呂/自動/追い焚き等の表示・アイコン、"
                        " 運転/停止などの給湯関連ボタンがある操作盤。"
                        " 【NG例（heater=false/null）】: エアコンのリモコン/室内機パネル、"
                        " インターホン/ドアホン、床暖房パネル、ガスメーター、電気ブレーカ、"
                        " ただの壁スイッチ。\n"
                        "- tv（テレビ）: テレビ本体の画面・ベゼル・リモコン等が明瞭。"
                        " モニターやデジタルサイネージの可能性が高く判別困難なら null。\n\n"
                        "【出力の厳密性】\n"
                        "- JSON以外の文字列や説明を含めない。\n"
                        "- presenceの各キーは必ず出力し、boolean もしくは null とする。\n"
                        "- 迷った場合は null を選ぶ（推測で true にしない）。\n"
                    )},
                    {"type": "image_url",
                     "image_url": {"url": "data:image/jpeg;base64," + base64.b64encode(data).decode("utf-8")}},
                ],
            },
        ]
        kwargs = dict(model=self.model_name, messages=msg, response_format={"type": "json_object"})
        if self._supports_temperature():
            kwargs["temperature"] = 0.2
        resp = self._client.chat.completions.create(**kwargs)
        return json.loads(resp.choices[0].message.content)


# ===== 1枚解析 =====
def _analyze_one(
    index: int,
    blob,
    nano: OpenAIClient,
    mini: OpenAIClient,
    thresholds: Dict[str, float],
    defaults: Dict[str, Any],
    base_dir: str,
) -> Dict[str, Any]:

    # 入力正規化
    src_name = f"{index:04d}.jpg"
    if isinstance(blob, tuple) and len(blob) == 2 and isinstance(blob[1], (bytes, bytearray)):
        src_name = os.path.basename(str(blob[0] or src_name))
        raw = bytes(blob[1])
    elif isinstance(blob, (bytes, bytearray)):
        raw = bytes(blob)
    elif hasattr(blob, "read"):
        try:
            raw = blob.read()
            if hasattr(blob, "name"):
                src_name = os.path.basename(getattr(blob, "name", src_name))
        except Exception:
            raw = b""
    else:
        raw = b""

    # 画像縮小
    data, _ = load_and_resize(raw)
    img = Image.open(io.BytesIO(data)).convert("RGB")

    # 画質フラグ
    qflags = _calc_basic_quality_flags(img)

    # nano 解析（日本語応答）
    try:
        d = nano.analyze_one(data) if nano.available() else {
            "labels": [], "scores": {"quality": 0.0},
            "comments": ["（ドライラン）APIキー未設定のため実解析は未実施"],
            "presence": {}
        }
    except Exception as e:
        d = {"labels": [], "scores": {"quality": 0.0}, "comments": [f"nano失敗: {e}"], "presence": {}}

    labels = d.get("labels", [])
    scores = {k: float(v) for k, v in (d.get("scores") or {}).items()} if isinstance(d.get("scores"), dict) else {}
    comments = [str(x) for x in (d.get("comments") or [])] if isinstance(d.get("comments"), list) else []
    presence = d.get("presence", {}) if isinstance(d.get("presence"), dict) else {}

    # 判定
    th = float(defaults.get("conf_th", thresholds.get("conf_th", 0.6)))
    verdict = "ng" if any(v >= th for v in scores.values()) else "ok"

    # ホワイトリスト
    def _split_lines(t: str) -> List[str]:
        return [ln.strip() for ln in (t or "").splitlines() if ln.strip()]
    ok_whitelist_global = _split_lines(str(defaults.get("ok_whitelist_global", "")))
    ok_whitelist_user   = _split_lines(str(defaults.get("ok_whitelist", "")))
    verdict = refine_verdict_by_text(verdict, comments, ok_whitelist_global + ok_whitelist_user)

    # ★追加: 要確認ワードで unknown に補正
    recheck_words = thresholds.get("RECHECK_WHITELIST", [])
    verdict = force_recheck_by_text(verdict, comments, recheck_words)

    # 保存
    safe_name = f"{index:04d}_" + os.path.basename(src_name)
    out_path = os.path.join(base_dir, safe_name)
    try:
        with open(out_path, "wb") as f:
            f.write(data)
    except Exception:
        pass

    return {
        "index": index,
        "file": safe_name,
        "labels": labels,
        "scores": scores,
        "comments": comments,
        "quality_flags": qflags,
        "verdict": verdict,
        "stage": "nano",
        "presence": presence,
    }


# ===== エクスポート =====
def export_csv(job_id: str, results) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["job_id", "index", "file", "verdict", "stage", "comments"])
    for r in results:
        w.writerow([
            job_id,
            f"{int(_rv(r, 'index', 0)):04d}",
            _rv(r, "file", ""),
            _rv(r, "verdict", ""),
            _rv(r, "stage", ""),
            " / ".join(_rv(r, "comments", []) or []),
        ])
    return buf.getvalue().encode("utf-8")

def export_json(job_id: str, summary, results) -> bytes:
    if not isinstance(summary, dict):
        summary = asdict(summary) if is_dataclass(summary) else {}
    imgs = []
    for r in results:
        if isinstance(r, dict):
            imgs.append(r)
        else:
            imgs.append({
                "index": _rv(r, "index", 0),
                "file": _rv(r, "file", ""),
                "labels": _rv(r, "labels", []),
                "scores": _rv(r, "scores", {}),
                "comments": _rv(r, "comments", []),
                "verdict": _rv(r, "verdict", ""),
                "stage": _rv(r, "stage", ""),
            })
    return json.dumps({"job_id": job_id, "summary": summary, "images": imgs}, ensure_ascii=False, indent=2).encode("utf-8")

def _zip_common(job_id: str, base_dir: str, results, want: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for r in results:
            if _rv(r, "verdict", "") != want:
                continue
            idx = int(_rv(r, "index", 0))
            name = f"{idx:04d}_" + _rv(r, "file", "")
            path = os.path.join(base_dir, name)
            if os.path.exists(path):
                z.write(path, arcname=name)
    return buf.getvalue()

def export_zip_ng(job_id: str, base_dir: str, results) -> bytes:
    return _zip_common(job_id, base_dir, results, "ng")

def export_zip_ok(job_id: str, base_dir: str, results) -> bytes:
    return _zip_common(job_id, base_dir, results, "ok")


# ===== 解析本体 =====
def analyze_headless(
    files,
    property_name: str,
    api_key: str,
    thresholds: Optional[Dict[str, float]] = None,
    defaults: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], str, str]:
    thresholds = thresholds or {}
    defaults   = defaults or {}

    if "OK_WHITELIST" in thresholds and isinstance(thresholds["OK_WHITELIST"], list):
        defaults.setdefault("ok_whitelist_global", "\n".join(thresholds["OK_WHITELIST"]))

    nano = OpenAIClient("gpt-5-nano", api_key)
    mini = OpenAIClient("", api_key)  # 予備（今は未使用）

    base_dir = tempfile.mkdtemp(prefix="analyze_")
    job_id = datetime.now().strftime("J%Y%m%dT%H%M%S")

    results: List[Dict[str, Any]] = []
    max_workers = max(1, min(8, len(files)))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_analyze_one, i, f, nano, mini, thresholds, defaults, base_dir) for i, f in enumerate(files)]
        for fu in as_completed(futures):
            r = fu.result()
            if r is not None:
                results.append(r)
    results.sort(key=lambda x: x.get("index", 0))

    # 設備の証跡（presence==true の画像 index を集計）
    presence_evidence = {"key": [], "wifi": [], "heater": [], "tv": []}
    for r in results:
        pr = r.get("presence") or {}
        for k in presence_evidence.keys():
            v = pr.get(k)
            if isinstance(v, bool) and v:
                presence_evidence[k].append(int(r.get("index", 0)))

    summary: Dict[str, Any] = {
        "ok":      sum(1 for r in results if r.get("verdict") == "ok"),
        "ng":      sum(1 for r in results if r.get("verdict") == "ng"),
        "unknown": sum(1 for r in results if r.get("verdict") == "unknown"),
        "counts_by_stage": {
            "nano": sum(1 for r in results if r.get("stage") == "nano"),
            "mini": sum(1 for r in results if r.get("stage") == "mini"),
            "full": sum(1 for r in results if r.get("stage") == "full"),
        },
        "presence_evidence": presence_evidence,
    }
    return summary, results, base_dir, job_id
