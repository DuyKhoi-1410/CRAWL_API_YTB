# -*- coding: utf-8 -*-
"""
Bronze Pipeline - YouTube Data Crawler
=======================================
Cai tien so voi phien ban cu:
  - Chong trung bang comment_id thay vi LOOKBACK 15 phut
  - Xoay vong 4 kenh (cross-channel interleaving) phan bo quota deu
  - Uu tien video cu nhat truoc (oldest-first) tranh mat du lieu
  - He thong ngan sach: gioi han theo trang API, khong theo so dong
  - FIX: Backfill loc known_ids (tranh trung voi incremental)
  - FIX: invalidPageToken -> mark done, khong reset ve dau
"""

import os
import re
import json
import time
import traceback
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Deque, Dict, List, Optional, Tuple

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# ======================================================================
# 1. CAU HINH (Configuration)
# ======================================================================

BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "bd-raw-data-bronze")

CHANNELS = {
    "UCFMEYTv6N64hIL9FlQ_hxBw": "Dong Tay Promotion",
    "UCTOWyiIkPEqyh_2O-ArJR5w": "Forest Studio",
    "UCkna2OcuN1E6u5I8GVtdkOw": "Vie Channel",
    "UCh_zF2FsiCflCPgYDudtcqg": "Yeah1",
}

MIN_DURATION_SEC    = 180    # Bo video < 3 phut (Shorts / Teaser)
TOTAL_PAGE_BUDGET   = 500    # Tong so trang comment / phien chay
MAX_PAGES_PER_VIDEO = 10     # Toi da trang comment / video / phien
MAX_API_ATTEMPTS    = 700    # Toi da request API / phien chay
KNOWN_IDS_LIMIT     = 300    # So comment_id luu trong state / video

WINDOW_LOOKBACK_DAYS = 20    # Cua so bat dau: N-20
WINDOW_END_DAYS      = 3     # Cua so ket thuc: N-3

NOW           = datetime.now(timezone.utc)
WINDOW_START  = NOW - timedelta(days=WINDOW_LOOKBACK_DAYS)
WINDOW_END    = NOW - timedelta(days=WINDOW_END_DAYS)
INGESTED_AT   = NOW.isoformat()
RUN_TS        = NOW.strftime("%Y%m%dT%H%M%SZ")

LOCAL_ROOT    = "./colab_output"
STATE_PATH    = "state/video_state.json"
BRONZE_PREFIX = f"bronze/youtube/run_date={NOW:%Y-%m-%d}/run_ts={RUN_TS}"

_api_attempts  = 0
_comment_pages = 0


# ======================================================================
# 2. EXCEPTIONS
# ======================================================================

class QuotaExceeded(Exception):
    """Het quota API ngay: dung va luu an toan."""

class RunBudgetExceeded(Exception):
    """Vuot tran request phien chay."""


# ======================================================================
# 3. STORAGE
# ======================================================================

class StorageAdapter:
    """Day du lieu tho len Bronze (GCS hoac Local)."""

    def __init__(self):
        self.use_gcs = bool(BUCKET_NAME)
        if self.use_gcs:
            from google.cloud import storage
            self.client = storage.Client()
            self.bucket = self.client.bucket(BUCKET_NAME)

    def _local_path(self, path: str) -> str:
        full = os.path.join(LOCAL_ROOT, path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        return full

    def read_state(self, path: str):
        if self.use_gcs:
            blob = self.bucket.blob(path)
            return json.loads(blob.download_as_text(encoding="utf-8")) if blob.exists() else None
        full = os.path.join(LOCAL_ROOT, path)
        if not os.path.exists(full):
            return None
        with open(full, encoding="utf-8") as f:
            return json.load(f)

    def write_raw_jsonl(self, path: str, rows: List[dict]) -> None:
        if not rows:
            return
        content = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows)
        if self.use_gcs:
            self.bucket.blob(path).upload_from_string(
                content.encode("utf-8"), content_type="application/x-ndjson"
            )
        with open(self._local_path(path), "w", encoding="utf-8", newline="\n") as f:
            f.write(content)

    def write_state_json(self, path: str, obj: dict) -> None:
        content = json.dumps(obj, ensure_ascii=False, indent=2)
        if self.use_gcs:
            self.bucket.blob(path).upload_from_string(
                content.encode("utf-8"), content_type="application/json"
            )
        with open(self._local_path(path), "w", encoding="utf-8", newline="\n") as f:
            f.write(content)


# ======================================================================
# 4. HELPERS
# ======================================================================

def get_api_key() -> str:
    key = os.environ.get("YOUTUBE_API_KEY")
    if not key:
        raise RuntimeError("Chua cau hinh YOUTUBE_API_KEY trong bien moi truong.")
    return key.strip()

def parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def clean_text(text) -> Optional[str]:
    return text.replace("\n", " ").replace("\r", " ") if text else None

def to_int(val) -> Optional[int]:
    try: return int(val)
    except (TypeError, ValueError): return None

def parse_duration_sec(iso: str) -> int:
    m = re.fullmatch(r"P(?:(\d+)D)?T?(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso or "")
    if not m: return 0
    d, h, mi, s = (int(x or 0) for x in m.groups())
    return d * 86400 + h * 3600 + mi * 60 + s

def execute_api(request, retries: int = 3):
    """Thuc thi API call voi retry + kiem tra ngan sach."""
    global _api_attempts
    for i in range(retries):
        if _api_attempts >= MAX_API_ATTEMPTS:
            raise RunBudgetExceeded(f"Dat tran {MAX_API_ATTEMPTS} API attempts.")
        _api_attempts += 1
        try:
            return request.execute()
        except HttpError as e:
            try:
                reason = json.loads(e.content.decode())["error"]["errors"][0].get("reason", "unknown")
            except Exception:
                reason = "unknown"
            if reason in ("quotaExceeded", "dailyLimitExceeded"):
                raise QuotaExceeded(reason)
            if (reason in ("rateLimitExceeded", "userRateLimitExceeded") or
                    (e.resp and e.resp.status >= 500)) and i < retries - 1:
                time.sleep(2 ** (i + 1))
                continue
            raise


# ======================================================================
# 5. VIDEO METADATA
# ======================================================================

def build_video_record(item: dict) -> Tuple[str, Optional[dict], str]:
    """Chuyen item API thanh record chuan. Tra ve (vid, record, status)."""
    vid   = item["id"]
    snip  = item.get("snippet", {})
    stats = item.get("statistics", {})
    det   = item.get("contentDetails", {})

    dur = parse_duration_sec(det.get("duration"))
    if 0 < dur < MIN_DURATION_SEC:
        return vid, None, "skipped_short"

    pub_at   = snip.get("publishedAt")
    age_days = round((NOW - parse_iso(pub_at)).total_seconds() / 86400, 2) if pub_at else None

    record = {
        "video_id":            vid,
        "channel_id":          snip.get("channelId"),
        "channel_title":       clean_text(snip.get("channelTitle")),
        "video_title":         clean_text(snip.get("title")),
        "program_name":        None,
        "video_published_at":  pub_at,
        "duration_sec":        dur,
        "view_count":          to_int(stats.get("viewCount")),
        "video_like_count":    to_int(stats.get("likeCount")),
        "total_comment_count": to_int(stats.get("commentCount")),
        "video_age_days":      age_days,
        "ingested_at":         INGESTED_AT,
        "run_id":              RUN_TS,
    }
    return vid, record, "active"


# ======================================================================
# 6. COMMENT CRAWLING
# ======================================================================

def _make_comment_row(thread: dict, vid: str) -> dict:
    cs = thread["snippet"]["topLevelComment"]["snippet"]
    return {
        "comment_id":           thread["snippet"]["topLevelComment"]["id"],
        "video_id":             vid,
        "program_name":         None,
        "parent_id":            None,
        "channel_id":           cs.get("channelId"),
        "author_channel_id":    (cs.get("authorChannelId") or {}).get("value"),
        "author_display_name":  clean_text(cs.get("authorDisplayName")),
        "raw_text":             clean_text(cs.get("textOriginal") or cs.get("textDisplay")),
        "like_count":           to_int(cs.get("likeCount")),
        "reply_count":          to_int(thread["snippet"].get("totalReplyCount")),
        "comment_published_at": cs.get("publishedAt"),
        "ingested_at":          INGESTED_AT,
        "run_id":               RUN_TS,
    }


def _fetch_page(yt, vid: str, page_token: Optional[str] = None) -> Tuple[List[dict], Optional[str]]:
    """Goi API lay 1 trang comment (toi da 100 rows). Tang counter trang."""
    global _comment_pages
    resp = execute_api(
        yt.commentThreads().list(
            part="snippet", videoId=vid,
            maxResults=100, order="time",
            textFormat="plainText", pageToken=page_token,
        )
    )
    _comment_pages += 1
    return [_make_comment_row(t, vid) for t in resp.get("items", [])], resp.get("nextPageToken")


def _update_known_ids(vs: dict, rows: List[dict]) -> None:
    """Them comment_id moi vao danh sach da biet, giu toi da KNOWN_IDS_LIMIT."""
    existing = vs.get("latest_comment_ids") or []
    new_ids  = [r["comment_id"] for r in rows if r.get("comment_id")]
    vs["latest_comment_ids"] = list(dict.fromkeys(new_ids + existing))[:KNOWN_IDS_LIMIT]


def collect_new_comments(yt, vid: str, vs: dict, cbuf: List[dict], max_pages: int) -> Tuple[int, int]:
    """
    Incremental: cao comment MOI tu dinh xuong.
    Dung ngay khi gap comment_id da co trong known_ids.
    Returns: (pages_used, n_new_rows)
    """
    known = set(vs.get("latest_comment_ids") or [])
    seen  = set()
    fresh = []
    token = None
    pages = 0

    while pages < max_pages:
        rows, next_token = _fetch_page(yt, vid, page_token=token)
        pages += 1
        hit_overlap = False

        for row in rows:
            cid = row.get("comment_id")
            if cid in known or cid in seen:
                hit_overlap = True
                break
            fresh.append(row)
            if cid:
                seen.add(cid)

        if hit_overlap or not next_token:
            break
        token = next_token

    if fresh:
        cbuf.extend(fresh)
        newest = max(
            (r["comment_published_at"] for r in fresh if r.get("comment_published_at")),
            key=parse_iso, default=None,
        )
        if newest and (not vs.get("watermark") or parse_iso(newest) > parse_iso(vs["watermark"])):
            vs["watermark"] = newest
        _update_known_ids(vs, fresh)

    return pages, len(fresh)


def collect_backfill_page(yt, vid: str, vs: dict, cbuf: List[dict]) -> Tuple[int, bool]:
    """
    Backfill: cao 1 trang comment CU tu page_token luu trong state.

    FIX 1: Loc trung voi known_ids truoc khi dua vao buffer.
    FIX 2: invalidPageToken -> mark done (khong reset ve trang dau).

    Returns: (n_new_rows, backfill_complete)
    """
    known = set(vs.get("latest_comment_ids") or [])

    try:
        rows, next_token = _fetch_page(yt, vid, page_token=vs.get("backfill_page_token"))
    except HttpError as e:
        try:
            reason = json.loads(e.content.decode())["error"]["errors"][0].get("reason", "")
        except Exception:
            reason = ""
        if reason in ("invalidPageToken", "pageTokenInvalid"):
            # FIX 2: page token het han -> mark done, khong cao lai tu dau
            print(f"   [WARN] {vid}: backfill page token expired -> mark complete")
            vs["backfill_page_token"] = None
            vs["backfill_complete"]   = True
            return 0, True
        raise

    # FIX 1: chi lay comment chua co trong known_ids
    fresh = [r for r in rows if r.get("comment_id") not in known]

    if fresh:
        cbuf.extend(fresh)
        if not vs.get("latest_comment_ids"):
            _update_known_ids(vs, fresh)

    vs["backfill_page_token"] = next_token
    vs["backfill_complete"]   = not bool(next_token)
    return len(fresh), vs["backfill_complete"]


# ======================================================================
# 7. INTERLEAVED CRAWL (Oldest-First + Cross-Channel)
# ======================================================================

def _build_queues(state: dict) -> Dict[str, Deque[str]]:
    """
    Tao queue video cho tung kenh, sap xep oldest-first
    (video gan N-20 nhat len dau, uu tien truoc khi het cua so).
    """
    grouped: Dict[str, List[Tuple[datetime, str]]] = defaultdict(list)

    for vid, vs in state.get("videos", {}).items():
        if vs.get("status") != "active" or vs.get("comments_disabled"):
            continue
        cid = vs.get("channel_id")
        if cid not in CHANNELS:
            continue
        pub_str = vs.get("published_at") or ""
        pub_dt  = parse_iso(pub_str) if pub_str else datetime.min.replace(tzinfo=timezone.utc)
        grouped[cid].append((pub_dt, vid))

    queues: Dict[str, Deque[str]] = {}
    for cid in CHANNELS:
        items = sorted(grouped.get(cid, []))   # ASC -> cu nhat len dau
        queues[cid] = deque(vid for _, vid in items)
        print(f"   [{CHANNELS[cid]}] {len(queues[cid])} videos (oldest-first)")
    return queues


def process_comments_interleaved(yt, state: dict, cbuf: List[dict]) -> None:
    """
    Xoay vong cao comment qua 4 kenh trong gioi han ngan sach.
    Moi vong: moi kenh xu ly 1 trang cua video cu nhat con lai.
    """
    global _comment_pages
    _comment_pages = 0

    queues      = _build_queues(state)
    pages_used  = defaultdict(int)
    active_cids = [c for c in CHANNELS if queues.get(c)]

    if not active_cids:
        print("Khong co video active de cao comment.")
        return

    print(f"\n[COMMENT CRAWL] budget={TOTAL_PAGE_BUDGET}p | max={MAX_PAGES_PER_VIDEO}/video")
    print("   Xoay vong: " + " -> ".join(CHANNELS[c] for c in active_cids))

    progress = True
    while progress and _comment_pages < TOTAL_PAGE_BUDGET:
        progress = False

        for cid in active_cids:
            if _comment_pages >= TOTAL_PAGE_BUDGET:
                break
            q = queues[cid]
            if not q:
                continue

            # Tim video con trong budget trang
            vid = None
            for _ in range(len(q)):
                candidate = q.popleft()
                if pages_used[candidate] < MAX_PAGES_PER_VIDEO:
                    vid = candidate
                    break
            if vid is None:
                continue

            vs     = state["videos"][vid]
            before = _comment_pages

            try:
                if not vs.get("watermark"):
                    # Lan dau: lay trang backfill dau tien de khoi tao known_ids
                    n_new, done = collect_backfill_page(yt, vid, vs, cbuf)
                    mode = "INIT-BF"
                else:
                    next_mode = vs.get("next_comment_mode", "incremental")
                    if next_mode == "backfill" and not vs.get("backfill_complete"):
                        n_new, done = collect_backfill_page(yt, vid, vs, cbuf)
                        mode = "BACKFILL"
                        vs["next_comment_mode"] = "incremental"
                    else:
                        _, n_new = collect_new_comments(yt, vid, vs, cbuf, max_pages=1)
                        mode = "INCREM "
                        vs["next_comment_mode"] = "backfill" if not vs.get("backfill_complete") else "incremental"

                used = _comment_pages - before
                pages_used[vid] += used
                if used > 0:
                    progress = True

                cname = CHANNELS.get(cid, cid)[:12]
                print(f"   [{mode}] {cname:12} | {vid} | +{n_new:3d} new | p={pages_used[vid]}/{MAX_PAGES_PER_VIDEO}")

                # Day video tro lai queue neu con can cao
                if pages_used[vid] < MAX_PAGES_PER_VIDEO:
                    still_needed = (
                        not vs.get("backfill_complete") or
                        vs.get("next_comment_mode", "incremental") == "incremental"
                    )
                    if still_needed:
                        q.append(vid)

            except HttpError as e:
                try:
                    reason = json.loads(e.content.decode())["error"]["errors"][0].get("reason", "")
                except Exception:
                    reason = ""
                if reason == "commentsDisabled":
                    vs["comments_disabled"] = True
                    print(f"   [SKIP ] {vid}: commentsDisabled")
                else:
                    print(f"   [ERR  ] {vid}: {reason}")
                    pages_used[vid] = MAX_PAGES_PER_VIDEO


# ======================================================================
# 8. PIPELINE CHINH
# ======================================================================

def flush_bronze(storage: StorageAdapter, vbuf: list, cbuf: list, state: dict) -> None:
    """Ghi du lieu tho + state len Bronze Storage."""
    if vbuf:
        storage.write_raw_jsonl(
            f"{BRONZE_PREFIX}/video_snapshots/video_snapshots.jsonl", vbuf
        )
    if cbuf:
        storage.write_raw_jsonl(
            f"{BRONZE_PREFIX}/comments/comments.jsonl", cbuf
        )

    if vbuf or cbuf:
        print(f"\n[BRONZE] +{len(vbuf)} videos | +{len(cbuf)} comments -> GCS")
    else:
        print("\nKhong co du lieu moi trong phien nay.")

    storage.write_state_json(STATE_PATH, state)
    print(f"State saved -> {STATE_PATH}")


def run_bronze_pipeline() -> None:
    global _api_attempts, _comment_pages
    _api_attempts = _comment_pages = 0

    storage = StorageAdapter()
    yt      = build("youtube", "v3", developerKey=get_api_key(), cache_discovery=False)
    state   = storage.read_state(STATE_PATH) or {"videos": {}}
    state.setdefault("videos", {})

    vbuf: List[dict] = []
    cbuf: List[dict] = []

    print("=" * 64)
    print(f"BRONZE PIPELINE | RUN {RUN_TS}")
    print(f"Window : [{WINDOW_START.date()} -> {WINDOW_END.date()}) [N-{WINDOW_LOOKBACK_DAYS}d -> N-{WINDOW_END_DAYS}d)")
    print(f"Budget : {TOTAL_PAGE_BUDGET} pages | {MAX_PAGES_PER_VIDEO}/video | {MAX_API_ATTEMPTS} calls")
    print("=" * 64)

    try:
        # Buoc 1: Discovery - quet playlist uploads
        print("\n[DISCOVERY] Uploads Playlist...")
        for cid, cname in CHANNELS.items():
            try:
                items = execute_api(
                    yt.channels().list(part="contentDetails", id=cid)
                ).get("items")
                if not items:
                    continue
                uploads_id = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

                token, stop, added = None, False, 0
                while not stop:
                    resp = execute_api(
                        yt.playlistItems().list(
                            part="contentDetails", playlistId=uploads_id,
                            maxResults=50, pageToken=token,
                        )
                    )
                    for it in resp.get("items", []):
                        pub = it["contentDetails"].get("videoPublishedAt")
                        if not pub:
                            continue
                        pub_dt = parse_iso(pub)
                        if pub_dt >= WINDOW_END:
                            continue
                        if pub_dt < WINDOW_START:
                            stop = True
                            break

                        vid = it["contentDetails"]["videoId"]
                        if vid not in state["videos"]:
                            state["videos"][vid] = {
                                "status":                "active",
                                "published_at":          pub,
                                "channel_id":            cid,
                                "watermark":             None,
                                "latest_comment_ids":    [],
                                "next_comment_mode":     "incremental",
                                "backfill_page_token":   None,
                                "backfill_complete":     False,
                                "comments_disabled":     False,
                            }
                            added += 1
                        else:
                            vs = state["videos"][vid]
                            vs.setdefault("channel_id",          cid)
                            vs.setdefault("latest_comment_ids",  [])
                            vs.setdefault("backfill_complete",   False)
                            vs.setdefault("next_comment_mode",   "incremental")
                            vs.setdefault("comments_disabled",   False)

                    token = resp.get("nextPageToken")
                    if not token:
                        break

                print(f"   [{cname}] +{added} videos moi trong cua so")
            except HttpError as e:
                print(f"   [ERROR] {cname}: {e}")

        # Buoc 2: Finalize video het cua so
        finalized = 0
        for vs in state["videos"].values():
            if vs.get("status") == "active" and parse_iso(vs["published_at"]) < WINDOW_START:
                vs["status"] = "final"
                finalized += 1
        if finalized:
            print(f"   -> Finalized {finalized} video ngoai cua so [N-{WINDOW_LOOKBACK_DAYS}d]")

        active_vids = [v for v, s in state["videos"].items() if s.get("status") == "active"]
        print(f"\nActive videos dang theo doi: {len(active_vids)}")

        # Buoc 3: Fetch metadata video
        print("\n[FETCH VIDEO METADATA]")
        for i in range(0, len(active_vids), 50):
            chunk = active_vids[i : i + 50]
            try:
                resp = execute_api(
                    yt.videos().list(
                        part="snippet,contentDetails,statistics",
                        id=",".join(chunk),
                    )
                )
            except HttpError as e:
                print(f"   [ERROR] videos.list: {e}")
                continue

            returned = set()
            for item in resp.get("items", []):
                vid, record, status = build_video_record(item)
                returned.add(vid)
                vs = state["videos"][vid]
                if status == "skipped_short":
                    vs["status"] = "skipped_short"
                    continue
                if record:
                    vbuf.append(record)

            for vid in set(chunk) - returned:
                state["videos"][vid]["status"] = "unavailable"

        # Buoc 4: Cao comment (Interleaved + Oldest-First)
        process_comments_interleaved(yt, state, cbuf)

    except QuotaExceeded:
        print("\n[STOP] Het Quota API! Dang luu du lieu an toan...")
    except RunBudgetExceeded as e:
        print(f"\n[STOP SAFE] {e}")
    except Exception as e:
        print(f"\n[ERROR] {e}")
        traceback.print_exc()
    finally:
        flush_bronze(storage, vbuf, cbuf, state)
        print(f"\nAPI calls  : {_api_attempts}/{MAX_API_ATTEMPTS}")
        print(f"Trang cmt  : {_comment_pages}/{TOTAL_PAGE_BUDGET}")


if __name__ == "__main__":
    run_bronze_pipeline()
