# A streamlined, privacy-first Streamlit app
# Uses Chrome for Testing + YCS extension using the Chrome for Testing Browser for YouTube
# Uses Tweepy for Twitter

from __future__ import annotations
import os, re, io, time, json, zipfile, hashlib, logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from collections import deque, Counter, defaultdict
import numpy as np

import pandas as pd
import requests
import streamlit as st

import dotenv; dotenv.load_dotenv()

# VADER integration
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
try:
    from textblob import TextBlob
    _HAS_TEXTBLOB = True
except Exception:
    _HAS_TEXTBLOB = False

# X/Twitter API integration 
try:
    import tweepy
    _HAS_TWEEPY = True
except Exception:
    _HAS_TWEEPY = False

# App constants
APP_OUTDIR = Path("./output")
APP_OUTDIR.mkdir(parents=True, exist_ok=True)

FREE_MODELS = [
    "qwen/qwen3-coder:free",
    "meta-llama/llama-4-maverick:free",
    "nousresearch/hermes-3-llama-3.1-405b:free"
]

ETHICS_CODES_FULL = [
    "none","bias","privacy","transparency","accountability",
    "job_displacement","safety","misinformation","governance","other"
]
ETH_SHORT2FULL = {
    "n":"none","b":"bias","p":"privacy","t":"transparency","a":"accountability",
    "j":"job_displacement","s":"safety","m":"misinformation","g":"governance","o":"other"
}
ETH_FULL2SHORT = {v:k for k,v in ETH_SHORT2FULL.items()}

LABEL_TO_NUM = {"negative":-1, "neutral":0, "positive":1}
NUM_TO_LABEL = {-1:"negative", 0:"neutral", 1:"positive"}

logging.basicConfig(level=logging.INFO, format="%(message)s")
LOG = logging.getLogger("app")

def safe_read_csv(path: Path, **kwargs) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path, **kwargs)
    except Exception as e:
        LOG.info(f"[safe_read_csv] failed {path}: {e}")
        return pd.DataFrame()

def _strip_pii(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    pii_cols = ["author","reply_to_author","reply_to_text","authorLink","username","user","handle","link","channel"]
    out = out.drop(columns=[c for c in pii_cols if c in out.columns], errors="ignore")
    if "text" in out.columns:
        out["text"] = out["text"].astype(str)
        out["text"] = out["text"].str.replace(r"@\w+", "[REDACTED]", regex=True)
        out["text"] = out["text"].str.replace(r"https?://\S+", "[URL]", regex=True)
        out["text"] = out["text"].str.replace(r"\S+@\S+", "[EMAIL]", regex=True)
    return out

def _short_numeric_id(source_id: str, text: str) -> int:
    h = hashlib.sha1((str(source_id) + "||" + (text or "")).encode("utf-8")).hexdigest()
    return int(h[:10], 16) % 100000  # anonymized 5-digit to remove any PII

def _ensure_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate an integer from 0 to 99999 and hash it to remove PII
    """
    out = df.copy()

    # start from any existing ids
    if "id" not in out.columns:
        out["id"] = pd.Series([pd.NA] * len(out), dtype="Int64")

    def _needs(i):
        v = out.at[i, "id"]
        try:
            return pd.isna(v) or int(v) == 0
        except Exception:
            return True

    
    for i in range(len(out)):
        if not _needs(i):
            continue
        text = str(out.iloc[i].get("text", "") or "")
        if "comment_id" in out.columns and pd.notna(out.iloc[i].get("comment_id", pd.NA)):
            src = str(out.iloc[i]["comment_id"])
        elif "tweet_id" in out.columns and pd.notna(out.iloc[i].get("tweet_id", pd.NA)):
            src = str(out.iloc[i]["tweet_id"])
        else:
            src = str(pd.util.hash_pandas_object(pd.Series([text]), index=False).iloc[0])
        out.at[i, "id"] = _short_numeric_id(src, text)

    out["id"] = pd.to_numeric(out["id"], errors="coerce").fillna(0).astype(int) % 100000
    return out

def _rekey_parent_id_to_id(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure that the parent_id refers to the parent row's anonymized id, not their comment_id."""
    out = _ensure_id(df)
    # Use anonymized IDs instead of comment ID
    if "comment_id" in out.columns:
        map_key = dict(zip(out["comment_id"].astype(str), out["id"]))
        out["parent_id"] = out.get("parent_id","").astype(str).map(map_key)
    elif "tweet_id" in out.columns:
        map_key = dict(zip(out["tweet_id"].astype(str), out["id"]))
        out["parent_id"] = out.get("parent_id","").astype(str).map(map_key)
    out["parent_id"] = pd.to_numeric(out["parent_id"], errors="coerce").astype("Int64")
    # reorder based on new ids and remove native ids
    cols = ["id"] + [c for c in out.columns if c not in ("id","comment_id","tweet_id")]
    return out[cols]

def _read_text_auto(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8")
    except Exception:
        return p.read_text(encoding="latin-1", errors="ignore")

# YouTube data ingestion using Selenium and YCS-continued

def _default_cft_paths() -> Tuple[str,str]:
    base = Path("./ChromeForTesting")
    win_chrome = base / "chrome-win64" / "chrome.exe"
    win_driver = base / "chromedriver-win64" / "chromedriver.exe"
    if win_chrome.exists() and win_driver.exists():
        return str(win_chrome.resolve()), str(win_driver.resolve())
    mac_chrome = base / "chrome-mac-arm64" / "Chromium.app" / "Contents" / "MacOS" / "Chromium"
    mac_driver = base / "chromedriver-mac-arm64" / "chromedriver"
    if mac_chrome.exists() and mac_driver.exists():
        return str(mac_chrome.resolve()), str(mac_driver.resolve())
    macx_chrome = base / "chrome-mac" / "Chromium.app" / "Contents" / "MacOS" / "Chromium"
    macx_driver = base / "chromedriver-mac-x64" / "chromedriver"
    if macx_chrome.exists() and macx_driver.exists():
        return str(macx_chrome.resolve()), str(macx_driver.resolve())
    lin_chrome = base / "chrome-linux64" / "chrome"
    lin_driver = base / "chromedriver-linux64" / "chromedriver"
    if lin_chrome.exists() and lin_driver.exists():
        return str(lin_chrome.resolve()), str(lin_driver.resolve())
    return "", ""

def unpack_crx(crx_path: Path, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    b = crx_path.read_bytes()
    if b[:4] != b"Cr24":
        try:
            with zipfile.ZipFile(io.BytesIO(b)) as zf:
                zf.extractall(out_dir)
                return out_dir
        except Exception:
            pass
    magic = b"PK\x03\x04"
    i = b.find(magic)
    if i == -1:
        raise RuntimeError("Not a CRX or ZIP")
    with zipfile.ZipFile(io.BytesIO(b[i:])) as zf:
        zf.extractall(out_dir)
    return out_dir

def make_driver(unpacked_ext: Path, chrome_binary: str, chromedriver_path: str, browser_version: str, download_dir: Optional[Path]):
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    opts = Options()
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-popup-blocking")
    opts.add_argument("--start-maximized")

    if not Path(chrome_binary).exists():
        raise RuntimeError(f"Invalid CHROME_BINARY: {chrome_binary}")
    if not Path(chromedriver_path).exists():
        raise RuntimeError(f"Invalid CHROMEDRIVER: {chromedriver_path}")
    opts.binary_location = str(Path(chrome_binary).resolve())

    if browser_version:
        opts.browser_version = browser_version
        opts.add_argument(f"--browser-version={browser_version}")

    ext_path = str(unpacked_ext.resolve())
    opts.add_argument(f"--disable-extensions-except={ext_path}")
    opts.add_argument(f"--load-extension={ext_path}")

    if download_dir:
        dpath = str(download_dir.resolve())
        prefs = {
            "download.default_directory": dpath,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "safebrowsing.disable_download_protection": True,
            "profile.default_content_setting_values.automatic_downloads": 1,
        }
        opts.add_experimental_option("prefs", prefs)

    service = Service(executable_path=str(Path(chromedriver_path).resolve()))
    driver = webdriver.Chrome(service=service, options=opts)
    if download_dir:
        try:
            driver.execute_cdp_cmd("Browser.setDownloadBehavior", {
                "behavior": "allow",
                "downloadPath": str(download_dir.resolve())
            })
        except Exception:
            pass
    return driver

def goto_video(driver, url: str, timeout: int = 45):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    driver.get(url)
    for sel in [
        (By.CSS_SELECTOR, "button[aria-label*='I agree']"),
        (By.XPATH, "//button[contains(., 'I agree') or contains(., 'Accept all')]"),
        (By.XPATH, "//span[contains(., 'Reject all')]/ancestor::button"),
    ]:
        try:
            WebDriverWait(driver, 5).until(EC.element_to_be_clickable(sel)).click()
            break
        except Exception:
            pass
    WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1.title, h1 yt-formatted-string")))

def wait_for_ycs_panel(driver, poll_s: float = 0.5):
    from selenium.webdriver.common.by import By
    probes = [
        (By.XPATH, "//*[contains(., 'YouTube Comment Search')]"),
        (By.XPATH, "//button[normalize-space()='Load all']"),
        (By.XPATH, "//button[.//span[normalize-space()='Load all']]"),
        (By.XPATH, "//button[normalize-space()='save']"),
        (By.XPATH, "//button[normalize-space()='open']"),
        (By.CSS_SELECTOR, "[class*='ycs'], [id*='ycs']"),
    ]
    while True:
        for how, what in probes:
            try:
                if driver.find_elements(how, what):
                    time.sleep(0.8)
                    return
            except Exception:
                pass
        time.sleep(poll_s)

def ycs_click_load_all(driver):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    for xp in ["//button[normalize-space()='Load all']", "//button[.//span[normalize-space()='Load all']]"]:
        try:
            WebDriverWait(driver, 6).until(EC.element_to_be_clickable((By.XPATH, xp))).click()
            break
        except Exception:
            pass

def ycs_wait_and_click_save(driver, max_wait_s: int = 0):
    import time
    start = time.time()
    def _ready() -> bool:
        return bool(driver.execute_script(r"""
            try{
              const app = document.getElementsByClassName('ycs-app')[0];
              if (!app) return 0;
              const st = document.getElementById('ycs_status_cmnt');
              const k = st && st.children && st.children[0] && st.children[0].children
                        && st.children[0].children[0] && st.children[0].children[0].children
                        ? st.children[0].children[0].children.length : 0;
              return k >= 5 ? 1 : 0;
            }catch(e){ return 0; }
        """))
    def _click_save() -> bool:
        return driver.execute_script(r"""
            try{
              const btn = document.getElementById('ycs_save_all_comments');
              if (!btn) return 0;
              try{ btn.scrollIntoView({block:'center'}); }catch(e){}
              btn.click();
              return 1;
            }catch(e){ return 0; }
        """) == 1
    while True:
        if _ready():
            if _click_save():
                time.sleep(0.6)
                return
        if max_wait_s and (time.time() - start) > max_wait_s:
            raise RuntimeError("YCS save not clicked before timeout")
        time.sleep(0.12)

def _prepare_download_dir(d: Path) -> Path:
    d.mkdir(parents=True, exist_ok=True)
    for p in d.glob("*.crdownload"):
        try: p.unlink()
        except Exception: pass
    return d

def wait_for_download(download_dir: Path, start_ts: float) -> Path:
    while True:
        files = [p for p in download_dir.glob("*") if p.is_file()]
        partials = [p for p in files if p.suffix == ".crdownload" and p.stat().st_mtime >= start_ts - 0.5]
        done = [p for p in files if p.suffix != ".crdownload" and p.stat().st_mtime >= start_ts - 0.5]
        if done and not partials:
            return max(done, key=lambda p: p.stat().st_mtime)
        time.sleep(0.3)

_YT_LINK = re.compile(r'(https?://(?:www\.)?youtube\.com/\S+|https?://youtu\.be/\S+)', re.I)

def _skip_blanks(lines, i):
    n=len(lines)
    while i<n and not lines[i].strip(): i+=1
    return i

def _read_until_blank(lines, i):
    res=[]; n=len(lines)
    while i<n and lines[i].strip():
        res.append(lines[i]); i+=1
    return "\n".join(res), i

def _clean_text(s: str) -> str:
    s = re.sub(r'[ \t]+', ' ', s).strip()
    s = re.sub(r'(?m)^\s*Replies:\s*$', '', s)
    return re.sub(r'\n{3,}', '\n\n', s).strip()

def _parse_meta(s: str):
    posted = s.split("|",1)[0].strip()
    likes=0; replies=0
    m1=re.search(r'like:\s*([0-9,]+)',s,re.I)
    m2=re.search(r'repl(?:y|ies):\s*([0-9,]+)',s,re.I)
    if m1: likes=int(m1.group(1).replace(",",""))
    if m2: replies=int(m2.group(1).replace(",",""))
    return posted,likes,replies

def _parse_reply_block(lines, i):
    n=len(lines); assert re.match(r'^\s*-\s*$', lines[i]); i+=1
    i=_skip_blanks(lines,i)
    author=""
    if i<n and lines[i].lstrip().startswith("@"):
        author=lines[i].strip(); i+=1
        if i<n and lines[i].strip().startswith("youtube.com/@"): i+=1
    i=_skip_blanks(lines,i)
    link=""
    if i<n and _YT_LINK.search(lines[i]): link=_YT_LINK.search(lines[i]).group(0); i+=1
    i=_skip_blanks(lines,i)
    posted=""; likes=0
    if i<n and "like:" in lines[i].lower(): posted,likes,_=_parse_meta(lines[i]); i+=1
    i=_skip_blanks(lines,i)
    body,i=_read_until_blank(lines,i); body=_clean_text(body)
    return dict(author=author,text=body,likes=likes,link=link,replies=[],time=posted), i

def _parse_comment_block(lines, i):
    n=len(lines)
    author = lines[i].strip() if i<n else ""
    i+=1
    if i<n and lines[i].strip().startswith("youtube.com/@"): i+=1
    i=_skip_blanks(lines,i)
    link=""
    if i<n and _YT_LINK.search(lines[i]): link=_YT_LINK.search(lines[i]).group(0); i+=1
    i=_skip_blanks(lines,i)
    posted=""; likes=0; replies=0
    if i<n and "like:" in lines[i].lower():
        posted,likes,replies=_parse_meta(lines[i]); i+=1
    i=_skip_blanks(lines,i)
    body,i=_read_until_blank(lines,i); body=_clean_text(body)
    repl=[]
    i=_skip_blanks(lines,i)
    while i<n and re.match(r'^\s*-\s*$', lines[i]):
        r,i=_parse_reply_block(lines,i); repl.append(r)
        i=_skip_blanks(lines,i)
    return dict(author=author,text=body,likes=likes,link=link,replies=repl,time=posted), i

def _parse_plain(s: str) -> List[dict]:
    lines=[ln.rstrip("\n") for ln in s.splitlines()]
    i=_skip_blanks(lines,0)
    out=[]
    while i<len(lines):
        try:
            e,i=_parse_comment_block(lines,i)
            out.append(e)
        except Exception:
            i+=1
        i=_skip_blanks(lines,i)
    return out

def parse_ycs_save_txt(s: str) -> List[dict]:
    import re
    def _uurl(u: str) -> str:
        u = (u or "").strip()
        if not u: return ""
        return u if u.startswith("http") else "https://" + u
    def _cid(url: str) -> str:
        m = re.search(r'[?&#]lc=([A-Za-z0-9_.\-]+)', url or "")
        return m.group(1) if m else ""
    def _body(block: str) -> str:
        lines = block.splitlines()
        keep = []
        for ln in lines:
            t = ln.strip()
            if not t:
                keep.append(ln); continue
            if t in ("[COMMENT]","[REPLY]","Replies:"): continue
            if t.startswith("youtube.com/@"): continue
            if "youtube.com/watch" in t: continue
            if re.search(r'\blike:\s*\d+', t, flags=re.I): continue
            if re.search(r'\breply:\s*\d+', t, flags=re.I): continue
            if re.search(r'^\d+.*ago\b', t, flags=re.I) or "edited)" in t: continue
            if t.startswith("YCS -") or t.startswith("Comments File created"): continue
            keep.append(ln)
        out = "\n".join(keep).strip()
        out = re.sub(r'^\s*@\S+\s*\n', '', out).strip()
        return out
    def _parse_block(block: str) -> dict:
        user_m = re.search(r'@([^\n]+)', block)
        user = f"@{user_m.group(1).strip()}" if user_m else ""
        ch_m = re.search(r'(?:https?://)?youtube\.com/@[^\n]+', block)
        channel = _uurl(ch_m.group(0)) if ch_m else ""
        url_m = re.search(r'(?:https?://)?youtube\.com/watch[^\n]+', block)
        url = _uurl(url_m.group(0)) if url_m else ""
        time_m = re.search(r'(\d[^\n]*?ago|edited\))', block, flags=re.I)
        time_s = time_m.group(1).strip() if time_m else ""
        likes_m = re.search(r'like:\s*(\d+)', block, flags=re.I)
        likes = int(likes_m.group(1)) if likes_m else 0
        text = _body(block)
        return {"author": user,"authorLink":channel,"link":url,"time":time_s,"likes":likes,"id":_cid(url),"text":text}
    s = s.replace("\r\n", "\n").replace("\r", "\n").replace('\\"', '"').strip()
    sections = [sec.strip() for sec in re.split(r'(?m)^\s*#{3,}\s*$', s) if sec.strip()]
    out: List[dict] = []
    for sec in sections:
        if not sec.startswith("[COMMENT]"):
            continue
        parts = re.split(r'\n\[REPLY\]\n', sec)
        head = parts[0]
        comment = _parse_block(head)
        comment["replies"] = []
        for rep in parts[1:]:
            comment["replies"].append(_parse_block(rep))
        out.append(comment)
    return out

def parse_ycs_export(raw: str) -> pd.DataFrame:
    try:
        j = json.loads(raw)
        items = j if isinstance(j, list) else j.get("items") or j.get("comments") or []
    except Exception:
        if ("YCS - YouTube Comment Search" in raw) or ("#####" in raw) or ("[COMMENT]" in raw):
            items = parse_ycs_save_txt(raw)
        else:
            items = _parse_plain(raw)
    def _text(e): return (e.get("text") or e.get("contentText") or "").strip()
    def _num(x):
        try: return int(x)
        except Exception:
            try: return int(str(x).replace(",",""))
            except Exception: return 0
    def _flatten(e, parent_row=None):
        row = dict(
            platform="youtube",
            comment_id=e.get("id",""),
            parent_id=parent_row["comment_id"] if parent_row else None,
            is_reply=bool(parent_row),
            text=_text(e),
            likes=_num(e.get("likes") or e.get("likeCount")),
            posted=(e.get("time") or e.get("publishedTimeText") or e.get("posted") or "")
        )
        yield row
        for r in e.get("replies") or []:
            if isinstance(r, dict):
                yield from _flatten(r, row)
    rows=[]
    for it in items:
        if isinstance(it, dict):
            rows.extend(list(_flatten(it, None)))
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.drop_duplicates(subset=["text"], keep="first").reset_index(drop=True)
    df["likes"] = pd.to_numeric(df["likes"], errors="coerce").fillna(0).astype(int)
    df["text"] = df["text"].fillna("").astype(str)
    return _strip_pii(df)

def run_youtube(urls: List[str], crx_path: Path, chrome_binary: str, chromedriver: str, cft_version: str, outdir: Path) -> pd.DataFrame:
    ext_dir = unpack_crx(crx_path, outdir / "ycs_unpacked")
    dl_dir = _prepare_download_dir(outdir / "ycs_downloads")
    driver = make_driver(ext_dir, chrome_binary, chromedriver, cft_version, dl_dir)
    all_rows=[]
    try:
        for u in urls:
            goto_video(driver, u)
            wait_for_ycs_panel(driver)
            ycs_click_load_all(driver)
            ycs_wait_and_click_save(driver, max_wait_s=0)
            t0 = time.time()
            # wait for the text file download to start
            started = False
            for _ in range(25):
                if any(p.suffix == ".crdownload" for p in dl_dir.glob("*")):
                    started = True; break
                if any(p.is_file() and p.suffix != ".crdownload" and p.stat().st_mtime >= t0 - 0.5 for p in dl_dir.glob("*")):
                    started = True; break
                time.sleep(0.25)
            if not started:
                raise RuntimeError("YCS save did not start")
            fpath = wait_for_download(dl_dir, start_ts=t0)
            raw = _read_text_auto(fpath)
            try: fpath.unlink()
            except Exception: pass
            df = parse_ycs_export(raw)
            if df.empty:
                continue
            df["video"] = u
            df = _rekey_parent_id_to_id(_ensure_id(df))
            all_rows.append(df)
    finally:
        try: driver.quit()
        except Exception: pass
    if not all_rows:
        return pd.DataFrame()
    out = pd.concat(all_rows, ignore_index=True)
    # hide all native ids within the saved CSV
    out_priv = out[[c for c in out.columns if c not in ("comment_id","tweet_id")]]
    (outdir / "youtube.csv").write_text(out_priv.to_csv(index=False), encoding="utf-8")
    return out_priv

# X/Twitter data ingestion
def _x_client(bearer_token: str):
    if not _HAS_TWEEPY:
        raise RuntimeError("tweepy not installed")
    if not bearer_token:
        raise RuntimeError("X Bearer token missing")
    return tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)

def fetch_tweets_official(query: str, limit: int, scope: str, start_date: Optional[str], end_date: Optional[str], bearer: str) -> pd.DataFrame:
    client = _x_client(bearer)
    fields = ["created_at", "public_metrics", "referenced_tweets"]
    exp = []
    use_all = (scope or "recent").lower() == "all"
    def _to_iso8601(s: Optional[str]) -> Optional[str]:
        from datetime import datetime, timezone
        if not s: return None
        try:
            if len(s) == 10:
                dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            else:
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None
        return dt.isoformat().replace("+00:00", "Z")
    start_iso = _to_iso8601(start_date)
    end_iso   = _to_iso8601(end_date)

    remaining = limit
    next_token = None
    rows = []
    while remaining > 0:
        if use_all:
            resp = client.search_all_tweets(
                query=query, max_results=min(100, remaining),
                tweet_fields=fields, expansions=exp,
                start_time=start_iso, end_time=end_iso,
                next_token=next_token)
        else:
            resp = client.search_recent_tweets(
                query=query, max_results=min(100, remaining),
                tweet_fields=fields, expansions=exp,
                next_token=next_token)
        meta = getattr(resp, "meta", {}) or {}
        data = getattr(resp, "data", []) or []
        for tw in data:
            parent_id = None
            pid = getattr(tw,"referenced_tweets",None) or []
            for x in pid:
                if getattr(x,"type","")=="replied_to":
                    parent_id = str(getattr(x,"id",""))
            m = getattr(tw, "public_metrics", {}) or {}
            likes = int(m.get("like_count", 0) or 0)
            rows.append(dict(
                platform="twitter",
                tweet_id=str(getattr(tw, "id", "")),
                parent_id=str(parent_id) if parent_id else None,
                is_reply=bool(parent_id),
                text=getattr(tw, "text", "") or "",
                likes=likes,
                link="",
                posted=str(getattr(tw, "created_at", "")),
                video=query,
            ))
        remaining -= len(data)
        next_token = meta.get("next_token")
        if not next_token:
            break
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["reply_to_author"] = ""
    df["reply_to_text"] = ""
    df["author"] = ""
    df = _strip_pii(df)
    df = _rekey_parent_id_to_id(_ensure_id(df))
    # remove native ids within the saved CSV
    return df[[c for c in df.columns if c not in ("comment_id","tweet_id")]]

# Scoring data

def label_from_vader(x: float) -> str:
    return "positive" if x >= 0.05 else ("negative" if x <= -0.05 else "neutral")  # per VADER validation thresholds :contentReference[oaicite:11]{index=11}

def score_sentiment(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = _rekey_parent_id_to_id(_ensure_id(df))
    sia = SentimentIntensityAnalyzer()
    out["vader"] = out["text"].apply(lambda t: sia.polarity_scores(t or "")["compound"])
    if _HAS_TEXTBLOB:
        out["textblob"] = out["text"].apply(lambda t: TextBlob(t or "").sentiment.polarity)
        out["score"] = (out["vader"] + out["textblob"]) / 2.0
    else:
        out["textblob"] = None
        out["score"] = out["vader"]
    out["vader_label"] = out["vader"].apply(label_from_vader)
    out["label_num"] = out["vader_label"].map(LABEL_TO_NUM)
    return out

# OpenRouter API
_API_CALLS_PLANNED = 0
_API_CALLS_MADE = 0
_API_CACHE_HITS = 0
_DEBUG_BUFFER: List[str] = []
_LLM_RATELIMIT_TIMES = deque()

def _debug(msg: str):
    pass

def _llm_rate_limit_wait(rpm: int):
    rpm = max(1, int(rpm))
    window = 60.0
    now = time.monotonic()
    while _LLM_RATELIMIT_TIMES and now - _LLM_RATELIMIT_TIMES[0] > window:
        _LLM_RATELIMIT_TIMES.popleft()
    if len(_LLM_RATELIMIT_TIMES) >= rpm:
        sleep_s = window - (now - _LLM_RATELIMIT_TIMES[0]) + 0.05
        _debug(f"[rate-limit] sleeping {sleep_s:.2f}s")
        time.sleep(max(0.0, sleep_s))
    _LLM_RATELIMIT_TIMES.append(time.monotonic())

def _retry_wait_from_headers(h: Dict[str,str]) -> float:
    ra = h.get("Retry-After")
    if ra:
        try: return float(ra)
        except Exception: pass
    reset = h.get("X-RateLimit-Reset")
    if reset:
        try:
            v = float(reset)
            if v > 1e12: v = v / 1000.0
            return max(0.0, v - time.time())
        except Exception:
            pass
    return 3.0

def _openrouter_request(model: str, messages: List[Dict[str,str]], api_key: str, temperature: float, max_tokens: int) -> str:
    global _API_CALLS_MADE
    url = os.environ.get("OPENROUTER_URL", "https://openrouter.ai/api/v1/chat/completions")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {"model": model, "messages": messages, "temperature": float(temperature), "max_tokens": int(max_tokens)}
    body = json.dumps(payload)
    _debug(f"[openrouter] POST {url} model={model} bytes={len(body)} max_tokens={max_tokens}")
    max_attempts = 5
    base_timeout = 35
    for attempt in range(1, max_attempts+1):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=base_timeout)
            _API_CALLS_MADE += 1
            _debug(f"[openrouter] status={r.status_code} len={len(r.text)} attempt={attempt}")
            _debug(f"[openrouter] headers: { {k:v for k,v in list(r.headers.items())[:8]} }")
        except requests.RequestException as e:
            _debug(f"[openrouter] network error: {e}")
            time.sleep(min(12.0, 1.5 * attempt))
            if attempt == max_attempts: return ""
            continue
        if 200 <= r.status_code < 300:
            try:
                j = r.json()
                return j["choices"][0]["message"]["content"]
            except Exception:
                _debug("[openrouter] JSON parse failed, returning raw text")
                return r.text or ""
        retry_after = _retry_wait_from_headers({k:v for k,v in r.headers.items()})
        if r.status_code == 429:
            _debug(f"[openrouter] 429; sleeping {retry_after:.2f}s")
            time.sleep(max(2.0, retry_after)); continue
        if 500 <= r.status_code < 600:
            _debug(f"[openrouter] {r.status_code}; sleeping {min(10.0, retry_after):.2f}s")
            time.sleep(max(2.0, min(10.0, retry_after))); continue
        _debug(f"[openrouter] error HTTP_{r.status_code}: {r.text[:2000]}")
        return r.text or f"HTTP_{r.status_code}"
    return ""

def _norm_label_short(x: str) -> str:
    x = (x or "").strip().lower()
    if x in {"-","neg","negative"}: return "negative"
    if x in {"+","pos","positive"}: return "positive"
    return "neutral"

def _norm_score(x: str, lab: str) -> float:
    try: v = float(str(x).strip())
    except Exception: v = 0.0
    if lab == "positive" and v < 0: v = abs(v)
    elif lab == "negative" and v > 0: v = -abs(v)
    elif lab == "neutral": v = 0.0
    return max(-1.0, min(1.0, v))

def _norm_sarcasm(x: str) -> bool:
    t = (str(x) if x is not None else "").strip().lower()
    return t in {"1","y","yes","true"}

def _norm_ethics_short(s: str) -> str:
    if not s: return "none"
    items = []
    for ch in str(s).lower().replace(" ", "").split(","):
        ch = ch.strip()
        if not ch: continue
        items.append(ETH_SHORT2FULL.get(ch, ch))
    items = [z for z in items if z in ETHICS_CODES_FULL]
    return ",".join(sorted(set(items))) if items else "none"

def _chunk_rows(todo: pd.DataFrame, header: str, max_prompt_chars: int) -> List[str]:
    """
    Generate PII removed rows based on the StreamLit UI
    """
    if todo is None or todo.empty:
        return []

    n = len(todo)
    budget = max(2000, int(max_prompt_chars) - len(header))
    per_row_cap = max(1200, min(6000, budget // max(1, min(n, 120))))

    lines = []
    for r in todo.itertuples(index=False):
        tid = int(getattr(r, "id"))
        txt = (getattr(r, "text", "") or "").replace("\n", " ").strip()
        # extra non-PII context helps sentiment without leaking identities
        likes = int(getattr(r, "likes", 0) or 0)
        is_reply = 1 if bool(getattr(r, "is_reply", False)) else 0
        posted = str(getattr(r, "posted", "") or "")[:40]
        # trim text by budget after adding small context
        free_cap = max(200, per_row_cap - 60)  # reserve for id + fields
        txt = txt[:free_cap]
        lines.append(f"{tid}:: reply={is_reply}; likes={likes}; posted='{posted}'; text={txt}\n")

    body = "".join(lines)
    if len(header) + len(body) <= max_prompt_chars:
        return [header + body]

    chunks = []
    cur = header
    for ln in lines:
        if len(cur) + len(ln) > max_prompt_chars:
            chunks.append(cur)
            cur = header + ln
        else:
            cur += ln
    if cur.strip():
        chunks.append(cur)
    return chunks

def run_llm_batch(
    df: pd.DataFrame,
    *,
    model: str,
    api_key: str,
    rpm: int,
    max_prompt_chars: int,
    toks_per_row: int,
    outdir: str,
    progress_cb: Callable[[Dict[str, Any]], None] | None = None,
) -> pd.DataFrame:
    """
    Batch LLM labeling and integration 
    """
    import hashlib, re, json, os

    if df is None or df.empty:
        return pd.DataFrame(columns=["id","llm_label","llm_score","llm_sarcasm","llm_ethics","cache_sig","model"])

    def _hash_sig(text: str) -> str:
        t = (text or "").replace("\r", " ").replace("\n", " ").strip()
        return hashlib.sha1(t.encode("utf-8", "ignore")).hexdigest()[:12]

    def _vendor_alias(mid: str) -> str:
        try:
            return (mid.split("/", 1)[1].split(":", 1)[0]).split("-", 1)[0].strip().lower()
        except Exception:
            return re.sub(r"[^a-z0-9]+", "", mid.lower())

    def _emit(i: int, n: int):
        if not progress_cb: return
        progress_cb({"planned": planned_calls, "made": _API_CALLS_MADE, "cache": _API_CACHE_HITS, "chunk": (i, n), "debug": list(_DEBUG_BUFFER)})

    def _write_csv(path: str, df_out: pd.DataFrame):
        df_out = df_out.sort_values("id")
        df_out.to_csv(path, index=False)

    base = df.copy()
    if "id" not in base.columns:
        base = base.reset_index().rename(columns={"index": "id"})
    base["id"] = pd.to_numeric(base["id"], errors="coerce").fillna(0).astype(int)
    base["text"] = base["text"].astype(str).fillna("")
    base["cache_sig"] = base["text"].map(_hash_sig)

    # paths
    os.makedirs(outdir, exist_ok=True)
    safe = model.replace("/", "_").replace(":", "_")
    out_path = os.path.join(outdir, f"labels_{safe}.csv")

    # prior cache file (id, cache_sig)
    prior = pd.read_csv(out_path) if os.path.exists(out_path) else pd.DataFrame()
    need_cols = {"id","llm_label","llm_score","llm_sarcasm","llm_ethics","cache_sig"}
    for c in need_cols:
        if c not in prior.columns:
            prior[c] = [] if c != "id" else pd.Series([], dtype="int64")

    have = set(zip(prior["id"].astype(int), prior["cache_sig"].astype(str)))
    mask = [(int(i), str(s)) not in have for i, s in zip(base["id"], base["cache_sig"])]
    todo = base.loc[mask, ["id","text","cache_sig"]]

    # prompt
    ETH_CODES = ["bias","privacy","transparency","job_displacement","safety","none"]
    sys_msg = (
        "You are a labeling engine. Return ONLY machine-readable output.\n"
        "Task: For each line, classify sentiment label, score, sarcasm, and ethics.\n"
        "Output one CSV line per input row exactly:\n"
        "id|label|score|sarcasm|ethics\n"
        "- label in {positive, negative, neutral}\n"
        "- score in [-1,1]\n"
        "- sarcasm 1 or 0\n"
        f"- ethics subset of {','.join(ETH_CODES)} or none\n"
        "No commentary. No markdown."
    )
    if _vendor_alias(model) == "deepseek":
        sys_msg += "\nNever write paragraphs. Output CSV only."

    def _build_chunks(frame: pd.DataFrame) -> list[tuple[list[int], str]]:
        if frame.empty:
            return []
        # build id<TAB>text lines
        lines = [f'{int(r.id)}\t{(r.text or "").replace(chr(10)," ").replace(chr(13)," ")[:4000]}' for r in frame.itertuples(index=False)]
        chunks = []
        buf_ids, buf_lines, buf_len = [], [], 0
        budget = max(2000, int(max_prompt_chars) - len(sys_msg) - 128)
        for rid, line in zip(frame["id"].tolist(), lines):
            add = len(line) + 1
            if buf_len and buf_len + add > budget:
                user = "Classify id<TAB>text. Return one CSV line per input row in the same order.\n\n" + "\n".join(buf_lines)
                chunks.append((buf_ids, user))
                buf_ids, buf_lines, buf_len = [], [], 0
            buf_ids.append(rid); buf_lines.append(line); buf_len += add
        if buf_ids:
            user = "Classify id<TAB>text. Return one CSV line per input row in the same order.\n\n" + "\n".join(buf_lines)
            chunks.append((buf_ids, user))
        return chunks

    lab_map = {"-":"negative","+":"positive","n":"neutral","neg":"negative","pos":"positive"}
    def _norm_label(x: str) -> str:
        t = (x or "").strip().lower()
        return lab_map.get(t, "positive" if t.startswith("pos") else "negative" if t.startswith("neg") else "neutral")
    def _norm_score_val(s: str, lab: str) -> float:
        try: v = float(str(s).strip())
        except Exception: v = 0.0
        if lab == "neutral": return 0.0
        return float(max(-1.0, min(1.0, v)))
    def _norm_bool(s: str) -> bool:
        t = (str(s) if s is not None else "").strip().lower()
        return t in {"1","y","yes","true"}
    def _norm_ethics(s: str) -> str:
        if not s: return "none"
        toks = [z.strip().lower() for z in str(s).replace(" ", "").split(",") if z.strip()]
        toks = [z for z in toks if z in set(ETH_CODES)]
        return ",".join(sorted(set(toks))) if toks else "none"

    def _parse_block(text: str) -> dict[int, dict]:
        raw = (text or "").strip()
        out: dict[int, dict] = {}

        if raw.startswith("{"):
            try:
                j = json.loads(raw)
                for k, v in j.items():
                    rid = int(re.sub(r"[^\d]", "", str(k)) or 0)
                    lab = _norm_label(v.get("L",""))
                    out[rid] = {
                        "id": rid,
                        "llm_label": lab,
                        "llm_score": _norm_score_val(v.get("S",0), lab),
                        "llm_sarcasm": bool(v.get("Z",0)) or _norm_bool(v.get("sarcasm",0)),
                        "llm_ethics": _norm_ethics(v.get("E","")),
                    }
                return out
            except Exception:
                pass

        for line in raw.splitlines():
            line = line.strip().strip("#").strip()
            if not line or not re.match(r"^\d+", line): 
                continue
            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 5:
                if len(parts) == 4: parts.append("none")
                else: continue
            try:
                rid = int(re.sub(r"[^\d]", "", parts[0]))
            except Exception:
                continue
            lab = _norm_label(parts[1])
            out[rid] = {
                "id": rid,
                "llm_label": lab,
                "llm_score": _norm_score_val(parts[2], lab),
                "llm_sarcasm": _norm_bool(parts[3]),
                "llm_ethics": _norm_ethics(parts[4]),
            }
        return out

    planned_calls = 0
    df_out = prior.copy()
    if todo.empty:
        _debug("[cache] nothing to do; returning prior")
        df_out["model"] = model
        return df_out[["id","llm_label","llm_score","llm_sarcasm","llm_ethics","cache_sig","model"]]

    chunks = _build_chunks(todo)
    planned_calls = len(chunks)
    _emit(0, planned_calls)

    def _request(ids: list[int], user_msg: str) -> dict[int, dict]:
        _llm_rate_limit_wait(int(rpm))
        max_tokens = max(64, int(toks_per_row) * max(1, len(ids)))
        messages = [{"role":"system","content":sys_msg},{"role":"user","content":user_msg}]
        reply = _openrouter_request(model, messages, api_key, 0.0, max_tokens)
        return _parse_block(reply)

    for idx, (ids, user_msg) in enumerate(chunks, start=1):
        parsed = _request(ids, user_msg)
        missing = [rid for rid in ids if rid not in parsed]
        if missing:
            frame = todo.set_index("id").loc[missing]
            sub_chunks = _build_chunks(frame.reset_index()[["id","text","cache_sig"]])
            passes = 0
            for _ids, _user in sub_chunks:
                if not missing: break
                passes += 1
                got = _request(_ids, _user)
                parsed.update(got)
                missing = [rid for rid in ids if rid not in parsed]
                if passes >= 2 and missing:
                    break

        # update any missing ids
        for rid in ids:
            if rid not in parsed:
                parsed[rid] = {"id": rid, "llm_label": "neutral", "llm_score": 0.0, "llm_sarcasm": False, "llm_ethics": "none"}

        rows = [parsed[rid] for rid in ids]
        chunk_df = pd.DataFrame(rows)
        sig_map = {int(r.id): str(r.cache_sig) for r in todo.itertuples(index=False)}
        chunk_df["cache_sig"] = chunk_df["id"].map(sig_map).astype(str)
        chunk_df["model"] = model

        if not df_out.empty:
            pre = set(zip(df_out["id"].astype(int), df_out["cache_sig"].astype(str)))
            new_rows = [r for r in chunk_df.itertuples(index=False) if (int(r.id), str(r.cache_sig)) not in pre]
            chunk_df = pd.DataFrame(new_rows, columns=chunk_df.columns)

        df_out = pd.concat([df_out, chunk_df], ignore_index=True)
        _write_csv(out_path, df_out[["id","llm_label","llm_score","llm_sarcasm","llm_ethics","cache_sig"]])
        _emit(idx, planned_calls)

    df_out["model"] = model
    return df_out[["id","llm_label","llm_score","llm_sarcasm","llm_ethics","cache_sig","model"]]

# Evaluting metrics on a per-model and per-platform basis
def _confusion(y_true: List[str], y_pred: List[str], labels=("negative","neutral","positive")) -> pd.DataFrame:
    idx = {lab:i for i,lab in enumerate(labels)}
    m = [[0]*len(labels) for _ in labels]
    for a,b in zip(y_true, y_pred):
        if a not in idx or b not in idx: continue
        m[idx[a]][idx[b]] += 1
    return pd.DataFrame(m, index=[f"T:{l}" for l in labels], columns=[f"P:{l}" for l in labels])

def _prf1(y_true: List[str], y_pred: List[str], labels=("negative","neutral","positive")) -> Tuple[float,float,float]:
    eps=1e-9
    precs=[]; recs=[]
    for lab in labels:
        tp = sum(1 for a,b in zip(y_true,y_pred) if a==lab and b==lab)
        fp = sum(1 for a,b in zip(y_true,y_pred) if a!=lab and b==lab)
        fn = sum(1 for a,b in zip(y_true,y_pred) if a==lab and b!=lab)
        prec = tp / (tp + fp + eps)
        rec  = tp / (tp + fn + eps)
        precs.append(prec); recs.append(rec)
    macro_p = sum(precs)/len(precs)
    macro_r = sum(recs)/len(recs)
    macro_f1 = 2*macro_p*macro_r / (macro_p+macro_r+eps)
    return macro_p, macro_r, macro_f1

def _weighted_f1(y_true: List[str], y_pred: List[str], labels=("negative","neutral","positive")) -> float:
    eps=1e-9
    total=len(y_true)
    f1s=[]; weights=[]
    for lab in labels:
        tp = sum(1 for a,b in zip(y_true,y_pred) if a==lab and b==lab)
        fp = sum(1 for a,b in zip(y_true,y_pred) if a!=lab and b==lab)
        fn = sum(1 for a,b in zip(y_true,y_pred) if a==lab and b!=lab)
        prec = tp / (tp + fp + eps)
        rec  = tp / (tp + fn + eps)
        f1 = 2*prec*rec / (prec+rec+eps)
        w = sum(1 for a in y_true if a==lab)
        f1s.append(f1); weights.append(w)
    return float(sum(f*w for f,w in zip(f1s,weights)) / (sum(weights)+eps))

def _accuracy(y_true: List[str], y_pred: List[str]) -> float:
    n = len(y_true)
    if n == 0: return 0.0
    eq = sum(1 for a,b in zip(y_true, y_pred) if a == b)
    return eq / n

def _cohen_kappa(y_true: List[str], y_pred: List[str], labels=("negative","neutral","positive")) -> float:
    # κ = (po - pe) / (1 - pe)
    n = len(y_true)
    if n == 0: return 0.0
    po = _accuracy(y_true,y_pred)
    from collections import Counter
    ct = Counter(y_true)
    cp = Counter(y_pred)
    pe = sum((ct[l]/n)*(cp[l]/n) for l in labels)
    if pe == 1.0: return 0.0
    return (po - pe) / (1 - pe + 1e-9)

def _mcc_multiclass(y_true: List[str], y_pred: List[str], labels=("negative","neutral","positive")) -> float:
    k=len(labels); C=[[0]*k for _ in range(k)]; idx={l:i for i,l in enumerate(labels)}
    for a,b in zip(y_true,y_pred):
        if a in idx and b in idx:
            C[idx[a]][idx[b]] += 1
    t=sum(C[i][i] for i in range(k))
    p=[sum(C[i][j] for j in range(k)) for i in range(k)]
    q=[sum(C[j][i] for j in range(k)) for i in range(k)]
    n=sum(p)
    s=sum(p[i]*q[i] for i in range(k))
    t_sum=sum(p[i] for i in range(k))
    denom=((t_sum**2 - sum(p_i**2 for p_i in p))*(t_sum**2 - sum(q_i**2 for q_i in q)))**0.5
    if denom == 0: return 0.0
    return (t*n - s) / (denom + 1e-9)

def _spearman(x: List[float], y: List[float]) -> float:
    import math
    if not x or not y or len(x)!=len(y): return 0.0
    n=len(x)
    rx=pd.Series(x).rank(method="average")
    ry=pd.Series(y).rank(method="average")
    cov=((rx - rx.mean())*(ry - ry.mean())).sum()/n
    stdx=rx.std(); stdy=ry.std()
    if stdx==0 or stdy==0: return 0.0
    return float(cov/(stdx*stdy))

def _standardize_llm_df(llm_df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize LLM scores
    """
    if llm_df is None or llm_df.empty:
        return pd.DataFrame(columns=["id", "llm_label", "llm_score"])

    df = llm_df.copy()

    # id
    if "id" not in df.columns:
        # try common key-id pairs, otherwise hash ids
        if "comment_id" in df.columns:
            df["id"] = pd.to_numeric(df["comment_id"], errors="coerce").fillna(0).astype(int) % 100000
        elif "tweet_id" in df.columns:
            df["id"] = pd.to_numeric(df["tweet_id"], errors="coerce").fillna(0).astype(int) % 100000
        else:
            base = df.get("text", "").fillna("").astype(str)
            df["id"] = pd.util.hash_pandas_object(base, index=False).astype(int) % 100000
    df["id"] = pd.to_numeric(df["id"], errors="coerce").fillna(0).astype(int) % 100000

    # label
    lbl_col = next((c for c in ["llm_label", "label", "prediction", "pred", "L"] if c in df.columns), None)
    if lbl_col is None:
        df["llm_label"] = "neutral"
    else:
        v = df[lbl_col].astype(str).str.strip().str.lower()
        norm = {
            "-": "negative", "neg": "negative", "negative": "negative",
            "0": "neutral", "neutral": "neutral",
            "+": "positive", "pos": "positive", "positive": "positive",
        }
        df["llm_label"] = v.map(norm).fillna("neutral")

    # score
    scr_col = next((c for c in ["llm_score", "score", "S", "polarity", "sentiment"] if c in df.columns), None)
    if scr_col is None:
        s = pd.Series(0.0, index=df.index, dtype=float)
    else:
        s = pd.to_numeric(df[scr_col], errors="coerce").fillna(0.0)

    def _align(row):
        lab = row["llm_label"]
        val = float(row["raw_score"])
        if lab == "neutral":
            return 0.0
        if lab == "positive":
            return abs(val)
        return -abs(val)  # negative
    df = df.assign(raw_score=s)
    df["llm_score"] = df.apply(_align, axis=1).clip(-1.0, 1.0)
    return df[["id", "llm_label", "llm_score"]].copy()

def evaluate_model(base_scored: pd.DataFrame, llm_df: pd.DataFrame) -> Dict[str, Any]:
    # Standardize incoming LLM labels to required columns
    llm_norm = _standardize_llm_df(llm_df)

    merged = base_scored[["id", "platform", "vader_label", "vader"]].merge(
        llm_norm[["id", "llm_label", "llm_score"]], on="id", how="inner"
    )
    if merged.empty:
        return {"empty": True}

    y_true = list(merged["vader_label"])
    y_pred = list(merged["llm_label"])

    acc = _accuracy(y_true, y_pred)
    mp, mr, mf1 = _prf1(y_true, y_pred)
    wf1 = _weighted_f1(y_true, y_pred)
    kappa = _cohen_kappa(y_true, y_pred)
    mcc = _mcc_multiclass(y_true, y_pred)
    rho = _spearman(list(merged["vader"]), list(merged["llm_score"]))
    cm = _confusion(y_true, y_pred)

    per_platform = []
    for plat, g in merged.groupby("platform"):
        y_t = list(g["vader_label"]); y_p = list(g["llm_label"])
        per_platform.append({
            "platform": plat,
            "n": len(g),
            "accuracy": _accuracy(y_t, y_p),
            "macro_f1": _prf1(y_t, y_p)[2],
            "weighted_f1": _weighted_f1(y_t, y_p),
            "kappa": _cohen_kappa(y_t, y_p),
            "mcc": _mcc_multiclass(y_t, y_p),
            "spearman_rho": _spearman(list(g["vader"]), list(g["llm_score"])),
        })

    return {
        "empty": False,
        "n": len(merged),
        "accuracy": acc,
        "macro_precision": mp,
        "macro_recall": mr,
        "macro_f1": mf1,
        "weighted_f1": wf1,
        "kappa": kappa,
        "mcc": mcc,
        "spearman_rho": rho,
        "confusion": cm,
        "per_platform": per_platform,
    }

# StreamLit UI
st.set_page_config(page_title="Social Media Sentiment Analysis Tool", layout="wide")
if "df_youtube" not in st.session_state: st.session_state.df_youtube = pd.DataFrame()
if "df_twitter" not in st.session_state: st.session_state.df_twitter = pd.DataFrame()
if "df_scored"  not in st.session_state: st.session_state.df_scored  = pd.DataFrame()
if "llm_labels" not in st.session_state: st.session_state.llm_labels = {}   # model -> df
if "df_all"     not in st.session_state: st.session_state.df_all     = pd.DataFrame()
if "_hydrated"  not in st.session_state: st.session_state._hydrated  = False

def _load_first(*candidates: Path) -> pd.DataFrame:
    for p in candidates:
        if p.exists() and p.stat().st_size > 0:
            return safe_read_csv(p)
    return pd.DataFrame()

def _hydrate_session_from_disk():
    if st.session_state._hydrated:
        return
    # load output folder first
    yt = _load_first(APP_OUTDIR/"youtube.csv", Path("youtube.csv"))
    tw = _load_first(APP_OUTDIR/"twitter.csv", Path("twitter.csv"))
    sc = _load_first(APP_OUTDIR/"comments_scored.csv", Path("comments_scored.csv"))

    if not yt.empty: st.session_state.df_youtube = yt
    if not tw.empty: st.session_state.df_twitter = tw

    if not sc.empty:
        # scored value must be prioritized
        st.session_state.df_scored = sc
        base_frames = [df for df in (yt, tw) if not df.empty]
        if base_frames:
            st.session_state.df_all = _rekey_parent_id_to_id(_ensure_id(pd.concat(base_frames, ignore_index=True)))
    else:
        base_frames = [df for df in (yt, tw) if not df.empty]
        if base_frames:
            st.session_state.df_all = _rekey_parent_id_to_id(_ensure_id(pd.concat(base_frames, ignore_index=True)))

    # load any saved LLM label files
    for p in list((APP_OUTDIR).glob("labels_*.csv")) + list(Path(".").glob("labels_*.csv")):
        d = safe_read_csv(p)
        if not d.empty:
            key = p.stem[len("labels_"):]
            st.session_state.llm_labels[key] = d

    st.session_state._hydrated = True

_hydrate_session_from_disk()

def _vendor_alias(mid: str) -> str:
    s = str(mid)
    if "/" in s:
        s = s.split("/", 1)[1]
    s = re.split(r"[-_:./]", s, 1)[0]
    s = re.sub(r"[^0-9a-zA-Z]+", "", s).lower() or "model"
    return s

def _render_llm_compare_wide(base_df: pd.DataFrame, labeled_by_model: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    import re

    def _short_name(mid: str) -> str:
        s = str(mid).lower()
        # detect Hermes label name before llama
        if "hermes" in s or "nousresearch" in s:
            return "Hermes"
        if "meta-llama" in s:
            return "Llama"
        if "qwen" in s:
            return "Qwen"
        if "/" in s:
            s = s.split("/", 1)[1]
        s = re.split(r"[-_:./]", s, 1)[0]
        s = re.sub(r"[^0-9a-zA-Z]+", "", s)
        return s or "model"

    # base (unique ids) — keep platform + likes so Evaluation can groupby
    b = base_df.copy()
    if "vader" in b.columns:
        b = b.rename(columns={"vader": "vader_score"})
    if "platform" not in b.columns:
        b["platform"] = "unknown"
    if "likes" not in b.columns:
        b["likes"] = 0

    wide = (
        b[["id", "text", "platform", "likes", "vader_label", "vader_score"]]
        .drop_duplicates(subset=["id"])
        .copy()
    )

    grouped: Dict[str, pd.DataFrame] = {}
    for mid, dfm in labeled_by_model.items():
        if dfm is None or dfm.empty:
            continue
        tag = _short_name(mid)
        cand = dfm.copy()
        keep_cols = [c for c in ["id", "llm_label", "llm_score", "llm_sarcasm", "llm_ethics"] if c in cand.columns]
        cand = cand[keep_cols]
        cand["id"] = pd.to_numeric(cand["id"], errors="coerce").fillna(0).astype(int)
        cand = cand.sort_values(["id"]).drop_duplicates(subset=["id"], keep="last")
        if tag not in grouped or len(cand) > len(grouped[tag]):
            grouped[tag] = cand

    # merge once based on tag order
    tag_order = ["Qwen", "Llama", "Hermes"]
    merge_order = [t for t in tag_order if t in grouped] + [t for t in grouped if t not in tag_order]
    for tag in merge_order:
        g = grouped[tag].rename(columns={
            "llm_label":   f"{tag}_label",
            "llm_score":   f"{tag}_score",
            "llm_sarcasm": f"{tag}_sarcasm",
            "llm_ethics":  f"{tag}_ethics",
        })
        wide = wide.merge(g, on="id", how="left")

    wide["vader_label"] = (
        wide.get("vader_label")
            .astype(str).str.strip().str.lower()
            .replace({"neg":"negative", "neu":"neutral", "pos":"positive"})
            .fillna("neutral")
    )
    wide["platform"] = wide["platform"].astype(str).replace({"": "unknown"})
    wide["likes"] = pd.to_numeric(wide.get("likes"), errors="coerce").fillna(0).astype(int)
    wide["vader_score"] = pd.to_numeric(wide.get("vader_score"), errors="coerce")

    wide = wide.sort_values(["id"]).drop_duplicates(subset=["id"], keep="last").reset_index(drop=True)
    return wide

def _file_token_from_model(mid: str) -> str:
    import re
    s = str(mid)
    s = s.replace("/", "_").replace(":", "_")
    s = re.sub(r"\s+", "_", s).strip("_")
    return s

def _build_tab_csv_bundle(wide: pd.DataFrame) -> bytes:
    import io, zipfile
    from pathlib import Path

    df = wide.copy()

    if "Human_label" not in df.columns:
        try:
            gt = _load_ground_truth(APP_OUTDIR / "ground_truth.csv")
            if gt.empty and Path("ground_truth.csv").exists():
                gt = _load_ground_truth(Path("ground_truth.csv"))
            if not gt.empty:
                df = df.merge(gt[["id","Human_label","Human_sarcasm","Human_ethics"]], on="id", how="left")
        except Exception:
            pass

    final_tbl = df.sort_values("id")

    def _metrics(y_true: pd.Series, y_pred: pd.Series) -> dict[str, float]:
        y_true = y_true.astype(str)
        y_pred = y_pred.astype(str)
        labels = ["negative","neutral","positive"]
        from sklearn.metrics import accuracy_score, f1_score, cohen_kappa_score, matthews_corrcoef
        return {
            "accuracy": accuracy_score(y_true, y_pred),
            "macro_f1": f1_score(y_true, y_pred, average="macro", labels=labels, zero_division=0),
            "weighted_f1": f1_score(y_true, y_pred, average="weighted", labels=labels, zero_division=0),
            "kappa": cohen_kappa_score(y_true, y_pred, labels=labels),
            "mcc": matthews_corrcoef(y_true, y_pred),
        }

    has_human = ("Human_label" in df.columns) and df["Human_label"].astype(str).str.strip().ne("").any()

    # Overall metrics
    if has_human:
        rows = []
        for m in ("Hermes","Llama","Qwen","VADER"):
            col = "vader_label" if m == "VADER" else f"{m}_label"
            if col in df.columns:
                met = _metrics(df["Human_label"], df[col])
                met["model"] = m
                met["n"] = int(df["Human_label"].notna().sum())
                rows.append(met)
        overall_metrics = pd.DataFrame(rows, columns=["model","n","accuracy","macro_f1","weighted_f1","kappa","mcc"])
    else:
        overall_metrics = pd.DataFrame(columns=["model","n","accuracy","macro_f1","weighted_f1","kappa","mcc"])

    # Per-platform
    if has_human and "platform" in df.columns:
        plat_rows = []
        for plat in sorted(df["platform"].dropna().astype(str).unique()):
            sub = df[df["platform"].astype(str) == plat]
            for m in ("Hermes","Llama","Qwen","VADER"):
                col = "vader_label" if m == "VADER" else f"{m}_label"
                if col in sub.columns:
                    met = _metrics(sub["Human_label"], sub[col])
                    met.update({"model": m, "platform": plat, "n": int(sub.shape[0])})
                    plat_rows.append(met)
        per_platform = pd.DataFrame(plat_rows, columns=["platform","model","n","accuracy","macro_f1","weighted_f1","kappa","mcc"])
        yt_csv = per_platform[per_platform["platform"].str.lower().eq("youtube")].drop(columns=["platform"]) if not per_platform.empty else pd.DataFrame(columns=["model","n","accuracy","macro_f1","weighted_f1","kappa","mcc"])
        tw_csv = per_platform[per_platform["platform"].str.lower().eq("twitter")].drop(columns=["platform"]) if not per_platform.empty else pd.DataFrame(columns=["model","n","accuracy","macro_f1","weighted_f1","kappa","mcc"])
    else:
        yt_csv = pd.DataFrame(columns=["model","n","accuracy","macro_f1","weighted_f1","kappa","mcc"])
        tw_csv = pd.DataFrame(columns=["model","n","accuracy","macro_f1","weighted_f1","kappa","mcc"])

    # Confusion matrix (Human vs Qwen)
    if has_human and "Qwen_label" in df.columns:
        cm = (
            pd.crosstab(
                index=df["Human_label"].astype(str),
                columns=df["Qwen_label"].astype(str),
                dropna=False,
            )
            .reindex(index=["negative","neutral","positive"], columns=["negative","neutral","positive"], fill_value=0)
            .stack()
            .reset_index()
            .rename(columns={"Human_label":"T","Qwen_label":"P",0:"count"})
        )
    else:
        cm = pd.DataFrame(columns=["T","P","count"])

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("Final table.csv", final_tbl.to_csv(index=False, encoding="utf-8"))
        z.writestr("Overall metrics.csv", overall_metrics.to_csv(index=False, encoding="utf-8"))
        z.writestr("Per-platform YouTube.csv", yt_csv.to_csv(index=False, encoding="utf-8"))
        z.writestr("Per-platform Twitter.csv", tw_csv.to_csv(index=False, encoding="utf-8"))
        z.writestr("Evaluation Tab - Qwen Confusion Matrix Table.csv", cm.to_csv(index=False, encoding="utf-8"))
    return buf.getvalue()

def _build_all_confusions_zip(wide: "pd.DataFrame") -> bytes:
    import io, zipfile
    import pandas as pd
    from pathlib import Path

    df = wide.copy()

    if "Human_label" not in df.columns:
        try:
            gt = _load_ground_truth(APP_OUTDIR / "ground_truth.csv")
            if gt.empty and Path("ground_truth.csv").exists():
                gt = _load_ground_truth(Path("ground_truth.csv"))
            if not gt.empty:
                df = df.merge(gt[["id","Human_label","Human_sarcasm","Human_ethics"]], on="id", how="left")
        except Exception:
            pass

    has_human = ("Human_label" in df.columns) and df["Human_label"].astype(str).str.strip().ne("").any()
    if not has_human:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
            z.writestr("README.txt", "No Human_label available. Confusion matrices cannot be computed.")
        return buf.getvalue()

    def _norm(s: "pd.Series") -> "pd.Series":
        return (s.astype(str).str.strip().str.lower()
                  .replace({"neg":"negative","neu":"neutral","pos":"positive"}))

    labs = ["negative","neutral","positive"]
    tags = []
    for tag, col in [("Hermes","Hermes_label"),("Llama","Llama_label"),
                     ("Qwen","Qwen_label"),("VADER","vader_label")]:
        if col in df.columns:
            tags.append((tag, col))

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
        y_true = _norm(df["Human_label"])
        for tag, col in tags:
            y_pred = _norm(df[col])
            cm = (pd.crosstab(index=y_true, columns=y_pred, dropna=False)
                    .reindex(index=labs, columns=labs, fill_value=0))
            z.writestr(f"Evaluation Tab - {tag} Confusion Matrix Table.csv",
                       cm.to_csv(index=True, encoding="utf-8"))
    return buf.getvalue()

def _build_model_platform_results_zip(wide: "pd.DataFrame") -> bytes:
    import io, zipfile
    import pandas as pd
    from pathlib import Path

    df = wide.copy()

    if "Human_label" not in df.columns:
        try:
            gt = _load_ground_truth(APP_OUTDIR / "ground_truth.csv")
            if gt.empty and Path("ground_truth.csv").exists():
                gt = _load_ground_truth(Path("ground_truth.csv"))
            if not gt.empty:
                df = df.merge(gt[["id","Human_label"]], on="id", how="left")
        except Exception:
            pass

    has_human = ("Human_label" in df.columns) and df["Human_label"].astype(str).str.strip().ne("").any()
    if not has_human:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
            z.writestr("README.txt", "No Human_label available. Per-platform results cannot be computed.")
        return buf.getvalue()

    def _norm(s: "pd.Series") -> "pd.Series":
        return s.astype(str).str.strip().str.lower().replace({"neg":"negative","neu":"neutral","pos":"positive"})

    models = [
        ("Hermes","Hermes_label"),
        ("Llama","Llama_label"),
        ("Qwen","Qwen_label"),
        ("VADER","vader_label"),
    ]

    if "platform" in df.columns:
        plat_col = df["platform"].astype(str).str.lower()
    else:
        plat_col = pd.Series(["all"] * len(df), index=df.index)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
        y_true = _norm(df["Human_label"])

        for tag, col in models:
            if col not in df.columns:
                continue

            y_pred = _norm(df[col])
            mask = y_true.ne("").fillna(False) & y_pred.ne("").fillna(False)

            sub_t = y_true[mask]
            sub_p = y_pred[mask]
            sub_plat = plat_col[mask]

            rows = []
            for plat, idx in sub_plat.groupby(sub_plat).groups.items():
                t = sub_t.loc[idx]
                p = sub_p.loc[idx]
                n = int(len(p))
                rows.append({
                    "platform": str(plat),
                    "n": n,
                    "neg": int((p == "negative").sum()),
                    "neu": int((p == "neutral").sum()),
                    "pos": int((p == "positive").sum()),
                    "model_vs_human_acc": float((p == t).mean()) if n else 0.0,
                })

            # pad zero rows for any platforms with no data
            existing = {r["platform"] for r in rows}
            for plat in sorted(set(plat_col.unique()) - existing):
                rows.append({"platform": str(plat), "n": 0, "neg": 0, "neu": 0, "pos": 0, "model_vs_human_acc": 0.0})

            out = (pd.DataFrame(rows, columns=["platform","n","neg","neu","pos","model_vs_human_acc"])
                     .sort_values("platform"))
            z.writestr(f"Evaluation Tab - {tag} Per-Platform Results.csv",
                       out.to_csv(index=False, encoding="utf-8"))
    return buf.getvalue()

def _write_dashboard_from_wide(wide: "pd.DataFrame", out_html: "Path") -> str:
    import pandas as pd, json

    df = wide.copy()

    def _norm(s: "pd.Series") -> "pd.Series":
        return (s.astype(str).str.strip().str.lower()
                  .replace({"neg":"negative","neu":"neutral","pos":"positive"}))

    def _has(col: str) -> bool:
        return col in df.columns

    if "platform" not in df.columns: df["platform"] = "unknown"
    if "likes" not in df.columns: df["likes"] = 0
    if "vader_score" not in df.columns and "vader" in df.columns:
        df["vader_score"] = df["vader"]
    df["likes"] = pd.to_numeric(df["likes"], errors="coerce").fillna(0)
    df["vader_score"] = pd.to_numeric(df.get("vader_score", 0), errors="coerce").fillna(0)

    label_cols = [c for c in ["Hermes_label","Llama_label","Qwen_label","vader_label"] if _has(c)]
    primary_lab = label_cols[0] if label_cols else None

    # Ethical tags by descending sentiment frequency
    ethics_col = next((c for c in ["Hermes_ethics","Qwen_ethics","Llama_ethics"] if _has(c)), None)
    c1_vals = []
    if ethics_col and primary_lab:
        labs = _norm(df[primary_lab])
        for tags, lab in zip(df[ethics_col].fillna("none").astype(str), labs):
            for t in [x.strip() for x in tags.split(",") if x.strip()]:
                c1_vals.append({"ethics": t, "sentiment": lab})

    spec_c1 = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {"values": c1_vals},
        "title": {"text": "Ethics tags by sentiment"},
        "mark": "bar",
        "encoding": {
            "x": {"field": "ethics", "type": "nominal", "sort": "-y", "title": None},
            "y": {"aggregate": "count", "title": "Count"},
            "color": {"field": "sentiment", "type": "nominal", "title": None},
            "tooltip": [
                {"aggregate":"count","title":"rows"},
                {"field":"ethics","type":"nominal"},
                {"field":"sentiment","type":"nominal"}
            ],
        },
        "width": 680, "height": 380
    }

    # Stacked Label distribution by model
    c2_vals = []
    for col in label_cols:
        model = col.replace("_label","").capitalize()
        for lab, n in _norm(df[col]).value_counts().items():
            c2_vals.append({"model": model, "label": lab, "count": int(n)})

    spec_c2 = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {"values": c2_vals},
        "title": {"text": "Label distribution by model"},
        "mark": "bar",
        "encoding": {
            "x": {"field": "model", "type": "nominal", "title": None},
            "y": {"aggregate": "sum", "field": "count", "stack": "normalize", "title": "share"},
            "color": {"field": "label", "type": "nominal", "title": None},
            "tooltip": [
                {"field":"model","type":"nominal"},
                {"field":"label","type":"nominal"},
                {"aggregate":"sum","field":"count","title":"rows"},
                {"aggregate":"sum","field":"count","title":"share","format":".0%"}
            ],
        },
        "width": 680, "height": 380
    }

    # Likes vs polarity scatter plot
    c3_vals = []
    if primary_lab:
        for pol, likes, lab in zip(df["vader_score"], df["likes"], _norm(df[primary_lab])):
            c3_vals.append({"polarity": float(pol), "likes": float(likes), "label": lab})

    spec_c3 = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {"values": c3_vals},
        "title": {"text": "Likes vs polarity"},
        "mark": {"type":"point"},
        "encoding": {
            "x": {"field":"polarity","type":"quantitative","title":"polarity"},
            "y": {"field":"likes","type":"quantitative","title":"likes"},
            "color": {"field":"label","type":"nominal","title": None},
            "tooltip": [
                {"field":"label","type":"nominal"},
                {"field":"polarity","type":"quantitative","format":".3f"},
                {"field":"likes","type":"quantitative","format":".0f"}
            ]
        },
        "width": 680, "height": 380
    }

    # Stacked VADER score histogram
    stack_lab = "vader_label" if _has("vader_label") else primary_lab
    c4_vals = []
    if stack_lab:
        for pol, lab in zip(df["vader_score"], _norm(df[stack_lab])):
            c4_vals.append({"score": float(pol), "label": lab})

    spec_c4 = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {"values": c4_vals},
        "title": {"text": "VADER score histogram"},
        "mark": "bar",
        "encoding": {
            "x": {"bin":{"maxbins":24}, "field":"score", "type":"quantitative", "title":"score (binned)"},
            "y": {"aggregate":"count","title":"Count"},
            "color": {"field":"label","type":"nominal","title": None},
            "tooltip": [{"aggregate":"count","title":"rows"},{"field":"label","type":"nominal"}]
        },
        "width": 680, "height": 380
    }

    # Final Table
    table_cols = [c for c in df.columns if c not in ("author","authorLink","username","user","handle","link","channel")]
    table_html = df[table_cols].to_html(index=False)

    # Vega-Lite based HTML Template
    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/vega@5"></script>
  <script src="https://cdn.jsdelivr.net/npm/vega-lite@5"></script>
  <script src="https://cdn.jsdelivr.net/npm/vega-embed@6"></script>
  <style>
    body {{ background:#ffffff; color:#111827; font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial; margin: 12px; }}
    .grid2 {{ max-width: 95vw; margin: 0 auto; display:grid; grid-template-columns: 1fr 1fr; gap: 18px; }}
    .tbl  {{ width: 98vw; margin: 22px auto; overflow:auto; }}
    table {{ border-collapse: collapse; width: max-content; white-space: nowrap; }}
    th, td {{ border-bottom: 1px solid #e5e7eb; padding: 6px 10px; text-align: left; }}
  </style>
</head>
<body>
  <div class="grid2">
    <div id="chart_1"></div>
    <div id="chart_2"></div>
    <div id="chart_3"></div>
    <div id="chart_4"></div>
  </div>

  <script>
    const spec1 = {json.dumps(spec_c1, ensure_ascii=False)};
    const spec2 = {json.dumps(spec_c2, ensure_ascii=False)};
    const spec3 = {json.dumps(spec_c3, ensure_ascii=False)};
    const spec4 = {json.dumps(spec_c4, ensure_ascii=False)};
    vegaEmbed('#chart_1', spec1, {{actions:false}});
    vegaEmbed('#chart_2', spec2, {{actions:false}});
    vegaEmbed('#chart_3', spec3, {{actions:false}});
    vegaEmbed('#chart_4', spec4, {{actions:false}});
  </script>

  <div class="tbl"><h2>Final table</h2>{table_html}</div>
</body>
</html>"""

    out_html.parent.mkdir(parents=True, exist_ok=True)
    out_html.write_text(html, encoding="utf-8")
    return html

def _load_ground_truth(path: Path) -> pd.DataFrame:
    if "_gt_mem" in st.session_state and isinstance(st.session_state._gt_mem, pd.DataFrame):
        gt = st.session_state._gt_mem.copy()
    elif path.exists() and path.stat().st_size > 0:
        gt = pd.read_csv(path)
    else:
        return pd.DataFrame(columns=["id","text","Human_sarcasm","Human_label","Human_ethics"])

    gt = gt.copy()
    gt["id"] = pd.to_numeric(gt.get("id", 0), errors="coerce").fillna(0).astype(int)
    gt["text"] = gt.get("text", "").astype(str)

    gt["Human_sarcasm"] = (
        gt.get("Human_sarcasm", False)
          .astype(str).str.strip().str.lower().isin(["1","true","t","yes","y"])
    )
    gt["Human_label"] = gt.get("Human_label", "").astype(str).str.strip().str.lower()

    def _to_list(v: Any) -> list[str]:
        if isinstance(v, list):
            return [str(x).strip() for x in v if str(x).strip()]
        s = str(v or "").strip()
        if not s or s.lower() == "none":
            return []
        # JSON list or comma separated
        try:
            import json
            j = json.loads(s)
            if isinstance(j, list):
                return [str(x).strip() for x in j if str(x).strip()]
        except Exception:
            pass
        return [t.strip() for t in s.split(",") if t.strip()]

    gt["Human_ethics"] = gt.get("Human_ethics", "").apply(_to_list)

    gt = gt.sort_values("id").drop_duplicates("id", keep="last").reset_index(drop=True)
    return gt[["id","text","Human_sarcasm","Human_label","Human_ethics"]]


def _save_ground_truth(df_in: pd.DataFrame, path: Path) -> None:
    out = df_in.copy()

    out["id"] = pd.to_numeric(out["id"], errors="coerce").fillna(0).astype(int)
    out["text"] = out.get("text", "").astype(str)
    out["Human_sarcasm"] = out.get("Human_sarcasm", False).fillna(False).astype(bool)

    # map values to sentiment labels {positive,neutral,negative,""}
    out["Human_label"] = (
        out.get("Human_label", "")
           .astype(str).str.strip().str.lower()
           .map({"pos":"positive","positive":"positive",
                 "neu":"neutral","neutral":"neutral",
                 "neg":"negative","negative":"negative"}).fillna("")
    )

    def _serialize(v: Any) -> str:
        items = _coerce_ethics_list(v)
        if "none" in set(items):
            return "none"
        items = [str(x).strip().lower() for x in items if str(x).strip()]
        return ",".join(sorted(dict.fromkeys(items), key=str.lower)) or "none"

    out["Human_ethics"] = out.get("Human_ethics", []).apply(_serialize)

    out = out.sort_values("id").drop_duplicates("id", keep="last")
    path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(path, index=False, encoding="utf-8")

    mem = out.copy()
    mem["Human_ethics"] = mem["Human_ethics"].apply(_coerce_ethics_list)
    st.session_state._gt_mem = mem

def _canon_ground_truth(df: pd.DataFrame) -> pd.DataFrame:
    g = df.copy()

    # sort columns based on id
    if "id" not in g.columns:
        if "comment_id" in g.columns:
            g["id"] = pd.to_numeric(g["comment_id"], errors="coerce").fillna(0).astype(int) % 100000
        elif "tweet_id" in g.columns:
            g["id"] = pd.to_numeric(g["tweet_id"], errors="coerce").fillna(0).astype(int) % 100000
        else:
            base = g.get("text", "").fillna("").astype(str)
            g["id"] = pd.util.hash_pandas_object(base, index=False).astype(int) % 100000
    g["id"] = pd.to_numeric(g["id"], errors="coerce").fillna(0).astype(int)

    # keep Human defined sentiment labels
    lab_col = next((c for c in g.columns if str(c).strip().lower() in
                   {"human_label", "human_sentiment", "label", "gt_label"}), None)
    v = g.get(lab_col, "neutral")
    v = pd.Series(v, index=g.index).astype(str).str.strip().str.lower()
    g["Human_label"] = v.map({"neg": "negative", "negative":"negative",
                              "neu":"neutral", "neutral":"neutral",
                              "pos":"positive","positive":"positive"}).fillna("neutral")

    # keep Human defined sarcasm tags
    sar_col = next((c for c in g.columns if str(c).strip().lower() in
                   {"human_sarcasm","sarcasm","gt_sarcasm"}), None)
    s = g.get(sar_col, 0)
    s = pd.Series(s, index=g.index).astype(str).str.strip().str.lower()
    g["Human_sarcasm"] = s.isin({"1","y","yes","true"})

    # keep Human defined tags
    eth_col = next((c for c in g.columns if str(c).strip().lower() in {"human_ethics","ethics","gt_ethics"}), None)
    e = g.get(eth_col, "none")
    e = pd.Series(e, index=g.index).astype(str).str.lower().str.replace(" ", "", regex=False)
    
    # keep LLM defined tags
    allowed = set(ETHICS_CODES_FULL)
    def _clean_eth(x: str) -> str:
        items = [ETH_SHORT2FULL.get(tok, tok) for tok in x.split(",") if tok]
        items = [z for z in items if z in allowed]
        return ",".join(sorted(set(items))) if items else "none"
    g["Human_ethics"] = e.map(_clean_eth)

    return g[["id", "Human_label", "Human_sarcasm", "Human_ethics"]].drop_duplicates("id")

def _wide_from_session() -> pd.DataFrame | None:
    for k in ("wide", "final_wide", "scored_wide", "evaluation_wide"):
        df = st.session_state.get(k)
        if isinstance(df, pd.DataFrame) and not df.empty:
            return df.copy()
    return None

def _get_wide(force_rebuild: bool = False) -> pd.DataFrame:
    ss = st.session_state
    if not force_rebuild and isinstance(ss.get("wide"), pd.DataFrame) and not ss["wide"].empty:
        return ss["wide"]

    df_scored: pd.DataFrame | None = ss.get("df_scored")
    llm_labels: dict | None     = ss.get("llm_labels")

    if isinstance(df_scored, pd.DataFrame) and not df_scored.empty and llm_labels:
        wide = _render_llm_compare_wide(df_scored, llm_labels)
        ss["wide"] = wide
        ss["final_wide"] = wide
        ss["scored_wide"] = wide
        ss["evaluation_wide"] = wide
        return wide

    raise RuntimeError(
        "Wide dataframe not found and cannot be built. Make sure df_scored and llm_labels exist."
    )

# Export tab
def _render_export_tab():
    st.subheader("Export")

    try:
        wide = _get_wide()
    except RuntimeError as e:
        st.error(str(e))
        return

    # Export download files
    scored_csv = wide.to_csv(index=False, encoding="utf-8")
    html_str = _write_dashboard_from_wide(wide, APP_OUTDIR / "dashboard.html")
    tabs_zip = _build_tab_csv_bundle(wide)
    conf_zip = _build_all_confusions_zip(wide)
    perplat_zip = _build_model_platform_results_zip(wide)

    cols = st.columns(4)
    with cols[0]:
        st.download_button(
            "Download dashboard.html",
            data=html_str.encode("utf-8"),
            file_name="dashboard.html",
            mime="text/html",
            width='stretch',
        )
    with cols[1]:
        st.download_button(
            "Download scored.csv",
            data=scored_csv.encode("utf-8"),
            file_name="scored.csv",
            mime="text/csv",
            width='stretch',
        )
    with cols[2]:
        st.download_button(
            "Download Tab CSV Data.zip",
            data=tabs_zip,
            file_name="Tab CSV Data.zip",
            mime="application/zip",
            width='stretch',
        )
    with cols[3]:
        st.download_button(
            "Download all confusion matrices.zip",
            data=conf_zip,
            file_name="All Confusion Matrices.zip",
            mime="application/zip",
            width='stretch',
        )

    st.download_button(
        "Download per-platform results.zip",
        data=perplat_zip,
        file_name="Per-Platform Results.zip",
        mime="application/zip",
        use_container_width=True,
    )

def _show_llm_compare_table(wide_df: pd.DataFrame):
    # All sarcastic column values must be True/False
    cfg = {}
    for c in wide_df.columns:
        if c.endswith("_sarcasm"):
            cfg[c] = st.column_config.CheckboxColumn(c, help="Model marked this text as sarcastic", default=False, disabled=True)
    st.dataframe(wide_df, width='stretch', height=520, column_config=cfg)

def _collect_ethics_options(wide_df: pd.DataFrame) -> list[str]:
    vals: set[str] = set(ETHICS_CODES_FULL)

    # LLM model outputs
    for c in [x for x in wide_df.columns if x.endswith("_ethics")]:
        for s in wide_df[c].dropna().astype(str):
            for t in s.split(","):
                k = str(t).strip().lower()
                if k:
                    vals.add(k)

    gt_mem = st.session_state.get("_gt_mem")
    if isinstance(gt_mem, pd.DataFrame) and "Human_ethics" in gt_mem.columns:
        for cell in gt_mem["Human_ethics"]:
            if isinstance(cell, list):
                for t in cell:
                    k = str(t).strip().lower()
                    if k:
                        vals.add(k)
            else:
                for t in str(cell or "").split(","):
                    k = str(t).strip().lower()
                    if k:
                        vals.add(k)

    vals.add("none")
    return sorted(vals, key=str.lower)


def _save_ground_truth(df_in: pd.DataFrame, path: Path) -> None:
    out = df_in.copy()

    out["id"] = pd.to_numeric(out["id"], errors="coerce").fillna(0).astype(int)
    out["text"] = out["text"].astype(str)
    out["Human_sarcasm"] = out.get("Human_sarcasm", False).fillna(False).astype(bool)
    out["Human_label"] = out.get("Human_label", "").astype(str).str.strip().str.lower()

    def _serialize(v: Any) -> str:
        items = _coerce_ethics_list(v)  # exclusivity + default handled here
        if "none" in set(items):
            return "none"
        items = [str(x).strip().lower() for x in items if str(x).strip()]
        return ",".join(sorted(dict.fromkeys(items), key=str.lower)) or "none"

    out["Human_ethics"] = out.get("Human_ethics", []).apply(_serialize)

    out = out.sort_values("id").drop_duplicates(subset=["id"], keep="last")
    path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(path, index=False, encoding="utf-8")

    mem = out.copy()
    mem["Human_ethics"] = mem["Human_ethics"].apply(_coerce_ethics_list)
    st.session_state._gt_mem = mem

    Path("Ground-Truth Labels.csv").write_text(out.to_csv(index=False), encoding="utf-8")

def _render_manual_verify_tab():
    st.subheader("Manual Verify")

    try:
        wide = _get_wide(force_rebuild=False)
    except RuntimeError:
        st.info("Run LLM labeling first.")
        st.stop()

    wide_disp = wide.sort_values("id").reset_index(drop=True)

    # LLM Comparison table
    st.write("Final table")
    sarcasm_cols = {c: st.column_config.CheckboxColumn(c, disabled=True)
                    for c in wide_disp.columns if c.endswith("_sarcasm")}
    st.dataframe(
        wide_disp,
        width="stretch",
        height=360,
        column_config=sarcasm_cols,
    )

    # Manually Verification Table
    st.write("Enter Ground-Truth Labels")

    base = wide_disp[["id", "text"]].copy()
    gt_path = APP_OUTDIR / "ground_truth.csv"
    prior = _load_ground_truth(gt_path)
    if not prior.empty:
        prior_narrow = prior[["id","Human_sarcasm","Human_label","Human_ethics"]]
        base = base.merge(prior_narrow, on="id", how="left")

    if "Human_sarcasm" not in base.columns:
        base["Human_sarcasm"] = False
    if "Human_label" not in base.columns:
        base["Human_label"] = ""

    # Default to ['none']; coerce any prior values
    if "Human_ethics" not in base.columns:
        base["Human_ethics"] = pd.Series([["none"] for _ in range(len(base))], dtype=object)
    else:
        base["Human_ethics"] = base["Human_ethics"].apply(_coerce_ethics_list)

    ethics_options = _collect_ethics_options(wide_disp)

    editor_cfg = {
        "id": st.column_config.NumberColumn("id", disabled=True),
        "text": st.column_config.TextColumn("text", disabled=True, width="stretch"),
        "Human_sarcasm": st.column_config.CheckboxColumn("Human_sarcasm"),
        "Human_label": st.column_config.SelectboxColumn(
            "Human_label", options=["positive","neutral","negative"]
        ),
        # Core change: accept typed values not in options
        "Human_ethics": st.column_config.MultiselectColumn(
            "Human_ethics",
            options=ethics_options,
            accept_new_options=True,
            help="Pick or type tags. 'none' is exclusive.",
        ),
    }

    edited = st.data_editor(
        base[["id","text","Human_sarcasm","Human_label","Human_ethics"]].reset_index(drop=True),
        num_rows="fixed",
        width="stretch",
        column_config=editor_cfg,
        key="gt_editor",
    )

    if st.button("Save Manual Labels", type="primary", key="save_gt"):
        edited = edited.copy()
        edited["Human_ethics"] = edited["Human_ethics"].apply(_coerce_ethics_list)
        _save_ground_truth(edited, gt_path)
        st.success("Saved ground_truth.csv")

def _gt_path() -> Path:
    p = APP_OUTDIR / "Ground-Truth Labels.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def _coerce_label(x: Any) -> str:
    t = str(x or "").strip().lower()
    if t in {"", "none", "nan"}: return ""
    if t.startswith("pos"): return "positive"
    if t.startswith("neg"): return "negative"
    return "neutral"

def _coerce_ethics_list(v: Any) -> list[str]:
    if isinstance(v, list):
        items = v
    else:
        s = str(v or "").strip()
        items = [] if not s or s.lower() == "nan" else [t for t in s.split(",") if t.strip()]
    out, seen = [], set()
    for t in items:
        k = str(t).strip().lower()
        if k and k not in seen:
            seen.add(k); out.append(k)
    if not out or "none" in seen:
        return ["none"]
    return out

def _collect_ethics_options(wide_df: pd.DataFrame) -> list[str]:
    vals: set[str] = set(ETHICS_CODES_FULL)

    # from LLM model outputs
    for c in [x for x in wide_df.columns if x.endswith("_ethics")]:
        for s in wide_df[c].dropna().astype(str):
            for t in s.split(","):
                k = str(t).strip().lower()
                if k:
                    vals.add(k)

    gt_mem = st.session_state.get("_gt_mem")
    if isinstance(gt_mem, pd.DataFrame) and "Human_ethics" in gt_mem.columns:
        for cell in gt_mem["Human_ethics"].tolist():
            if isinstance(cell, list):
                for t in cell:
                    k = str(t).strip().lower()
                    if k:
                        vals.add(k)
            else:
                for t in str(cell or "").split(","):
                    k = str(t).strip().lower()
                    if k:
                        vals.add(k)

    custom = st.session_state.get("_custom_ethics", [])
    for t in custom:
        k = str(t).strip().lower()
        if k:
            vals.add(k)

    return sorted(vals, key=str.lower)

def _render_benchmark_tab() -> None:
    import numpy as np
    import pandas as pd
    import streamlit as st
    from pathlib import Path

    outdir = Path(APP_OUTDIR)
    outdir.mkdir(parents=True, exist_ok=True)

    st.markdown("""
    <style>
      header, [data-testid="stHeader"] { visibility: visible !important; display: block !important; }
      .block-container { padding-top: 2rem !important; padding-bottom: 0.4rem; }
      .stDataFrame { font-size: 12px; }
      h2, h3 { margin: 0.35rem 0 0.5rem 0; }
    </style>
    """, unsafe_allow_html=True)
    
    def _norm_label_series(s: pd.Series) -> pd.Series:
        v = s.astype(str).str.strip().str.lower()
        return v.replace({"neg":"negative","neu":"neutral","pos":"positive","":"neutral","none":"neutral"})

    def _norm_bool_series(s: pd.Series) -> pd.Series:
        return s.astype(str).str.strip().str.lower().isin({"1","true","y","yes","t"})

    def _norm_ethics_cell(x: str) -> str:
        allow = {"none","bias","privacy","transparency","job_displacement","safety",
                 "governance","misinformation","accountability","other"}
        toks = [t.strip().lower() for t in str(x).replace(";",",").replace("|",",").split(",") if t.strip()]
        toks = [t for t in toks if t in allow]
        if not toks or "none" in toks: return "none"
        return ",".join(sorted(set(toks)))

    def _safe_model_tags(df: pd.DataFrame) -> list[str]:
        tags=[]
        for tag in ["Qwen","Llama","Hermes"]:
            if f"{tag}_label" in df.columns: tags.append(tag)
        if "vader_label" in df.columns: tags.append("VADER")
        return tags

    def _confusion(y_true, y_pred, labels=("negative","neutral","positive")) -> pd.DataFrame:
        idx = {lab:i for i,lab in enumerate(labels)}
        m = np.zeros((3,3), dtype=int)
        for a,b in zip(y_true, y_pred):
            ia = idx.get(str(a)); ib = idx.get(str(b))
            if ia is not None and ib is not None: m[ia, ib] += 1
        return pd.DataFrame(m, index=["negative","neutral","positive"], columns=["negative","neutral","positive"])

    def _jaccard_lists(pred: str, human: str) -> float:
        p = {t for t in str(pred).split(",") if t and t != "none"}
        h = {t for t in str(human).split(",") if t and t != "none"}
        if not p and not h: return 1.0
        if not p or not h:  return 0.0
        return len(p & h) / float(len(p | h))

    # scored data
    base = st.session_state.df_scored.copy() if not st.session_state.df_scored.empty else st.session_state.df_all.copy()
    if base.empty:
        st.warning("No scored data found."); return

    if "vader" in base.columns and "vader_score" not in base.columns:
        base = base.rename(columns={"vader":"vader_score"})
    if "vader_label" not in base.columns and "vader_score" in base.columns:
        base["vader_label"] = base["vader_score"].apply(label_from_vader)

    wide = _render_llm_compare_wide(base, st.session_state.llm_labels)

    gt = safe_read_csv(outdir / "ground_truth.csv")
    if gt.empty and Path("ground_truth.csv").exists():
        gt = safe_read_csv(Path("ground_truth.csv"))
    if gt.empty or not {"id","Human_label"} <= set(gt.columns):
        st.error("Ground-truth not found. Expected: id, Human_label[, Human_sarcasm, Human_ethics]."); return

    gt = gt.copy()
    gt["id"] = pd.to_numeric(gt["id"], errors="coerce").fillna(0).astype(int)
    gt["Human_label"] = _norm_label_series(gt["Human_label"])
    gt["Human_sarcasm"] = _norm_bool_series(gt["Human_sarcasm"]) if "Human_sarcasm" in gt.columns else False
    gt["Human_ethics"] = gt["Human_ethics"].map(_norm_ethics_cell) if "Human_ethics" in gt.columns else "none"

    wide = wide.merge(gt[["id","Human_label","Human_sarcasm","Human_ethics"]], on="id", how="left")

    # Final table
    final_cols = ["id","text","platform","vader_label"]
    for tag in ["Qwen","Llama","Hermes"]:
        for c in (f"{tag}_label", f"{tag}_score"):
            if c in wide.columns: final_cols.append(c)
    final_cols += ["Human_label","Human_sarcasm","Human_ethics"]
    final_table = wide[final_cols].copy()
    final_table["text"] = final_table["text"].astype(str).str.replace(r"\s+", " ", regex=True).str.slice(0, 72)

    # model metrics calculated on a per-platform basis
    model_tags = _safe_model_tags(wide)
    metrics_rows = []
    per_platform = {"youtube": [], "twitter": []}
    conf_by_model = {}

    for tag in model_tags:
        lab_col = "vader_label" if tag == "VADER" else f"{tag}_label"
        sub = wide[["platform","Human_label", lab_col]].dropna(subset=[lab_col, "Human_label"]).copy()
        sub[lab_col]       = _norm_label_series(sub[lab_col])
        sub["Human_label"] = _norm_label_series(sub["Human_label"])
        y_t = sub["Human_label"].tolist()
        y_p = sub[lab_col].tolist()

        acc   = _accuracy(y_t, y_p)
        _, _, mf1 = _prf1(y_t, y_p)
        wf1   = _weighted_f1(y_t, y_p)
        kappa = _cohen_kappa(y_t, y_p)
        mcc   = _mcc_multiclass(y_t, y_p)

        if tag != "VADER" and f"{tag}_sarcasm" in wide.columns and "Human_sarcasm" in wide.columns:
            ssub = wide[[f"{tag}_sarcasm", "Human_sarcasm"]].dropna()
            import numpy as _np
            sarcasm_acc = float(_np.mean(_np.array(_norm_bool_series(ssub[f"{tag}_sarcasm"])) == _np.array(_norm_bool_series(ssub["Human_sarcasm"])))) if len(ssub) else float("nan")
        else:
            sarcasm_acc = float("nan")
        if tag != "VADER" and f"{tag}_ethics" in wide.columns and "Human_ethics" in wide.columns:
            ej = [_jaccard_lists(_norm_ethics_cell(p), _norm_ethics_cell(h)) for p,h in zip(wide[f"{tag}_ethics"], wide["Human_ethics"])]
            ethics_j = float(np.mean([x for x in ej if x is not None])) if len(ej) else float("nan")
        else:
            ethics_j = float("nan")

        metrics_rows.append({
            "model": tag, "n": len(sub),
            "accuracy": round(acc,6),
            "macro_f1": round(mf1,6),
            "weighted_f1": round(wf1,6),
            "kappa": round(kappa,6),
            "mcc": round(mcc,6),
            "sarcasm_acc": None if np.isnan(sarcasm_acc) else round(float(sarcasm_acc),6),
            "ethics_jaccard": None if np.isnan(ethics_j) else round(float(ethics_j),6),
        })

        for plat, g in sub.groupby("platform", dropna=False):
            ytp = g["Human_label"].tolist()
            ypp = g[lab_col].tolist()
            row = {"model": tag, "n": len(g),
                   "neg": int(sum(1 for x in ypp if x == "negative")),
                   "neu": int(sum(1 for x in ypp if x == "neutral")),
                   "pos": int(sum(1 for x in ypp if x == "positive")),
                   "acc": round(_accuracy(ytp, ypp), 6)}
            key = str(plat).strip().lower()
            if key in per_platform: per_platform[key].append(row)

        conf_by_model[tag] = _confusion(y_t, y_p)

    overall_df = pd.DataFrame(metrics_rows, columns=[
        "model","n","accuracy","macro_f1","weighted_f1","kappa","mcc","sarcasm_acc","ethics_jaccard"
    ]).sort_values("model")

    # CSV Export files
    try:
        (outdir / "Evaluation Tab - Final Table.csv").write_text(final_table.to_csv(index=False), encoding="utf-8")
        (outdir / "Evaluation Tab - Overall Model Evaluation Results Table.csv").write_text(overall_df.to_csv(index=False), encoding="utf-8")
        for plat in ("youtube","twitter"):
            dfp = pd.DataFrame(per_platform[plat], columns=["model","n","neg","neu","pos","acc"]).sort_values("model")
            (outdir / f"Evaluation Tab - Per-Platform Combined - {plat}.csv").write_text(dfp.to_csv(index=False), encoding="utf-8")
    except Exception:
        pass

    chart_1, chart_2 = st.columns([3,2], gap="small")
    with chart_1:
        st.subheader("Final table (top 5)")
        st.dataframe(final_table.head(5), width='stretch', hide_index=True, height=220)
    with chart_2:
        st.subheader("Overall metrics")
        st.dataframe(overall_df, width='stretch', hide_index=True, height=220)

    colY, colT = st.columns(2, gap="small")
    with colY:
        st.subheader("Per-platform: YouTube")
        tblY = pd.DataFrame(per_platform["youtube"], columns=["model","n","neg","neu","pos","acc"]).sort_values("model")
        st.dataframe(tblY, width='stretch', hide_index=True, height=185)
    with colT:
        st.subheader("Per-platform: Twitter")
        tblT = pd.DataFrame(per_platform["twitter"], columns=["model","n","neg","neu","pos","acc"]).sort_values("model")
        st.dataframe(tblT, width='stretch', hide_index=True, height=185)

    try:
        best_tag = overall_df.sort_values(["macro_f1","accuracy"], ascending=False).iloc[0]["model"]
        st.subheader(f"Best model based on confusion matrix: {best_tag}")
        cm = conf_by_model.get(best_tag, pd.DataFrame())
        st.dataframe(cm, width='stretch', height=165)  # full 3 rows
        (outdir / f"Evaluation Tab - {best_tag} Confusion Matrix Table.csv").write_text(
            cm.to_csv(index=True), encoding="utf-8"
        )
    except Exception:
        pass

st.title("Social Media Sentiment Analysis Tool")

tabs = st.tabs(["YouTube Ingestion", "Twitter Ingestion", "Scoring", "LLM Comparison", "Manual Verify", "Evaluation", "Export"])

# YouTube data ingestion Tab
with tabs[0]:
    st.subheader("YouTube via YCS extension")

    default_chrome, default_driver = _default_cft_paths()

    colA, colB = st.columns(2)
    with colA:
        chrome_bin = st.text_input(
            "Chrome for Testing binary",
            value=default_chrome or r"C:\Users\User\Desktop\Final_test\chrome-win64\chrome.exe",
        )
        cft_version = st.text_input("CFT version hint", value="142")
        crx_path = st.text_input(
            "YCS extension .crx",
            value=str((Path("extension") / "mfobjniokjbcldieppimekoibpocahed.crx").resolve()),
        )
    with colB:
        chromedriver = st.text_input(
            "ChromeDriver path",
            value=default_driver or r"C:\Users\User\Desktop\Final_test\chromedriver-win64\chromedriver.exe",
        )
        # default to output folder
        outdir_str = st.text_input("Output dir", value=str((Path.cwd() / "output").resolve()))

    urls = st.text_area("YouTube video URLs (one per line)", value="", height=120).strip().splitlines()
    run_yt = st.button("Run YouTube Ingestion", type="primary")
    if run_yt:
        try:
            if not urls or not any(u.strip() for u in urls):
                st.error("No URLs provided."); st.stop()
            df_yt = run_youtube(
                [u.strip() for u in urls if u.strip()],
                crx_path=Path(crx_path),
                chrome_binary=chrome_bin,
                chromedriver=chromedriver,
                cft_version=cft_version,
                outdir=Path(outdir_str),
            )
            if df_yt.empty:
                st.warning("No comments parsed.")
            st.session_state.df_youtube = _strip_pii(df_yt)
            st.dataframe(st.session_state.df_youtube.head(50))
            st.success(f"YouTube rows: {len(st.session_state.df_youtube)}")
        except Exception as e:
            st.exception(e)

# X/Twitter data ingestion Tab

with tabs[1]:
    st.subheader("X/Twitter official API")
    if not _HAS_TWEEPY:
        st.info("Install tweepy to enable this tab:  pip install tweepy")
    tw_col1, tw_col2, tw_col3 = st.columns(3)
    with tw_col1:
        x_bearer = st.text_input("X Bearer token", value=os.environ.get("X_BEARER_TOKEN",""), type="password")
        x_query = st.text_input("Search query", value="")
    with tw_col2:
        x_limit = st.number_input("Max tweets", min_value=1, value=100, step=1)
        x_scope = st.selectbox("Scope", options=["recent","all"], index=0)
    with tw_col3:
        x_start = st.text_input("Start date (YYYY-MM-DD)", value="")
        x_end   = st.text_input("End date (YYYY-MM-DD)", value="")
    run_tw = st.button("Fetch Tweets", type="primary")
    if run_tw:
        try:
            if not x_query.strip():
                st.error("Enter a query."); st.stop()
            df_tw = fetch_tweets_official(
                query=x_query.strip(), limit=int(x_limit), scope=x_scope,
                start_date=x_start.strip() or None, end_date=x_end.strip() or None,
                bearer=x_bearer.strip()
            )
            st.session_state.df_twitter = df_tw
            if df_tw.empty:
                st.warning("No tweets returned.")
            else:
                (APP_OUTDIR / "twitter.csv").write_text(df_tw.to_csv(index=False), encoding="utf-8")
                st.dataframe(_strip_pii(df_tw).head(50))
                st.success(f"Twitter rows: {len(df_tw)}")
        except Exception as e:
            st.exception(e)

# Scoring Tab

with tabs[2]:
    st.subheader("VADER/TextBlob scoring")
    frames = [d for d in [st.session_state.df_all, st.session_state.df_youtube, st.session_state.df_twitter]
              if isinstance(d, pd.DataFrame) and not d.empty]
    if frames and st.session_state.df_all.empty:
        df_all = pd.concat(frames, ignore_index=True)
        need = ["platform","video","text","likes","posted","is_reply","parent_id"]
        for c in need:
            if c not in df_all.columns:
                df_all[c] = "" if c not in ("likes","is_reply","parent_id") else (0 if c=="likes" else (False if c=="is_reply" else pd.NA))
        st.session_state.df_all = _rekey_parent_id_to_id(_ensure_id(df_all))

    if not st.session_state.df_all.empty and st.button("Score sentiment", type="primary"):
        st.session_state.df_scored = score_sentiment(st.session_state.df_all)
    
    if not st.session_state.df_scored.empty:
        dfv = st.session_state.df_scored.copy()
        disp_cols = [c for c in dfv.columns if c not in ("comment_id","tweet_id")]
        st.dataframe(dfv[disp_cols].head(50))
        chart_1,chart_2,chart_3 = st.columns(3)
        with chart_1:
            st.metric("Rows", len(dfv))
        with chart_2:
            counts = dfv["vader_label"].value_counts(dropna=False).to_dict()
            st.json(counts)
        with chart_3:
            mean_vader = float(dfv["vader"].mean())
            mean_sent = float(dfv["label_num"].mean())
            st.metric("Mean VADER", f"{mean_vader:.4f}")
            st.metric("Mean sentiment (−1..+1)", f"{mean_sent:.4f}")
    else:
        st.info("Load existing CSVs found on disk or ingest, then click Score sentiment.")

# LLM Comparison Tab
with tabs[3]:
    st.subheader("OpenRouter LLM Scoring")
    if st.session_state.df_scored.empty:
        st.info("Run scoring first.")
    else:
        _API_CALLS_PLANNED = _API_CALLS_MADE = _API_CACHE_HITS = 0
        _DEBUG_BUFFER = []

        colL, colR = st.columns(2)
        with colL:
            api_key = st.text_input(
                "OpenRouter API key",
                value=os.environ.get("OPENROUTER_API_KEY",""),
                type="password"
            )
            rpm = st.number_input("Requests per minute", min_value=1, value=int(os.environ.get("OPENROUTER_RPM","20")), step=1)
            models = st.multiselect("Models", options=FREE_MODELS, default=FREE_MODELS)
        with colR:
            prompt_chars = st.number_input("Max prompt chars", min_value=2000, value=int(os.environ.get("LLM_MAX_PROMPT_CHARS","120000")), step=1000)
            toks_per_row = st.number_input("Output tokens per row", min_value=5, value=int(os.environ.get("LLM_OUT_TOKENS_PER_ROW","8")), step=1)

        tele_box = st.empty()
        debug_box = st.empty()

        def _progress(state: Dict[str,Any]):
            tele_box.write({
                "planned_calls": int(state.get("planned", 0)),
                "calls_made": int(state.get("made", 0)),
                "cache_hits": int(state.get("cache", 0)),
                "chunk": f"{state.get('chunk', (0,0))[0]}/{state.get('chunk', (0,0))[1]}",
            })
            dbg = state.get("debug", [])
            debug_box.code("\n".join(dbg)[-8000:])

        if st.button("Run LLM labeling", type="primary"):
            if not api_key:
                st.error("API key required."); st.stop()
            scored_cols = ["id","platform","video","text","likes","posted","is_reply"]
            base_df = st.session_state.df_scored[scored_cols].copy()

            # run all models at once
            all_outputs = {}
            for m in models:
                out = run_llm_batch(
                    base_df,
                    model=m, api_key=api_key, rpm=int(rpm),
                    max_prompt_chars=int(prompt_chars), toks_per_row=int(toks_per_row),
                    outdir=APP_OUTDIR,
                    progress_cb=_progress,
                )
                st.session_state.llm_labels[_file_token_from_model(m)] = out
                all_outputs[m] = out

            try:
                wide = _get_wide(force_rebuild=False)
            except RuntimeError:
                st.info("Run LLM labeling first.")
                st.stop()
            st.success("LLM labeling complete.")

        # Always show all models together if available
        if st.session_state.llm_labels:
            wide = _render_llm_compare_wide(
                st.session_state.df_scored,
                st.session_state.llm_labels
            )
            _show_llm_compare_table(wide)

# Manual Verification Tab
with tabs[4]:
    _render_manual_verify_tab()

# Benchmarking Tab
with tabs[5]:
    _render_benchmark_tab()

# Data Export Tab
with tabs[6]:
    _render_export_tab()