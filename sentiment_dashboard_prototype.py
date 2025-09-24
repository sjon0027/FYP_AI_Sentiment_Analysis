"""
YouTube data processor

Requires: selenium, vaderSentiment, requests, tqdm (pip install selenium vaderSentiment requests tqdm)

Requires: ChromeDriver + YCS extension paths to be configured

Usage: python sentiment_dashboard_prototype.py <URL 1> [<URL 2> ... etc]

If no arguments are given, a small demo set inside the script is used.
"""
from __future__ import annotations
import os, re, json, sys, concurrent.futures, requests, textwrap
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, parse_qs

# --- add for X/Twitter + zero-shot ---
import asyncio
from typing import Literal, NamedTuple
from zero_shot import classify as zs_classify  # optional LLM/heuristics

from selenium.common.exceptions import TimeoutException, JavascriptException
import time

# Username data scraping toggle
# Set to False by default
usernameToggle: bool = False

# Path Configuration

BASE_DIR            = Path(__file__).resolve().parent
CHROMEDRIVER_PATH   = BASE_DIR / "chromedriver-win64" / "chromedriver.exe"
YCS_EXTENSION_PATH  = BASE_DIR / "chrome-extension-files"
TIMEOUT_REACHABLE   = 10        # seconds
POOL_WORKERS        = os.cpu_count() or 4

EXT_ROOTS: list[str] = []  # populated in make_driver()

# --- Web Store extension (YCS) ---
EXTENSION_ID = "mfobjniokjbcldieppimekoibpocahed"
EXT_CACHE_DIR = BASE_DIR / ".ext_cache"
DOWNLOAD_DIR = BASE_DIR / ".tmp_chrome" / "downloads"
YCS_CRX_PATH = os.getenv("YCS_CRX_PATH")  # optional override via env var


# Fixing URL information before it is processed

def normalize_url(url: str) -> str | None:
    url = url.strip()
    if not url:
        return None
    p = urlparse(url)
    host = p.netloc.lower()
    path = p.path.lstrip("/")
    vid = ""
    if host == "youtu.be":
        vid = path.split("?")[0]
    elif "youtube.com" in host:
        if path.startswith("watch"):
            vid = parse_qs(p.query).get("v", [""])[0]
        elif path.startswith("shorts/"):
            vid = path.split("/")[1]
        else:
            return None
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", vid):
        return f"https://www.youtube.com/watch?v={vid}"
    return None


def is_url_reachable(url: str) -> bool:
    try:
        return requests.get(url, timeout=TIMEOUT_REACHABLE).status_code == 200
    except requests.RequestException:
        return False


# Using Selenium to scrape and parse data accordingly 

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains

def _discover_ycs_extension_paths() -> list[str]:
    """
    Return a list of unpacked Chrome extension root folders (each must contain manifest.json).
    We ALWAYS include subfolders that have a manifest, even if the base folder also has one.
    You can override discovery with YCS_EXTENSION_PATHS="path1;path2" (or comma-separated).
    Extra diagnostics are printed for each manifest.
    """
    roots: list[str] = []

    # 1) explicit env var (highest priority)
    env = os.getenv("YCS_EXTENSION_PATHS", "").strip()
    if env:
        sep = ";" if ";" in env else ","
        for raw in (p.strip() for p in env.split(sep) if p.strip()):
            p = Path(raw)
            if p.is_dir() and (p / "manifest.json").exists():
                roots.append(str(p.resolve()))

    # 2) probe the default folder and ALL of its subfolders
    base = Path(YCS_EXTENSION_PATH)
    if base.exists():
        if (base / "manifest.json").exists():
            roots.append(str(base.resolve()))
        for child in base.iterdir():
            if child.is_dir() and (child / "manifest.json").exists():
                roots.append(str(child.resolve()))

    # 3) de-dupe but keep order
    seen = set()
    uniq = []
    for r in roots:
        if r not in seen:
            seen.add(r)
            uniq.append(r)

    # 4) read each manifest for diagnostics and basic sanity (YouTube host/matches)
    diag = []
    final = []
    for r in uniq:
        man_path = Path(r) / "manifest.json"
        try:
            man = json.loads(man_path.read_text(encoding="utf-8"))
        except Exception as e:
            diag.append(f"{r} -> manifest read error: {e}")
            continue
        name = man.get("name", "")
        mv = man.get("manifest_version")
        matches = []
        for cs in man.get("content_scripts", []):
            matches += cs.get("matches", [])
        hostp = man.get("host_permissions", [])
        youtube_ok = any("youtube.com" in m for m in matches + hostp)
        diag.append(f"{r} -> name={name!r}, mv={mv}, cs_matches={len(matches)}, youtube={youtube_ok}")
        final.append(r)

    print("   • Detected extension manifests:")
    for line in diag:
        print("     -", line)

    return final

def _ensure_webstore_crx(ext_id: str) -> Path:
    EXT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    crx_path = EXT_CACHE_DIR / f"{ext_id}.crx"
    if crx_path.exists() and crx_path.stat().st_size > 0:
        return crx_path
    url = ("https://clients2.google.com/service/update2/crx"
           f"?response=redirect&prodversion=9999.0.0.0&acceptformat=crx3"
           f"&x=id%3D{ext_id}%26installsource%3Dondemand%26uc")
    r = requests.get(url, timeout=60, stream=True)
    r.raise_for_status()
    with open(crx_path, "wb") as f:
        for chunk in r.iter_content(8192):
            if chunk: f.write(chunk)
    return crx_path

def _resolve_crx_path() -> Path:
    """Prefer user-provided CRX, else cached, else download once."""
    if YCS_CRX_PATH:
        p = Path(YCS_CRX_PATH)
        if not p.exists():
            raise FileNotFoundError(f"YCS_CRX_PATH not found: {p}")
        return p
    p = EXT_CACHE_DIR / "mfobjniokjbcldieppimekoibpocahed.crx"
    if p.exists() and p.stat().st_size > 0:
        return p
    # as a last resort, fetch from Google’s update service
    return _ensure_webstore_crx(EXTENSION_ID)

def make_driver() -> webdriver.Chrome:
    opt = webdriver.ChromeOptions()
    opt.add_argument("--start-maximized")
    for arg in ("--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"):
        opt.add_argument(arg)
    opt.add_experimental_option("excludeSwitches", ["enable-logging"])
    opt.add_argument("--no-first-run")
    opt.add_argument("--no-default-browser-check")
    opt.add_argument("--disable-session-crashed-bubble")
    opt.add_argument("--remote-debugging-port=0")

    # fresh temporary profile + known downloads dir
    tmp_profile = BASE_DIR / ".tmp_chrome" / f"profile-{os.getpid()}-{int(time.time())}"
    tmp_profile.mkdir(parents=True, exist_ok=True)
    opt.add_argument(f"--user-data-dir={tmp_profile}")

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    prefs = {
        "download.default_directory": str(DOWNLOAD_DIR),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    opt.add_experimental_option("prefs", prefs)

    # install YOUR CRX
    crx = _resolve_crx_path()
    print(f"   • Installing extension: {crx}")
    opt.add_extension(str(crx))

    try:
        return webdriver.Chrome(options=opt)
    except Exception:
        return webdriver.Chrome(service=Service(str(CHROMEDRIVER_PATH)), options=opt)

# Parse and categorize extension text data into top-level comment and their replies

def _parse_head(arr: list[str], idx: int) -> tuple[dict, int]:
    """Parse the raw data before processing them into parent comments and replies."""
    d = {
        "username":    arr[idx + 1] if usernameToggle else "",
        "profile_url": arr[idx + 2],
        "comment_url": arr[idx + 3],   # unique comment ID for every comment
        "posted":      "",
        "edited":      False,
        "likes":       0,
        "replies":     0,              # filled for top-level comments only
    }
    meta = arr[idx + 4]
    if "(edited)" in meta:
        d["edited"] = True
        meta = meta.replace("(edited)", "").strip()
    for part in (p.strip() for p in meta.split("|")):
        if part.startswith("like:"):
            d["likes"] = int(part.split(":", 1)[1])
        elif part.startswith("reply:"):
            d["replies"] = int(part.split(":", 1)[1])
        else:              # first token gives us the relative date
            d["posted"] = part
    return d, idx + 5          # iterates to the next unread line


def parse_comment_block(block: str) -> Optional[tuple[dict, list[dict]]]:
    """Return (parent_dict, [reply_dict, ...]) or None if not a comment block."""
    lines = [ln.rstrip() for ln in block.splitlines() if ln.strip()]
    if not lines or "[COMMENT]" not in lines[0]:
        return None

    # Parent comment categorization
    parent, i = _parse_head(lines, 0)

    body: list[str] = []
    while i < len(lines) and lines[i] != "Replies:":
        body.append(lines[i]);  i += 1
    parent["comment"] = "\n".join(body)

    # Reply (child) comment categorization
    replies: list[dict] = []
    if i < len(lines) and lines[i] == "Replies:":
        i += 1                        # skip literal marker
        while i < len(lines):
            if lines[i] != "[REPLY]": # ignore [REPLY] text
                i += 1;  continue

            rep, j = _parse_head(lines, i)
            text: list[str] = []
            while j < len(lines) and lines[j] != "[REPLY]":
                text.append(lines[j]);  j += 1
            rep["comment"]  = "\n".join(text)
            rep["parent_id"] = parent["comment_url"]
            replies.append(rep)
            i = j

    return parent, replies

def _dismiss_youtube_consent(driver) -> bool:
    """Click 'I agree' / 'Accept all' buttons on Google/YouTube consent screens (EU/UK etc.)."""
    try:
        return driver.execute_script(r"""
            function clickBtn(b){
                try { b.click(); return true; } catch(e) { return false; }
            }
            const texts = ['i agree','accept all','accept','agree','got it','ok'];
            const btns = Array.from(document.querySelectorAll('button, form button, #introAgreeButton, #button'));
            for (const b of btns){
                const t = (b.textContent||'').trim().toLowerCase();
                if (!t) continue;
                if (texts.some(x => t.includes(x))) { return clickBtn(b); }
            }
            // Sometimes consent is in an iframe or different container
            const q = sel => document.querySelector(sel);
            const cands = [
              "form[action*='consent'] button",
              "button[aria-label*='Agree']",
              "button[aria-label*='Accept']",
              "[role='dialog'] button"
            ];
            for (const sel of cands){
                const el = q(sel); if (el && clickBtn(el)) return true;
            }
            return false;
        """)
    except Exception:
        return False

def _goto_video(driver, url: str, attempts: int = 3, wait_secs: int = 15):
    """Navigate reliably to a YouTube watch page and ensure the title/metadata is present."""
    last_err = None
    for _ in range(attempts):
        try:
            driver.get("about:blank")  # clear NTP/new-tab distractions
            time.sleep(0.5)
            driver.get(url)
            # Give the page a moment to render and try to remove consent wall if present
            time.sleep(1.0)
            _dismiss_youtube_consent(driver)

            def ready(drv):
                try:
                    const_ok = drv.execute_script("return document.readyState") == "complete"
                except Exception:
                    const_ok = True
                # Any of these indicate a loaded watch page
                on_watch = "youtube.com/watch" in drv.current_url
                has_meta = drv.execute_script("""
                    return !!(document.querySelector('ytd-watch-metadata')
                              || document.querySelector('h1')
                              || document.querySelector('meta[itemprop="name"]'));
                """)
                return on_watch and has_meta

            WebDriverWait(driver, wait_secs).until(ready)
            return
        except Exception as e:
            last_err = e
            # one more try after a short pause
            time.sleep(1.2)
    raise TimeoutException(f"Could not reach a stable YouTube watch page. Last error: {last_err}")

def _force_inject_ycs(driver) -> int:
    """
    Hard-inject all content_scripts (JS + CSS) from the discovered YCS extension roots
    directly into the page. This bypasses MV3 withheld host-permissions / toolbar UI.
    Returns number of JS files injected.
    """
    roots = EXT_ROOTS or _discover_ycs_extension_paths()
    injected = 0
    for r in roots:
        man_path = Path(r) / "manifest.json"
        if not man_path.exists():
            continue
        try:
            man = json.loads(man_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        # CSS first (so UI has styles)
        for cs in man.get("content_scripts", []):
            for css in cs.get("css", []):
                p = Path(r) / css
                if p.exists():
                    css_text = p.read_text(encoding="utf-8", errors="ignore")
                    driver.execute_script("""
                        try {
                            var s=document.createElement('style');
                            s.textContent=arguments[0];
                            (document.head||document.documentElement).appendChild(s);
                        } catch(e) {}
                    """, css_text)
        # Then JS
        for cs in man.get("content_scripts", []):
            for js in cs.get("js", []):
                p = Path(r) / js
                if p.exists():
                    js_text = p.read_text(encoding="utf-8", errors="ignore")
                    # execute in page world
                    driver.execute_script("""
                        try {
                            var s=document.createElement('script');
                            s.textContent=arguments[0];
                            (document.documentElement||document.body).appendChild(s);
                            s.remove();
                        } catch(e) {}
                    """, js_text)
                    injected += 1
    return injected

def _find_btn_by_text(driver, texts: list[str]):
    """Return the first button/anchor whose visible text contains any of the given phrases."""
    return driver.execute_script("""
        const wants = arguments[0].map(s=>s.toLowerCase());
        const elems = Array.from(document.querySelectorAll('button,[role=button],a'));
        for (const el of elems) {
            const t = (el.innerText||'').trim().toLowerCase();
            if (!t) continue;
            for (const w of wants) { if (t.includes(w)) return el; }
        }
        return null;
    """, texts)

def _wait_for_download_text(timeout: int = 120) -> str:
    """Wait for a new file in DOWNLOAD_DIR and return its text."""
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    seen = {p.name for p in DOWNLOAD_DIR.glob("*") if p.is_file()}
    end = time.time() + timeout
    while time.time() < end:
        for p in DOWNLOAD_DIR.glob("*"):
            if not p.is_file() or p.suffix == ".crdownload":  # still downloading
                continue
            if p.name in seen:
                continue
            try:
                return p.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                pass
        time.sleep(0.5)
    raise TimeoutException("No downloaded export appeared.")

def get_comments(driver: webdriver.Chrome, url: str, timeout: int = 90) -> tuple[str, str, list[dict]]:
    """Returns video_id, video_title, list[comment-dict] (raw, no sentiment yet)."""
    wait = WebDriverWait(driver, timeout)

    # Robust navigation to the watch page (handles NTP + consent)
    _goto_video(driver, url, attempts=3, wait_secs=min(20, timeout))
    
    print("   ✓ Loaded watch page:", driver.current_url)

    # Now the title should be present; keep a short wait as a guard
    try:
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
    except TimeoutException:
        # Fallback to metadata if h1 structure changes
        wait.until(lambda d: d.execute_script("return !!document.querySelector('ytd-watch-metadata, meta[itemprop=\"name\"]')"))

    full_title = driver.title.rsplit(" - YouTube", 1)[0].strip()

    print("   • Waiting for YCS to inject...")

    print("   • Ensuring YCS UI is visible and loading comments…")

    # Scroll a bit to bring the YCS panel area into view (under the video)
    driver.execute_script("window.scrollBy(0, 600);")
    time.sleep(0.6)

    # Try to locate a 'Load all' (or similar) button; YCS exposes this on the page UI
    load_btn = None
    deadline = time.time() + 30
    while time.time() < deadline and not load_btn:
        load_btn = _find_btn_by_text(driver, ["load all", "load comments", "load"])
        if load_btn:
            break
        # Nudge the page to trigger lazy injection if needed
        driver.execute_script("window.scrollBy(0, 300);"); time.sleep(0.4)
        driver.execute_script("window.scrollBy(0, -150);"); time.sleep(0.3)

    if load_btn:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", load_btn)
        print("   ✓ Found 'Load' control. Loading comments…")
        driver.execute_script("arguments[0].click();", load_btn)
        # Give YCS time to fetch comments; we'll also watch for Export/Save/Download to appear
        time.sleep(1.0)
    else:
        print("   • 'Load' control not visible; proceeding to export search directly (some builds auto-load).")

    # Hook the blob exporter BEFORE we click Export/Save
    driver.execute_script("""
        window.__ycs = window.__ycs || [];
        const _origCreateURL = URL.createObjectURL;
        URL.createObjectURL = function(blob) {
            try { blob.text().then(t => window.__ycs.push(t)); } catch(e) {}
            return _origCreateURL.apply(this, arguments);
        };
    """)

    # Wait (up to 2 min) for an Export/Save/Download control to appear, then click it.
    export_btn = None
    deadline = time.time() + 120
    while time.time() < deadline and not export_btn:
        export_btn = _find_btn_by_text(driver, ["export", "save all comments", "save comments", "download"])
        if export_btn:
            break
        time.sleep(1.0)

    if not export_btn:
        # As a final try, open/expand any YCS panel and search again once
        driver.execute_script("""
            const toggles = [
              '[id^="ycs_toggle"]',
              ".ycs-app-main button",
              "[class*='ycs'][role='button']"
            ];
            for (const s of toggles) {
                const el = document.querySelector(s);
                if (el) { try { el.click(); } catch(e) {} }
            }
        """)
        time.sleep(0.6)
        export_btn = _find_btn_by_text(driver, ["export", "save all comments", "save comments", "download"])

    if not export_btn:
        raise RuntimeError("YCS page controls not found: no Export/Save/Download after loading.")
    
    print("   • Clicking 'Save all comments'…")

    btn = export_btn
    if not btn:
        # 3) Find a "Save/Export comments" control: try TEXT FIRST, then strict selectors
        btn = driver.execute_script("""
            // Prefer text: works better for DevTools-console builds
            const wants = ['save all comments','save comments','export','download'];
            const elems = Array.from(document.querySelectorAll('button,[role=button],a'));
            for (const el of elems) {
                const t = (el.innerText||'').trim().toLowerCase();
                if (!t) continue;
                if (wants.some(w => t.includes(w))) return el;
            }
            // Fallback to strict selectors (classic YCS UI)
            const sel = [
              "#ycs_save_all_comments",
              "button#ycs_save_all_comments",
              "button[id*='ycs'][id*='save']",
              "button[data-ycs*='save']",
              "[id^='ycs_btn_save']",
              "[class*='ycs'][role='button']"
            ];
            for (const s of sel) {
                const el = document.querySelector(s);
                if (el) return el;
            }
            return null;
        """)
    
    if not btn:
        # Try toggling any YCS console/panel then search again
        driver.execute_script("""
            const toggles = [
              '[id^="ycs_toggle"]',
              ".ycs-app-main button",
              "[class*='ycs'][role='button']"
            ];
            for (const s of toggles) {
                const el = document.querySelector(s);
                if (el) { try { el.click(); } catch(e) {} }
            }
        """)
        time.sleep(0.6)
        btn = driver.execute_script("""
            const wants = ['save all comments','save comments','export','download'];
            const elems = Array.from(document.querySelectorAll(
              "#ycs_save_all_comments, button#ycs_save_all_comments, button[id*='ycs'][id*='save'], button[data-ycs*='save'], [id^='ycs_btn_save'], [class*='ycs'][role='button'], button,[role=button],a"
            ));
            for (const el of elems) {
                const t = (el.innerText||'').trim().toLowerCase();
                if (!t) continue;
                if (wants.some(w => t.includes(w))) return el;
            }
            return null;
        """)

    # 4) Click the control and wait for the exported blob text
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
    driver.execute_script("arguments[0].click();", btn)

    try:
        WebDriverWait(driver, 30).until(
            lambda d: d.execute_script("return (window.__ycs && window.__ycs.length) ? true : false;")
        )
        raw_dump = driver.execute_script("return window.__ycs.pop();")
        print("   ✓ Captured comments blob.")
    
    except TimeoutException:
        print("   • Blob hook didn’t fire; reading downloaded export instead…")
        raw_dump = _wait_for_download_text(timeout=120)
        print("   ✓ Read downloaded export.")

    # Every block in the extension dump starts with "#####"
    blocks = [b.strip() for b in raw_dump.split("#####") if b.strip()]
    tops, children = [], []
    for blk in blocks:
        res = parse_comment_block(blk)
        if res:
            p, rs = res
            tops.append(p)
            children.extend(rs)
    parsed = tops + children

    print(f"   ✓ Parsed {len(parsed)} comments (incl. replies).")

    vid = parse_qs(urlparse(url).query).get("v", [""])[0]
    return vid, full_title, parsed

# Sentiment analysis using VADER

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Create Global VADER object for repeated Sentiment Analysis
_VADER = SentimentIntensityAnalyzer()

class Row(NamedTuple):
    video_id   : str
    video      : str
    comment_id : str
    parent_id  : Optional[str] # No parent id required for parent comments
    is_reply   : bool
    username   : str
    comment    : str
    likes      : int
    replies    : int
    polarity   : float
    label      : Literal["positive", "negative", "neutral"]

def _score(txt: str) -> float:
    """Return VADER 'compound' score within the range of -1 to +1."""
    return _VADER.polarity_scores(txt)["compound"]

def score_comments(video_id: str, video_title: str, comments: list[dict]) -> list[Row]:
    texts = [c["comment"] for c in comments]
    with concurrent.futures.ThreadPoolExecutor(max_workers=POOL_WORKERS) as pool:
        polarities = list(pool.map(_score, texts))

    rows: list[Row] = []
    for c, p in zip(comments, polarities):
        label = "positive" if p >= 0.05 else "negative" if p <= -0.05 else "neutral"
        rows.append(Row(
            video_id      = video_id,
            video         = video_title,
            comment_id    = c["comment_url"],
            parent_id     = c.get("parent_id"),
            is_reply      = bool(c.get("parent_id")),
            username      = c["username"] if usernameToggle else "",
            comment       = c["comment"],
            likes         = c["likes"],
            replies       = c["replies"],
            polarity      = round(p, 4),
            label         = label
        ))
    return rows

# --- X/Twitter ingestion via twscrape (async) ---
def _label_from_polarity(p: float) -> Literal["positive","negative","neutral"]:
    return "positive" if p >= 0.05 else "negative" if p <= -0.05 else "neutral"

async def _search_x_async(queries: list[str], limit: int = 200) -> list[dict]:
    # local import so the rest of the script runs even if twscrape isn't installed
    from twscrape import API, gather
    api = API()  # uses default account DB (~/.twscrape/accounts.db)
    rows = []
    for q in queries:
        tweets = await gather(api.search(q, limit=limit))
        for t in tweets:
            # twscrape uses SNScrape-like models; these attrs are stable:
            # t.id, t.rawContent, t.user.username, t.replyCount, t.likeCount, t.url
            text = t.rawContent or ""
            pol = _VADER.polarity_scores(text)["compound"]
            label = _label_from_polarity(pol)

            # zero-shot (optional): improves 'label' + adds ethics + sarcasm
            zs = zs_classify(text)
            zsent = zs.get("sentiment", label)
            ethics = zs.get("ethics", [])
            sarcasm = bool(zs.get("sarcasm", False))

            rows.append({
                "video_id"   : f"search:{q}",         # reuse schema, treat query like a "video"
                "video"      : f"X: {q}",
                "comment_id" : f"https://x.com/i/web/status/{t.id}",
                "parent_id"  : None,
                "is_reply"   : getattr(t, "inReplyToTweetId", None) is not None,
                "username"   : t.user.username if getattr(t, "user", None) else "",
                "comment"    : text,
                "likes"      : int(getattr(t, "likeCount", 0) or 0),
                "replies"    : int(getattr(t, "replyCount", 0) or 0),
                "polarity"   : float(pol),
                "label"      : zsent if zsent in ("positive","neutral","negative") else label,
                # extra fields for UI enrichments:
                "ethics_privacy":    "privacy" in ethics,
                "ethics_bias":       "bias" in ethics,
                "ethics_surveillance":"surveillance" in ethics,
                "ethics_accountability":"accountability" in ethics,
                "sarcasm": sarcasm,
                "posted": getattr(t, "date", None).isoformat() if getattr(t, "date", None) else "",
                "_platform": "X"
            })
    return rows

def fetch_x_rows(queries: list[str], limit: int = 200) -> list[dict]:
    if not queries:
        return []
    return asyncio.run(_search_x_async(queries, limit))

# Creating a Vega-Lite Dashboard from scratch based on the data scrapped

def make_dashboard(rows: list[Row] | list[dict], out_html: Path) -> None:
    import html, json as _json

    # Convert to dicts, enrich with platform + ethics flags for UI
    data = []
    for r in rows:
        d = r._asdict() if hasattr(r, "_asdict") else dict(r)
        # Platform inference: X tweets use x.com links; YT otherwise
        cid = d.get("comment_id","")
        platform = "X" if ("x.com/" in cid or "twitter.com/" in cid) else "YouTube"
        d["_platform"] = d.get("_platform", platform)

        # Safe defaults for ethics fields (so spec always has booleans)
        for k in ("ethics_privacy","ethics_bias","ethics_surveillance","ethics_accountability","sarcasm"):
            d.setdefault(k, False)

        data.append(d)

    video_opts = sorted({d["video"] for d in data})
    video_opts_display = ['All'] + video_opts

    vl_spec = {
      "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
      "config": {"view": {"stroke": "transparent"}, "legend": {"orient": "bottom"}, "range": {"category": {"scheme": "tableau10"}}},
      "vconcat": [
        {
          "hconcat": [
            {
              "title": "Sentiment distribution by platform",
              "width": 480, "height": 300,
              "data": {"name": "comments"},
              "transform": [
                {"filter": "video_pick=='All' || datum.video == video_pick"},
                {"filter": "length(platform_pick)==0 || indexof(platform_pick, datum._platform) >= 0"}
              ],
              "mark": {"type": "bar", "cornerRadiusEnd": 2},
              "encoding": {
                "x": {"field": "label", "type": "nominal", "sort": ["negative","neutral","positive"]},
                "y": {"aggregate": "count", "title": "Comments"},
                "color": {"field": "_platform", "type": "nominal", "title": "Platform"},
                "tooltip": [{"aggregate": "count", "title": "Count"}, {"field": "_platform", "title": "Platform"}]
              }
            },
            {
              "title": "Polarity histogram (filterable)",
              "width": 520, "height": 300,
              "data": {"name": "comments"},
              "transform": [
                {"filter": "video_pick=='All' || datum.video == video_pick"},
                {"filter": "length(platform_pick)==0 || indexof(platform_pick, datum._platform) >= 0"},
                {"filter": "length(label_pick)==0 || indexof(label_pick, datum.label) >= 0"}
              ],
              "mark": "bar",
              "encoding": {
                "x": {"field": "polarity", "type": "quantitative", "bin": {"maxbins": 40}},
                "y": {"aggregate": "count", "title": "Comments"},
                "color": {"field": "label", "type": "nominal", "title": "Sentiment"}
              }
            }
          ],
          "resolve": {"scale": {"color": "independent"}}
        },
        {
          "hconcat": [
            {
              "title": "Ethics facet counts",
              "width": 500, "height": 280,
              "data": {"name": "comments"},
              "transform": [
                {"filter": "video_pick=='All' || datum.video == video_pick"},
                {"filter": "length(platform_pick)==0 || indexof(platform_pick, datum._platform) >= 0"},
                {"fold": ["ethics_privacy","ethics_bias","ethics_surveillance","ethics_accountability"], "as": ["ethic","flag"]},
                {"filter": "datum.flag == true"}
              ],
              "mark": "bar",
              "encoding": {
                "x": {"field": "ethic", "type": "nominal", "title": "Concern"},
                "y": {"aggregate": "count", "title": "Mentions"},
                "color": {"field": "ethic", "type": "nominal", "legend": None}
              }
            },
            {
              "title": "Time trend (by posted string if available)",
              "width": 500, "height": 280,
              "data": {"name": "comments"},
              "transform": [
                {"filter": "video_pick=='All' || datum.video == video_pick"},
                {"filter": "length(platform_pick)==0 || indexof(platform_pick, datum._platform) >= 0"},
                {"calculate": "datum.posted || ''", "as": "posted_str"}
              ],
              "mark": {"type": "line", "point": True},
              "encoding": {
                "x": {"field": "posted_str", "type": "ordinal", "title": "Posted (as scraped)"},
                "y": {"aggregate": "count", "title": "Comments"},
                "color": {"field": "_platform", "type": "nominal", "title": "Platform"}
              }
            }
          ]
        },
        {
          "title": "Comments (filterable)",
          "width": 1024, "height": 360,
          "data": {"name": "comments"},
          "transform": [
            {"filter": "video_pick=='All' || datum.video == video_pick"},
            {"filter": "length(platform_pick)==0 || indexof(platform_pick, datum._platform) >= 0"},
            {"filter": "length(label_pick)==0 || indexof(label_pick, datum.label) >= 0"}
          ],
          "mark": "rect",
          "transform": [],
          "encoding": {},
          "params": []
        }
      ],
      "params": [
        {"name": "video_pick", "value": "All",
         "bind": {"input": "select", "options": video_opts, "labels": video_opts_display, "name": "Video: "}},
        {"name": "platform_pick", "value": [], "bind": {"input": "select", "options": ["YouTube","X"], "multiple": True, "name": "Platform: "}},
        {"name": "label_pick", "value": [], "bind": {"input": "select", "options": ["negative","neutral","positive"], "multiple": True, "name": "Sentiment: "}}
      ],
      "datasets": {"comments": data}
    }

    html_tpl = f"""<!DOCTYPE html>
<html lang="en"><head>
  <meta charset="utf-8" />
  <title>AI Sentiment Dashboard</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <script src="https://cdn.jsdelivr.net/npm/vega@5"></script>
  <script src="https://cdn.jsdelivr.net/npm/vega-lite@5"></script>
  <script src="https://cdn.jsdelivr.net/npm/vega-embed@6"></script>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 16px; }}
    .wrap {{ max-width: 1100px; margin: auto; }}
    .footer {{ margin-top: 16px; color: #666; font-size: 12px; }}
  </style>
</head>
<body>
<div class="wrap">
  <h1>AI Sentiment & Ethics Dashboard</h1>
  <div id="vis"></div>
  <div class="footer">Colour-blind friendly palette; filters persist; published via GitHub Pages.</div>
</div>
<script>
  const spec = { _json.dumps(vl_spec) };
  vegaEmbed("#vis", spec, {{ "actions": true }});
</script>
</body></html>"""
    out_html.write_text(html_tpl, encoding="utf-8")
    print(f"\nDashboard written to: {out_html.resolve()}")

# Main process caller function

def process_videos(urls: list[str]) -> None:
    print("Normalising URLs.")
    seen, duplicates, malformed = set(), [], []
    canon: list[str] = []
    for u in urls:
        n = normalize_url(u)
        if not n:
            malformed.append(u)
        elif n in seen:
            duplicates.append(n)
        else:
            seen.add(n)
            canon.append(n)

    if malformed:
        print("\nIgnored malformed:", malformed)
    if duplicates:
        print("\nDuplicates removed:", duplicates)

    print("\nChecking reachability.")
    reachable = [u for u in canon if is_url_reachable(u)]
    unreachable = set(canon) - set(reachable)
    if unreachable:
        print("\nInaccessible:", unreachable)
    if not reachable:
        print("Nothing to do.")
        return

    print(f"\nScraping {len(reachable)} video(s).")

    driver = make_driver()
    all_rows: list[Row] = []

    try:
        for u in reachable:
            print("->", u)
            vid, title, comments = get_comments(driver, u)
            print(f"   {len(comments)} raw comments")
            all_rows.extend(score_comments(vid, title, comments))
    finally:
        driver.quit()

    print(f"\nTotal comments processed: {len(all_rows)}")
    return all_rows


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="YouTube + X sentiment pipeline")
    parser.add_argument("--yt", nargs="*", help="One or more YouTube video URLs")
    parser.add_argument("--x-query", action="append", help="Add a Twitter/X search query (repeatable)")
    parser.add_argument("--x-limit", type=int, default=200, help="Max tweets per query (default 200)")
    args = parser.parse_args()

    all_rows = []
    if args.yt:
        yt_rows = process_videos(args.yt) or []
        all_rows.extend(yt_rows)
    if args.x_query:
        x_rows = fetch_x_rows(args.x_query, limit=args.x_limit)
        all_rows.extend(x_rows)

    if not all_rows:
        print("No inputs provided. Example:\n  python sentiment_dashboard_prototype.py --yt https://www.youtube.com/watch?v=cpcfdwnf4M8 --x-query \"AI ethics\"")
    else:
        make_dashboard(all_rows, Path("sentiment_dashboard.html"))