import json
import os
import re
import threading
import hashlib
import requests as req_http
import feedparser
from datetime import datetime, timezone
from collections import Counter
from flask import Flask, jsonify, render_template
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# ── USGS direct-fetch background refresh ─────────────────────────────────
# Memastikan live_api.json selalu ter-update langsung dari USGS setiap 5 menit
# sebagai pelengkap pipeline Kafka (producer → Kafka → consumer → HDFS → live_api.json)
USGS_URL = (
    "https://earthquake.usgs.gov/fdsnws/event/1/query"
    "?format=geojson"
    "&minlatitude=-11&maxlatitude=6"
    "&minlongitude=95&maxlongitude=141"
    "&minmagnitude=2"
    "&orderby=time"
    "&limit=100"
)
_usgs_lock = threading.Lock()
_usgs_last_fetch = 0  # epoch seconds

def _classify_magnitude(mag):
    if mag is None: return "unknown"
    if mag < 3: return "mikro"
    if mag < 4: return "minor"
    if mag < 5: return "sedang"
    return "kuat"

def _classify_depth(d):
    if d is None: return "unknown"
    if d < 70: return "dangkal"
    if d < 300: return "menengah"
    return "dalam"

def _fetch_usgs_and_save():
    """Fetch langsung dari USGS API dan update live_api.json."""
    global _usgs_last_fetch
    try:
        r = req_http.get(USGS_URL, timeout=15,
                         headers={"User-Agent": "GempaRadar/1.0"})
        r.raise_for_status()
        features = r.json().get("features", [])
        now_iso = datetime.now(timezone.utc).isoformat()
        events = []
        for f in features:
            p  = f.get("properties", {})
            c  = f.get("geometry", {}).get("coordinates", [None, None, None])
            rt = p.get("time")
            if rt:
                from datetime import datetime as _dt
                et = _dt.fromtimestamp(rt / 1000, tz=timezone.utc).isoformat()
            else:
                et = now_iso
            events.append({
                "id": f.get("id", ""),
                "source": "usgs_api",
                "timestamp": now_iso,
                "event_time": et,
                "event_time_epoch": rt,
                "magnitude": p.get("mag"),
                "magnitude_type": p.get("magType", ""),
                "depth_km": c[2],
                "longitude": c[0],
                "latitude": c[1],
                "place": p.get("place", ""),
                "status": p.get("status", ""),
                "alert": p.get("alert"),
                "tsunami": p.get("tsunami", 0),
                "felt": p.get("felt"),
                "sig": p.get("sig", 0),
                "url": p.get("url", ""),
                "mag_category": _classify_magnitude(p.get("mag")),
                "depth_category": _classify_depth(c[2]),
            })
        if events:
            path = os.path.join(DATA_DIR, "live_api.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(events, f, ensure_ascii=False)
            with _usgs_lock:
                _usgs_last_fetch = __import__('time').time()
            print(f"[USGS refresh] {len(events)} events saved — {now_iso[:19]}")
    except Exception as e:
        print(f"[USGS refresh] Gagal: {e}")

def _usgs_background_loop():
    import time as _time
    _time.sleep(5)  # tunggu Flask startup
    while True:
        _fetch_usgs_and_save()
        _time.sleep(300)  # refresh setiap 5 menit

# Start background USGS refresh thread
_usgs_thread = threading.Thread(target=_usgs_background_loop, daemon=True)
_usgs_thread.start()

# ── RSS direct-fetch background refresh ──────────────────────────────────
RSS_URL = "https://news.google.com/rss/search?q=gempa+indonesia&hl=id&gl=ID&ceid=ID:id"
RSS_MAX_ITEMS = 50
_rss_lock = threading.Lock()

def _fetch_rss_and_save():
    """Fetch langsung dari Google News RSS dan update live_rss.json."""
    try:
        feed = feedparser.parse(RSS_URL)
        if not feed.entries:
            print("[RSS refresh] Tidak ada entri ditemukan")
            return
        now_iso = datetime.now(timezone.utc).isoformat()

        # Baca data lama untuk de-duplikasi
        path = os.path.join(DATA_DIR, "live_rss.json")
        try:
            with open(path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            existing = []

        existing_ids = {item.get("id") for item in existing}
        new_items = []

        for entry in feed.entries:
            url = entry.get("link", "")
            item_id = hashlib.md5(url.encode("utf-8")).hexdigest()[:8]
            if item_id in existing_ids:
                continue

            # Parse waktu publikasi
            pub_time = now_iso
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                try:
                    from datetime import datetime as _dt
                    pub_time = _dt(*entry.published_parsed[:6], tzinfo=timezone.utc).isoformat()
                except Exception:
                    pass

            new_items.append({
                "id": item_id,
                "source": "Google News (Gempa Indonesia)",
                "source_priority": 1,
                "timestamp": now_iso,
                "published_time": pub_time,
                "title": entry.get("title", ""),
                "url": url,
                "summary": entry.get("summary", ""),
                "tags": [],
                "feed_url": RSS_URL,
            })

        if new_items:
            combined = new_items + existing
            combined = combined[:RSS_MAX_ITEMS]  # batasi jumlah
            with _rss_lock:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(combined, f, ensure_ascii=False, indent=4)
            print(f"[RSS refresh] {len(new_items)} berita baru ditambahkan — total {len(combined)}")
        else:
            print(f"[RSS refresh] Tidak ada berita baru — total existing {len(existing)}")
    except Exception as e:
        print(f"[RSS refresh] Gagal: {e}")

def _rss_background_loop():
    import time as _time
    _time.sleep(10)  # tunggu setelah Flask startup
    while True:
        _fetch_rss_and_save()
        _time.sleep(300)  # refresh setiap 5 menit

# Start background RSS refresh thread
_rss_thread = threading.Thread(target=_rss_background_loop, daemon=True)
_rss_thread.start()

# ── Image cache: url → image_url string (empty string = no image found) ──
_img_cache = {}
_img_lock  = threading.Lock()

_OG_RE = [
    re.compile(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', re.I),
    re.compile(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']', re.I),
    re.compile(r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']', re.I),
    re.compile(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']twitter:image["\']', re.I),
]
_CANON_RE = [
    re.compile(r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']([^"\']+)["\']', re.I),
    re.compile(r'<link[^>]+href=["\']([^"\']+)["\'][^>]+rel=["\']canonical["\']', re.I),
]
# Images from these domains are Google's own assets, not article images
_GOOGLE_IMAGE_DOMAINS = ('google.com', 'gstatic.com', 'googleapis.com', 'googleusercontent.com')

def _is_google_url(url: str) -> bool:
    return any(d in url for d in ('news.google.com', 'google.com/url',))

def _fetch_og_image(url: str) -> str:
    """Fetch the article's og:image, properly resolving Google News redirects."""
    _headers_fb = {"User-Agent": "facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_ufi.php)"}
    _headers_bot = {"User-Agent": "Googlebot/2.1 (+http://www.google.com/bot.html)"}
    try:
        r = req_http.get(url, timeout=8, allow_redirects=True, headers=_headers_fb)
        actual_url = r.url
        # If we're still on Google (Google served its own wrapper page), resolve the actual article URL
        if _is_google_url(actual_url) or _is_google_url(url):
            # Try canonical link in the Google page
            for pat in _CANON_RE:
                m = pat.search(r.text)
                if m:
                    canon = m.group(1).strip()
                    if canon.startswith('http') and not _is_google_url(canon):
                        r = req_http.get(canon, timeout=6, allow_redirects=True, headers=_headers_bot)
                        actual_url = r.url
                        break
            else:
                # Could not resolve — skip
                return ""
        # Extract og:image, filtering out any Google-served images
        for pat in _OG_RE:
            m = pat.search(r.text)
            if m:
                img = m.group(1).strip()
                if (img.startswith('http')
                        and not any(d in img for d in _GOOGLE_IMAGE_DOMAINS)
                        and len(img) < 600):
                    return img
    except Exception:
        pass
    return ""

def _prefetch_images(urls: list):
    """Background-fetch og:images and persist results to live_rss.json."""
    rss_path = os.path.join(DATA_DIR, "live_rss.json")
    for url in urls:
        with _img_lock:
            if url in _img_cache:
                continue
        result = _fetch_og_image(url)
        with _img_lock:
            _img_cache[url] = result
        # Persist to RSS JSON so images survive Flask restarts
        if result:
            try:
                with _rss_lock:
                    with open(rss_path, 'r', encoding='utf-8') as f:
                        rss = json.load(f)
                    changed = False
                    for item in rss:
                        if item.get('url') == url and not item.get('image_url'):
                            item['image_url'] = result
                            changed = True
                            break
                    if changed:
                        with open(rss_path, 'w', encoding='utf-8') as f:
                            json.dump(rss, f, ensure_ascii=False, indent=4)
            except Exception:
                pass


def read_json(filename):
    path = os.path.join(DATA_DIR, filename)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def compute_stats_from_api(api_data):
    """Hitung statistik dari live_api.json sebagai fallback jika spark_results.json belum ada."""
    if not api_data:
        return {}

    total = len(api_data)
    magnitudes = [d.get("magnitude", 0) for d in api_data if d.get("magnitude") is not None]
    depths = [d.get("depth_km", 0) for d in api_data if d.get("depth_km") is not None]

    # Distribusi magnitudo
    mag_dist = {"Mikro (<3)": 0, "Minor (3-4)": 0, "Sedang (4-5)": 0, "Kuat (>5)": 0}
    for m in magnitudes:
        if m < 3:
            mag_dist["Mikro (<3)"] += 1
        elif m < 4:
            mag_dist["Minor (3-4)"] += 1
        elif m < 5:
            mag_dist["Sedang (4-5)"] += 1
        else:
            mag_dist["Kuat (>5)"] += 1

    # Top 10 wilayah
    places = []
    for d in api_data:
        place = d.get("place", "")
        # Ambil bagian setelah "of " jika ada
        if " of " in place:
            place = place.split(" of ", 1)[1]
        places.append(place)
    top_wilayah = Counter(places).most_common(10)

    # Distribusi kedalaman
    dangkal = sum(1 for d in depths if d < 70)
    menengah = sum(1 for d in depths if 70 <= d < 300)
    dalam = sum(1 for d in depths if d >= 300)
    avg_depth = round(sum(depths) / len(depths), 1) if depths else 0

    return {
        "source": "live_data",
        "total_gempa": total,
        "avg_magnitude": round(sum(magnitudes) / len(magnitudes), 2) if magnitudes else 0,
        "max_magnitude": max(magnitudes) if magnitudes else 0,
        "rata_rata_kedalaman": avg_depth,
        "distribusi_magnitudo": mag_dist,
        "top_wilayah": [{"wilayah": w, "count": c} for w, c in top_wilayah],
        "distribusi_kedalaman": {
            "Dangkal (<70 km)": dangkal,
            "Menengah (70-300 km)": menengah,
            "Dalam (>300 km)": dalam,
        },
        "wilayah_teraktif": top_wilayah[0][0] if top_wilayah else "N/A",
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/data")
def api_data():
    api_data_raw = read_json("live_api.json") or []
    rss_data = read_json("live_rss.json") or []
    spark_results = read_json("spark_results.json")

    # Selalu hitung ulang dari live data agar statistik selalu fresh
    live_stats = compute_stats_from_api(api_data_raw)
    if live_stats:
        spark_results = live_stats
        spark_results["note"] = "Data dihitung dari live USGS feed (auto-refresh setiap 5 menit)"
    elif not spark_results or spark_results.get("total_gempa", 0) == 0:
        spark_results = {"note": "Belum ada data"}

    # Sort gempa: terbaru dulu, return all for tab filtering on frontend
    api_data_sorted = sorted(api_data_raw, key=lambda x: x.get("event_time", ""), reverse=True)

    # Sort berita: terbaru dulu
    rss_data_sorted = sorted(
        rss_data,
        key=lambda x: x.get("published_time", x.get("timestamp", "")),
        reverse=True,
    )

    # Enrich berita with cached og:image; trigger background prefetch for missing
    news_items = []
    urls_to_fetch = []
    for item in rss_data_sorted[:15]:
        url = item.get("url", "")
        # Check in-memory cache first; fall back to persisted value in RSS JSON
        with _img_lock:
            cached = _img_cache.get(url)
        if cached is None:
            persisted = item.get("image_url") or ""
            if persisted:
                with _img_lock:
                    _img_cache[url] = persisted
                item = dict(item, image_url=persisted)
            else:
                urls_to_fetch.append(url)
                item = dict(item, image_url="")
        else:
            item = dict(item, image_url=cached)
        news_items.append(item)

    if urls_to_fetch:
        t = threading.Thread(target=_prefetch_images, args=(urls_to_fetch,), daemon=True)
        t.start()

    return jsonify({
        "gempa_terbaru": api_data_sorted[:20],
        "gempa_all": api_data_sorted,          # full list for tab filters
        "berita_terbaru": news_items,
        "spark_results": spark_results,
        "server_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    })


@app.route("/api/stats")
def api_stats():
    """Endpoint ringkas untuk chart data."""
    api_data = read_json("live_api.json") or []
    stats = compute_stats_from_api(api_data)
    return jsonify(stats)


if __name__ == "__main__":
    print("=" * 50)
    print("  GempaRadar Dashboard")
    print("  Buka: http://localhost:5000")
    print("=" * 50)
    app.run(host="0.0.0.0", port=5000, debug=False)
