import json
import os
import re
import threading
import requests as req_http
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

# ── Image cache: url → image_url string (empty string = no image found) ──
_img_cache = {}
_img_lock  = threading.Lock()

_OG_RE = [
    re.compile(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', re.I),
    re.compile(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']', re.I),
    re.compile(r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']', re.I),
    re.compile(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']twitter:image["\']', re.I),
]

def _fetch_og_image(url: str) -> str:
    try:
        r = req_http.get(
            url, timeout=6, allow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; GempaRadar/1.0)"},
        )
        for pat in _OG_RE:
            m = pat.search(r.text)
            if m:
                img = m.group(1).strip()
                if img.startswith("http"):
                    return img
    except Exception:
        pass
    return ""

def _prefetch_images(urls: list):
    for url in urls:
        with _img_lock:
            if url in _img_cache:
                continue
        result = _fetch_og_image(url)
        with _img_lock:
            _img_cache[url] = result


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

    if not spark_results or spark_results.get("total_gempa", 0) == 0 or spark_results.get("source") == "placeholder":
        spark_results = compute_stats_from_api(api_data_raw)
        spark_results["note"] = "Data dihitung dari live feed (Spark belum dijalankan)"

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
        with _img_lock:
            cached = _img_cache.get(url)
        if cached is None:
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
