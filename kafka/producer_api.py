import json
import time
import logging
import hashlib
from datetime import datetime, timezone
 
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
 
# ── Konfigurasi ───────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC_NAME = "gempa-api"
POLLING_INTERVAL_SECONDS = 30      # 30 detik (untuk demo)
 
# USGS FDSN API — bounding box seluruh wilayah Indonesia
USGS_API_URL = (
    "https://earthquake.usgs.gov/fdsnws/event/1/query"
    "?format=geojson"
    "&minlatitude=-11"
    "&maxlatitude=6"
    "&minlongitude=95"
    "&maxlongitude=141"
    "&minmagnitude=2"
    "&orderby=time"
    "&limit=100"
)
 
# ── Logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("producer_api")
 
 
# ── Helper: inisialisasi Kafka Producer ──────────────────────────────────
def create_producer() -> KafkaProducer:
    """
    [NamaAnggota2]: Buat Kafka Producer dengan konfigurasi idempoten
    agar tidak ada event yang hilang atau duplikat di sisi broker.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        # Serialisasi key dan value sebagai UTF-8 bytes
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=5,
        retry_backoff_ms=500,
        # Kompresi untuk efisiensi jaringan
        compression_type="gzip",
        # Timeout koneksi
        request_timeout_ms=30000,
    )
 
 
# ── Helper: fetch dari USGS ───────────────────────────────────────────────
def fetch_usgs_earthquakes() -> list[dict]:
 
    try:
        response = requests.get(USGS_API_URL, timeout=15)
        response.raise_for_status()
        geojson = response.json()
    except requests.RequestException as e:
        log.error("Gagal fetch USGS API: %s", e)
        return []
 
    earthquakes = []
    features = geojson.get("features", [])
    log.info("USGS mengembalikan %d event gempa", len(features))
 
    for feature in features:
        props = feature.get("properties", {})
        coords = feature.get("geometry", {}).get("coordinates", [None, None, None])
        usgs_id = feature.get("id", "unknown")
 
        # Konversi Unix timestamp (ms) ke ISO string
        raw_time = props.get("time")  # epoch milliseconds
        if raw_time:
            dt = datetime.fromtimestamp(raw_time / 1000, tz=timezone.utc)
            event_time_iso = dt.isoformat()
            event_time_epoch = raw_time
        else:
            event_time_iso = datetime.now(timezone.utc).isoformat()
            event_time_epoch = int(time.time() * 1000)
 
        # ── Struktur event JSON yang konsisten ────────────────────────────
        event = {
            # Identifikasi
            "id": usgs_id,
            "source": "usgs_api",
            # Waktu
            "timestamp": datetime.now(timezone.utc).isoformat(),   # waktu ingestion
            "event_time": event_time_iso,                          # waktu gempa sebenarnya
            "event_time_epoch": event_time_epoch,
            # Parameter seismik
            "magnitude": props.get("mag"),
            "magnitude_type": props.get("magType", ""),
            "depth_km": coords[2] if coords[2] is not None else None,
            # Lokasi
            "longitude": coords[0],
            "latitude": coords[1],
            "place": props.get("place", "Unknown"),
            # Status & metadata
            "status": props.get("status", ""),
            "alert": props.get("alert"),        # bisa null, green, yellow, orange, red
            "tsunami": props.get("tsunami", 0),
            "felt": props.get("felt"),
            "sig": props.get("sig", 0),         # significance score USGS
            "url": props.get("url", ""),
            # Klasifikasi magnitudo (berguna untuk Spark)
            "mag_category": classify_magnitude(props.get("mag")),
            # Klasifikasi kedalaman
            "depth_category": classify_depth(coords[2]),
        }
        earthquakes.append(event)
 
    return earthquakes
 
 
def classify_magnitude(mag) -> str:
   
    if mag is None:
        return "unknown"
    if mag < 3.0:
        return "mikro"
    elif mag < 4.0:
        return "minor"
    elif mag < 5.0:
        return "sedang"
    else:
        return "kuat"
 
 
def classify_depth(depth_km) -> str:
  
    if depth_km is None:
        return "unknown"
    if depth_km < 70:
        return "dangkal"
    elif depth_km < 300:
        return "menengah"
    else:
        return "dalam"
 
 
# ── Helper: deduplikasi antar polling cycle ───────────────────────────────
class SeenEventTracker:

    def __init__(self, ttl_seconds: int = 3600):
        self._seen: dict[str, float] = {}   # id -> timestamp saat dilihat
        self._ttl = ttl_seconds
 
    def is_new(self, event_id: str) -> bool:
        now = time.time()
        # Bersihkan entry lama
        expired = [k for k, v in self._seen.items() if now - v > self._ttl]
        for k in expired:
            del self._seen[k]
        # Cek apakah baru
        if event_id in self._seen:
            return False
        self._seen[event_id] = now
        return True
 
    @property
    def count(self) -> int:
        return len(self._seen)
 
 
# ── Callback Kafka ─────────────────────────────────────────────────────────
def on_send_success(record_metadata):
    log.debug(
        "✓ Terkirim → topic=%s | partition=%d | offset=%d",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )
 
 
def on_send_error(exc):
    log.error("✗ Gagal kirim ke Kafka: %s", exc)
 
 
# ── Main Loop ─────────────────────────────────────────────────────────────
def main():
    log.info("=" * 55)
    log.info("  GempaRadar — Producer API (USGS)")
    log.info("  Topic     : %s", TOPIC_NAME)
    log.info("  Interval  : %d detik (%d menit)", POLLING_INTERVAL_SECONDS, POLLING_INTERVAL_SECONDS // 60)
    log.info("=" * 55)
 
    producer = create_producer()
    tracker = SeenEventTracker(ttl_seconds=30)   # 30 detik — agar setiap cycle kirim semua event terbaru ke Kafka
    cycle = 0
 
    try:
        while True:
            cycle += 1
            log.info("── Cycle #%d | %s ──", cycle, datetime.now().strftime("%H:%M:%S"))
 
            earthquakes = fetch_usgs_earthquakes()
            sent_count = 0
            skip_count = 0
 
            for quake in earthquakes:
                event_id = quake["id"]
 
                # Skip jika event ini sudah dikirim dalam 1 jam terakhir
                if not tracker.is_new(event_id):
                    skip_count += 1
                    continue
 
                # Key = ID gempa USGS (unik per event)
                message_key = event_id
 
                producer.send(
                    TOPIC_NAME,
                    key=message_key,
                    value=quake,
                ).add_callback(on_send_success).add_errback(on_send_error)
 
                log.info(
                    "  → [%s] M%.1f | %s | depth=%.1f km | %s",
                    event_id,
                    quake["magnitude"] or 0,
                    quake["mag_category"],
                    quake["depth_km"] or 0,
                    quake["place"][:50],
                )
                sent_count += 1
 
            # Flush memastikan semua pesan terkirim sebelum sleep
            producer.flush()
            log.info(
                "  ✓ Cycle #%d selesai: %d dikirim, %d di-skip (duplikat) | total tracked: %d",
                cycle, sent_count, skip_count, tracker.count,
            )
 
            log.info("  Menunggu %d detik...\n", POLLING_INTERVAL_SECONDS)
            time.sleep(POLLING_INTERVAL_SECONDS)
 
    except KeyboardInterrupt:
        log.info("Producer dihentikan oleh user (Ctrl+C).")
    finally:
        producer.flush()
        producer.close()
        log.info("Producer ditutup dengan bersih.")
 
 
if __name__ == "__main__":
    main()
