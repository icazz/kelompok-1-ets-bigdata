# 🌏 GempaRadar: Real-Time Earthquake Monitoring System

> **ETS Big Data — Kelompok 1 | Mata Kuliah: Big Data dan Data Lakehouse**

---

## 👥 Anggota Kelompok

| NRP | Nama Lengkap | Peran |
|-----|--------------|-------|
| 5027231086 | Kharisma Fahrun Nisa` | Setup Docker (Hadoop & Kafka), buat topic, troubleshooting infrastruktur |
| 5027241079 | M. Hikari Reiziq Rakhmadinta | `dashboard/app.py` + `index.html` |
| 5027221053 | Aras Rizky Ananta | `producer_api.py` — integrasi API eksternal USGS |
| 5027241036 | Arya Bisma Putra Refman | `spark_processing.py` — 3 analisis wajib + Spark MLlib |
| 5027241058 | Ica Zika Hamizah | `producer_rss.py` + `consumer_to_hdfs.py` |

---

## 🌍 Topik: GempaRadar — Monitor Aktivitas Seismik Wilayah Indonesia

**Klien:** BPBD (Badan Penanggulangan Bencana Daerah) Provinsi

**Pertanyaan bisnis:**
> *"Di wilayah mana aktivitas gempa paling tinggi dalam periode ini, dan seberapa sering gempa signifikan (M>4) terjadi?"*

**Justifikasi:** Indonesia berada di "Ring of Fire" — zona pertemuan lempeng Indo-Australia, Eurasia, dan Pasifik — dengan frekuensi gempa tertinggi di dunia. BPBD membutuhkan sistem real-time untuk koordinasi respons kebencanaan yang cepat dan berbasis data aktual, bukan hanya laporan manual.

---

## 🏗️ Arsitektur Sistem

```
[USGS Earthquake API]      [Google News RSS]
         │                        │
         ▼                        ▼
  producer_api.py          producer_rss.py
         │                        │
         ▼                        ▼
  topic: gempa-api ──── APACHE KAFKA ──── topic: gempa-rss
                              │
                              ▼
                    consumer_to_hdfs.py
                    (Python hdfs library)
                              │
                              ▼
              ╔═══════════════════════════╗
              ║       HADOOP HDFS         ║
              ║  /data/gempa/api/*.json   ║
              ║  /data/gempa/rss/*.json   ║
              ╚═══════════╤═══════════════╝
                          │
                          ▼
               spark_processing.py
               (Batch Analysis + MLlib)
               [otomatis via spark_runner.py]
                          │
              ┌───────────┴───────────┐
              ▼                       ▼
   /data/gempa/hasil/      spark_results.json
   (HDFS output)           (Dashboard input)
                                      │
                                      ▼
                          dashboard/app.py (Flask)
                          + Live USGS refresh (tiap 5 mnt)
                                      │
                                      ▼
                            localhost:5000
```

---

## ✨ Optimalisasi & Perbaikan

Selama pengerjaan proyek ini, sejumlah masalah teknis ditemukan dan diselesaikan. Berikut penjelasan lengkap setiap optimalisasi yang dilakukan.

### 1. Fix DNS `kafka-broker` di Windows Host (Socket Monkey-Patch)

**Masalah:** Kafka broker mengembalikan metadata dengan hostname internal Docker (`kafka-broker:9092`). Dari Windows host, nama ini tidak bisa di-resolve sehingga producer dan consumer gagal terhubung.

**Solusi:** Menambahkan socket monkey-patch di awal semua script Kafka tanpa perlu mengubah `hosts` file atau konfigurasi Docker:

```python
import socket
_orig_getaddrinfo = socket.getaddrinfo
def _patched_getaddrinfo(host, port, *args, **kwargs):
    if host == 'kafka-broker':
        host = '127.0.0.1'
    return _orig_getaddrinfo(host, port, *args, **kwargs)
socket.getaddrinfo = _patched_getaddrinfo
```

Patch ini diterapkan di `producer_api.py`, `producer_rss.py`, dan `consumer_to_hdfs.py`.

---

### 2. Otomatisasi Spark dengan `spark_runner.py`

**Masalah:** Sebelumnya, Spark harus dijalankan manual lewat `docker exec spark-master spark-submit ...` setiap kali ingin memperbarui analisis di dashboard. Ini tidak praktis untuk demo atau monitoring berkelanjutan.

**Solusi:** Membuat script baru `kafka/spark_runner.py` yang menjalankan Spark job secara otomatis setiap 10 menit via `subprocess`:

```python
INTERVAL_MINUTES = 10   # ubah sesuai kebutuhan
```

Cukup jalankan sekali, dan analisis akan diperbarui otomatis tanpa intervensi manual.

![Spark Runner berjalan — Spark job otomatis setiap 10 menit](image/spark_runner.png)

---

### 3. Pembersihan RSS Feed Mati

**Masalah:** Empat dari lima sumber RSS dikonfigurasi awal mengembalikan error atau konten tidak valid.

| Sumber | Masalah |
|--------|---------|
| BMKG | Migrasi ke XML proprietary, tidak kompatibel `feedparser` |
| Detik | HTTP 404 |
| Kompas | XML tidak valid / encoding error |
| Tempo | Menghapus endpoint RSS per-tag |
| Liputan6 | HTTP 404 |

**Solusi:** Keempat sumber dihapus. Diganti satu sumber stabil: Google News RSS
```
https://news.google.com/rss/search?q=gempa+indonesia&hl=id&gl=ID&ceid=ID:id
```

---

### 4. Fix `consumer_to_hdfs.py` — Bug `subscribe()` di Python 3.13 Windows

**Masalah:** `kafka-python-ng` pada Python 3.13 di Windows menyebabkan error `Invalid file descriptor: -1` saat menggunakan `subscribe()` karena bug pada implementasi `selectors`.

**Solusi:** Mengganti `subscribe()` dengan `assign()` + `seek_to_beginning()` sambil tetap mendaftarkan `group_id` agar consumer group tetap terlacak di broker:

```python
# Sebelum (error di Python 3.13 Windows)
consumer.subscribe(['gempa-api', 'gempa-rss'])

# Sesudah (stabil)
from kafka import TopicPartition
consumer.assign([TopicPartition('gempa-api', 0), TopicPartition('gempa-rss', 0)])
consumer.seek_to_beginning()
```

---

### 5. Fix Bug: Pin Peta 2D Tab "Berpotensi Tsunami" Tidak Muncul

**Masalah:** Tab "Berpotensi Tsunami" menampilkan 0 titik di peta meskipun ada gempa yang seharusnya masuk kriteria (contoh: M5.7 Gunungsitoli kedalaman 18 km). Penyebab: tiga bagian kode (`filterByTab`, `showDetailCard`, `renderSidebar`) menggunakan kondisi tsunami yang berbeda-beda dan tidak konsisten.

**Solusi:** Membuat satu fungsi terpusat `isTsunami(g)` di `dashboard/static/js/app.js` yang digunakan di semua bagian:

```javascript
function isTsunami(g) {
  const m = g.magnitude || 0, d = g.depth_km || 999;
  // tsunami flag dari USGS, ATAU M>=6 kedalaman<100km, ATAU M>=5.5 kedalaman<50km
  return g.tsunami == 1 || (m >= 6.0 && d < 100) || (m >= 5.5 && d < 50);
}
```

---

### 6. Fix Spark — Penghapusan Konfigurasi Delta Lake

**Masalah:** Konfigurasi Delta Lake (`io.delta`) masih tersisa di `SparkSession`, menyebabkan `ClassNotFoundException` yang menggagalkan semua Spark job karena library tidak tersedia di container.

**Solusi:** Menghapus 4 baris konfigurasi Delta dari `SparkSession`. Konfigurasi yang bersih:

```python
spark = SparkSession.builder \
    .appName("GempaRadar-Analysis") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:8020") \
    .getOrCreate()
```

---

## ⚙️ Persiapan & Setup

### Prasyarat

1. Docker Desktop terinstall dan berjalan
2. Python 3.10+ dengan virtual environment
3. Tambahkan ke `C:\Windows\System32\drivers\etc\hosts` (buka Notepad sebagai Administrator):
   ```
   127.0.0.1 datanode
   ```

### Step 1 — Install Python Dependencies

```sh
pip install -r requirements.txt
```

### Step 2 — Jalankan Infrastruktur Docker (urutan wajib)

```sh
# 1. Hadoop terlebih dahulu
docker compose -f docker-compose-hadoop.yml up -d

# 2. Kafka
docker compose -f docker-compose-kafka.yml up -d

# 3. Spark
docker compose -f docker-compose-spark.yml up -d
```

Verifikasi semua container berjalan:
```sh
docker ps
```

![Docker Desktop — semua container aktif](image/docker_desktop.png)

### Step 3 — Buat Kafka Topics (hanya pertama kali)

```sh
docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh \
  --create --topic gempa-api --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh \
  --create --topic gempa-rss --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

### Step 4 — Buat Direktori HDFS (hanya pertama kali)

```sh
docker exec hadoop-namenode hdfs dfs -mkdir -p /data/gempa/api/
docker exec hadoop-namenode hdfs dfs -mkdir -p /data/gempa/rss/
docker exec hadoop-namenode hdfs dfs -mkdir -p /data/gempa/hasil/
docker exec hadoop-namenode hdfs dfs -chmod -R 777 /data
```

---

## 🚀 Cara Menjalankan Sistem

Jalankan **masing-masing script di terminal terpisah** dari folder root proyek.

### Terminal 1 — Producer API (USGS)

```sh
python kafka/producer_api.py
```

![Producer API — monitoring event gempa dari USGS masuk ke Kafka](image/producer_api_gempa.png)

Script polling USGS FDSN API setiap 30 detik. Mengambil 100 event gempa terbaru di area Indonesia (bounding box lat -11 s/d 6, lon 95 s/d 141) dan mengirimkan setiap event sebagai JSON ke topic `gempa-api`.

---

### Terminal 2 — Producer RSS (Google News)

```sh
python kafka/producer_rss.py
```

![Producer RSS — monitoring artikel berita gempa masuk ke Kafka](image/producer_rss_gempa.png)

Script polling Google News RSS setiap 30 detik, mem-parse artikel dengan `feedparser`, dan mengirim berita baru (deduplikasi via hash URL) ke topic `gempa-rss`.

---

### Terminal 3 — Consumer to HDFS

```sh
python kafka/consumer_to_hdfs.py
```

![Consumer HDFS — buffer data dan flush ke HDFS setiap 2 menit](image/consumer_to_hdfs.png)

Consumer membaca kedua topic Kafka dan mengakumulasi data di buffer memori. Setiap **2 menit**, buffer di-flush ke HDFS sebagai file JSON bertimestamp (contoh: `/data/gempa/api/2026-05-08_15-00.json`) sekaligus disimpan lokal ke `dashboard/data/`.

---

### Terminal 4 — Spark Auto Runner

```sh
python kafka/spark_runner.py
```

![Spark Runner — Spark job analisis otomatis setiap 10 menit](image/spark_runner.png)

`spark_runner.py` menjalankan `spark_processing.py` via `docker exec` ke container `spark-master` langsung saat dijalankan, lalu mengulang setiap 10 menit secara otomatis. Output analisis mencakup:
- Distribusi magnitudo (Mikro / Minor / Sedang / Kuat)
- Top 10 wilayah paling aktif
- Distribusi & statistik kedalaman
- MLlib Linear Regression (prediksi tren magnitudo)

Hasil disimpan ke `dashboard/data/spark_results.json`.

---

### Terminal 5 — Flask Dashboard

```sh
python dashboard/app.py
```

![Flask Dashboard — server berjalan di localhost:5000](image/app_dashboard_py.png)

Dashboard tersedia di **http://localhost:5000**. Flask menjalankan background thread yang me-refresh data langsung dari USGS setiap 5 menit sebagai lapisan pelengkap pipeline Kafka.

---

## 📸 Screenshot Dashboard

### Dashboard — Globe + Statistik (localhost:5000)

![Dashboard](image/Dashboard.png)

### Peta 2D — Dark View

![Peta 2D Gelap](image/Peta_2D_Gelap_View.png)

### Peta 2D — Street View

![Peta 2D Street](image/Peta_2D_Street_View.png)

### Peta 3D (Mapbox)

![Peta 3D](image/Peta_3D.png)

### Berita Gempa

![Berita](image/Berita.png)

### HDFS Web UI — Overview (localhost:9870)

![HDFS Overview](image/overview_hdfs.png)

### HDFS Web UI — Browse Directory `/data`

![HDFS Browse](image/browse_directory_hdfs.png)

### HDFS Web UI — Browse Directory `/data/gempa`

![HDFS Browse Data](image/browse_directory_hdfs_data.png)

---

## ✅ Checklist Verifikasi End-to-End

```sh
# 1. Kafka: 2 topic aktif
docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh \
  --list --bootstrap-server localhost:9092
# Expected: gempa-api, gempa-rss

# 2. Kafka: consumer group LAG
docker exec kafka-broker /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group gempa-consumer-group

# 3. HDFS: file JSON tersimpan dengan timestamp
docker exec hadoop-namenode hdfs dfs -ls -R /data/gempa/
# Expected: file di /data/gempa/api/ dan /data/gempa/rss/

# 4. HDFS: hasil Spark
docker exec hadoop-namenode hdfs dfs -ls /data/gempa/hasil/
# Expected: distribusi_kedalaman/, distribusi_magnitudo/, top_wilayah/

# 5. Dashboard: data dari Spark
curl http://localhost:5000/api/data
# Expected: source: "spark_hdfs", total_gempa > 0
```

---

## ⚠️ Tantangan & Solusi

| Tantangan | Solusi |
|-----------|--------|
| `kafka-python-ng` bug di Python 3.13 Windows — `subscribe()` menyebabkan `Invalid file descriptor: -1` | Menggunakan `assign()` + `seek_to_beginning()` dengan manual `commit()` |
| Kafka broker hostname `kafka-broker` tidak bisa di-resolve dari Windows host | Socket monkey-patch redirect `kafka-broker` → `127.0.0.1` di semua script Python |
| BMKG/Detik/Kompas/Tempo/Liputan6 RSS error 404 atau XML tidak valid | Diganti satu sumber stabil: Google News RSS |
| Spark job gagal dengan `ClassNotFoundException` untuk Delta Lake | Hapus konfigurasi Delta dari `SparkSession` |
| Tab "Berpotensi Tsunami" menampilkan 0 pin di peta | Buat fungsi terpusat `isTsunami()` yang konsisten di seluruh kode frontend |
| Spark harus dijalankan manual setiap kali | Buat `spark_runner.py` — otomatis jalankan Spark setiap 10 menit |
| `spark_results.json` placeholder menyebabkan dashboard menampilkan nol sebelum Spark dijalankan | Fallback logic di `app.py`: hitung statistik dari `live_api.json` jika `source == "placeholder"` |

---

## 🛠️ Maintenance

### Mematikan Semua Layanan
```sh
docker compose -f docker-compose-spark.yml down
docker compose -f docker-compose-kafka.yml down
docker compose -f docker-compose-hadoop.yml down
```

### Menyalakan Ulang (urutan wajib: Hadoop → Kafka → Spark)
```sh
docker compose -f docker-compose-hadoop.yml up -d
docker compose -f docker-compose-kafka.yml up -d
docker compose -f docker-compose-spark.yml up -d
```

Setelah Docker siap, jalankan 5 script Python di terminal masing-masing:
```sh
python kafka/producer_api.py      # Terminal 1
python kafka/producer_rss.py      # Terminal 2
python kafka/consumer_to_hdfs.py  # Terminal 3
python kafka/spark_runner.py      # Terminal 4
python dashboard/app.py           # Terminal 5
```

---

## 📁 Struktur Repository

```
kelompok-1-ets-bigdata/
├── README.md
├── guide.md
├── docker-compose-hadoop.yml
├── docker-compose-kafka.yml
├── docker-compose-spark.yml
├── hadoop.env
├── requirements.txt
├── image/
│   ├── producer_api_gempa.png      ← Terminal monitoring Producer API
│   ├── producer_rss_gempa.png      ← Terminal monitoring Producer RSS
│   ├── consumer_to_hdfs.png        ← Terminal monitoring Consumer HDFS
│   ├── spark_runner.png            ← Terminal monitoring Spark Runner
│   ├── app_dashboard_py.png        ← Terminal monitoring Flask Dashboard
│   ├── docker_desktop.png          ← Docker Desktop semua container aktif
│   ├── Dashboard.png
│   ├── Peta_2D_Gelap_View.png
│   ├── Peta_2D_Street_View.png
│   ├── Peta_3D.png
│   ├── Berita.png
│   ├── overview_hdfs.png
│   ├── browse_directory_hdfs.png
│   └── browse_directory_hdfs_data.png
├── kafka/
│   ├── producer_api.py        ← Aras Rizky Ananta
│   ├── producer_rss.py        ← Ica Zika Hamizah
│   ├── consumer_to_hdfs.py    ← Ica Zika Hamizah
│   ├── spark_processing.py    ← Arya Bisma Putra Refman
│   └── spark_runner.py        ← Otomatisasi Spark (setiap 10 menit)
├── lakehouse/                     ← Data Lakehouse Pipeline (Bronze → Silver → Gold)
│   ├── 00_setup.md            ← Panduan setup & cara menjalankan
│   ├── 01_bronze.py           ← Anggota 1: Ingestion ke Bronze Delta
│   ├── 02_silver.py           ← Anggota 2: Cleaning + Time Travel
│   ├── 03_gold_ets.py         ← Anggota 3: Reproduksi Analisis ETS ke Gold Delta
│   ├── README_lakehouse.md    ← Dokumentasi teknis Silver & Gold Layer
│   └── lakehouse_data/        ← Output Delta tables (bronze/, silver/, gold/)
└── dashboard/
    ├── app.py                 ← M. Hikari Reiziq Rakhmadinta
    ├── templates/
    │   └── index.html         ← M. Hikari Reiziq Rakhmadinta
    ├── static/
    │   ├── css/style.css
    │   └── js/app.js
    └── data/
        ├── spark_results.json
        ├── live_api.json
        └── live_rss.json
```
