"""
Script: 02_silver.py
Deskripsi: Membersihkan data Bronze Layer (API & RSS gempa) menjadi Silver Layer
           yang siap dianalisis. Melakukan minimal 5 transformasi cleaning relevan
           domain GempaRadar, mencatat statistik baris yang hilang, serta
           mendemonstrasikan fitur Time Travel Delta Lake.
Tugas: Anggota 2 (Silver + Time Travel)
Project: GempaRadar Data Lakehouse | Kelompok 1 ETS Big Data

Transformasi yang dilakukan (Silver API):
  1. dropDuplicates(["id"])          — Hapus data gempa duplikat berdasarkan ID unik USGS
  2. filter(magnitude >= 0)          — Filter magnitude tidak valid / negatif
  3. filter(depth_km > 0)            — Filter kedalaman tidak valid (<= 0 km)
  4. to_timestamp("event_time")      — Cast event_time dari string ISO ke tipe TimestampType
  5. withColumn("jam_kejadian", ...) — Ekstrak jam kejadian dari event_time
  6. filter(place.isNotNull())       — Hapus baris tanpa informasi lokasi

Transformasi yang dilakukan (Silver RSS):
  1. dropDuplicates(["id"])               — Hapus berita duplikat berdasarkan ID hash
  2. filter(title.isNotNull)              — Filter berita tanpa judul
  3. to_timestamp("published_time")       — Cast published_time ke TimestampType
  4. withColumn("tanggal_terbit", ...)    — Ekstrak tanggal dari published_time
  5. regexp_replace("summary", HTML_TAG)  — Bersihkan HTML tag dari kolom summary
"""

import os
import sys

# Pindahkan CWD ke folder lakehouse/ agar path relatif bersifat portabel
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# ====================================================================
# AUTO-REDIRECT KE DOCKER CONTAINER (sama dengan 01_bronze.py)
# ====================================================================
if os.name == 'nt' and not os.environ.get('HADOOP_HOME'):
    import subprocess
    print("\n" + "="*60)
    print("  CLEANING SILVER LAYER - GEMPARADAR DATA LAKEHOUSE")
    print("="*60)
    print("[*] Mendeteksi Windows Host tanpa HADOOP_HOME.")
    print("[*] Mengalihkan eksekusi ke dalam container Docker 'spark-master'...")

    container_script = "/app/lakehouse/02_silver.py"
    cmd = [
        "docker", "exec", "-u", "root", "spark-master",
        "/opt/spark/bin/spark-submit",
        "--packages", "io.delta:delta-spark_2.12:3.1.0",
        "--conf", "spark.jars.ivy=/tmp/.ivy2",
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        container_script
    ]
    try:
        result = subprocess.run(cmd)
        sys.exit(result.returncode)
    except Exception as e:
        print(f"[-] Gagal mengalihkan ke container Docker: {e}")
        print("[!] Pastikan Docker Desktop aktif dan container 'spark-master' berjalan.")
        sys.exit(1)

print("\n" + "="*60)
print("  CLEANING SILVER LAYER - GEMPARADAR DATA LAKEHOUSE")
print("="*60)

# ====================================================================
# IMPORT LIBRARY
# ====================================================================
try:
    # pyrefly: ignore [missing-import]
    from pyspark.sql import SparkSession
    # pyrefly: ignore [missing-import]
    from pyspark.sql.functions import (
        col, to_timestamp, hour, to_date,
        regexp_replace, trim, lit, current_timestamp
    )
    # pyrefly: ignore [missing-import]
    from pyspark.sql.types import DoubleType
    try:
        # pyrefly: ignore [import-missing, missing-import]
        from delta import configure_spark_with_delta_pip
        HAS_DELTA_PACKAGE = True
    except Exception:
        HAS_DELTA_PACKAGE = False
except ImportError as e:
    print(f"[-] Gagal mengimpor library: {e}")
    sys.exit(1)

# ====================================================================
# INISIALISASI SPARKSESSION
# ====================================================================
print("[*] Menginisialisasi SparkSession dengan ekstensi Delta Lake...")
os.environ["HADOOP_USER_NAME"] = "hadoop"

try:
    builder = SparkSession.builder \
        .appName("Silver-GempaRadar") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:8020")

    if HAS_DELTA_PACKAGE:
        try:
            spark = configure_spark_with_delta_pip(
                builder,
                extra_packages=["io.delta:delta-spark_2.12:3.1.0"]
            ).getOrCreate()
        except Exception as inner_e:
            print(f"[*] Helper gagal ({inner_e}), fallback ke builder standar...")
            spark = builder.getOrCreate()
    else:
        spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print("[+] SparkSession berhasil dibuat!")
except Exception as e:
    print(f"[-] Gagal menginisialisasi SparkSession: {e}")
    sys.exit(1)

# ====================================================================
# DEFINISI PATH
# ====================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LAKEHOUSE_DATA_DIR = os.path.join(BASE_DIR, "lakehouse_data")

def local_path(relative):
    """Konversi path relatif lakehouse_data/ ke URI file:// Spark."""
    abs_path = os.path.join(LAKEHOUSE_DATA_DIR, relative)
    return f"file://{abs_path.replace(os.sep, '/')}"

BRONZE_API_PATH  = local_path("bronze/gempa_api")
BRONZE_RSS_PATH  = local_path("bronze/gempa_rss")
SILVER_API_PATH  = local_path("silver/gempa_api")
SILVER_RSS_PATH  = local_path("silver/gempa_rss")

print(f"\n[*] Sumber Bronze API  : {BRONZE_API_PATH}")
print(f"[*] Sumber Bronze RSS  : {BRONZE_RSS_PATH}")
print(f"[*] Output Silver API  : {SILVER_API_PATH}")
print(f"[*] Output Silver RSS  : {SILVER_RSS_PATH}")

# ====================================================================
# HELPER FUNCTION — CETAK STATISTIK BARIS
# ====================================================================
def print_stats(label, before, after):
    """Cetak ringkasan jumlah baris sebelum & sesudah satu tahap cleaning."""
    hilang   = before - after
    persen   = (hilang / before * 100) if before > 0 else 0.0
    print(f"    [{label}] {before:>5} baris → {after:>5} baris "
          f"| hilang {hilang:>4} baris ({persen:.1f}%)")

# ====================================================================
# BAGIAN 1: SILVER LAYER — DATA GEMPA API (USGS)
# ====================================================================
print("\n" + "="*50)
print("  BAGIAN 1: SILVER API (USGS Earthquake Data)")
print("="*50)

try:
    # --- Baca Bronze API ---
    print("[*] Membaca Bronze API Delta table...")
    bronze_api = spark.read.format("delta").load(BRONZE_API_PATH)
    total_raw_api = bronze_api.count()
    print(f"[+] Total baris Bronze API: {total_raw_api}")

    print("\n--- SKEMA BRONZE API ---")
    bronze_api.printSchema()

    # ----------------------------------------------------------------
    # STATISTIK per tahap — dicatat untuk README
    # ----------------------------------------------------------------
    stats_api = {"raw": total_raw_api}

    # Transformasi 1: Hapus Duplikat berdasarkan ID gempa (ID USGS bersifat unik)
    # ALASAN: Data bisa diingest lebih dari sekali ke Bronze (mode=append).
    #         Duplikat ID menyebabkan analisis count, magnitude rata-rata, dll. bias.
    step1 = bronze_api.dropDuplicates(["id"])
    stats_api["after_dedup"] = step1.count()
    print_stats("1-DeduplicateID", stats_api["raw"], stats_api["after_dedup"])

    # Transformasi 2: Filter magnitude tidak valid (magnitude < 0)
    # ALASAN: Magnitude negatif tidak memiliki arti fisik dalam skala Richter/Moment.
    #         Nilainya bisa muncul akibat kesalahan sensor atau data placeholder.
    step2 = step1.filter(col("magnitude") >= 0)
    stats_api["after_mag_filter"] = step2.count()
    print_stats("2-FilterMagnitude<0", stats_api["after_dedup"], stats_api["after_mag_filter"])

    # Transformasi 3: Filter kedalaman tidak valid (depth_km <= 0)
    # ALASAN: Kedalaman gempa harus bernilai positif (di bawah permukaan bumi).
    #         Nilai 0 atau negatif menandakan data tidak lengkap/error.
    step3 = step2.filter(col("depth_km") > 0)
    stats_api["after_depth_filter"] = step3.count()
    print_stats("3-FilterDepth<=0", stats_api["after_mag_filter"], stats_api["after_depth_filter"])

    # Transformasi 4: Filter baris tanpa lokasi (place IS NOT NULL)
    # ALASAN: Kolom 'place' adalah keterangan wilayah terdampak — data tanpa lokasi
    #         tidak berguna untuk analisis persebaran gempa per wilayah.
    step4 = step3.filter(col("place").isNotNull())
    stats_api["after_null_place"] = step4.count()
    print_stats("4-FilterNullPlace", stats_api["after_depth_filter"], stats_api["after_null_place"])

    # Transformasi 5: Cast event_time dari String ISO → TimestampType
    # ALASAN: Bronze menyimpan timestamp sebagai string.
    #         Tanpa cast, operasi time-series (groupBy jam/hari, window function) tidak bisa jalan.
    step5 = step4 \
        .withColumn("event_time",  to_timestamp(col("event_time"))) \
        .withColumn("_ingested_at", to_timestamp(col("_ingested_at")))
    stats_api["after_cast"] = step5.count()
    print_stats("5-CastTimestamp", stats_api["after_null_place"], stats_api["after_cast"])

    # Transformasi 6: Ekstrak kolom turunan — jam kejadian & tanggal kejadian
    # ALASAN: Kolom turunan jam & tanggal memudahkan analisis pola temporal gempa
    #         (misalnya: jam berapa paling sering terjadi gempa? trend per hari?).
    silver_api = step5 \
        .withColumn("jam_kejadian",     hour(col("event_time"))) \
        .withColumn("tanggal_kejadian", to_date(col("event_time"))) \
        .withColumn("_silver_processed_at", current_timestamp()) \
        .withColumn("_layer", lit("silver"))

    total_silver_api = silver_api.count()
    stats_api["final_silver"] = total_silver_api

    # Ringkasan statistik keseluruhan
    total_hilang_api = stats_api["raw"] - stats_api["final_silver"]
    persen_hilang_api = (total_hilang_api / stats_api["raw"] * 100) if stats_api["raw"] > 0 else 0.0

    print("\n[+] --- RINGKASAN CLEANING API ---")
    print(f"    Baris Bronze (raw) : {stats_api['raw']}")
    print(f"    Baris Silver bersih: {stats_api['final_silver']}")
    print(f"    Total baris hilang : {total_hilang_api} ({persen_hilang_api:.1f}%)")
    print(f"    Interpretasi: {persen_hilang_api:.1f}% data dibuang karena duplikat,")
    print(f"                  nilai magnitude/kedalaman tidak valid, atau lokasi kosong.")

    print("\n--- SKEMA SILVER API ---")
    silver_api.printSchema()
    print("\n[*] Contoh 3 baris Silver API:")
    silver_api.select(
        "id", "magnitude", "depth_km", "place",
        "event_time", "jam_kejadian", "tanggal_kejadian", "mag_category"
    ).show(3, truncate=False)

    # --- Simpan ke Silver Delta (Version 0) ---
    print(f"\n[*] Menulis Silver API ke Delta table (Version 0)...")
    silver_api.write.format("delta").mode("overwrite").save(SILVER_API_PATH)
    print(f"[+] Silver API tersimpan: {SILVER_API_PATH}")

except Exception as e:
    print(f"[-] Gagal memproses Silver API: {e}")
    import traceback
    traceback.print_exc()


# ====================================================================
# BAGIAN 2: SILVER LAYER — DATA BERITA RSS
# ====================================================================
print("\n" + "="*50)
print("  BAGIAN 2: SILVER RSS (Google News Earthquake Feed)")
print("="*50)

try:
    # --- Baca Bronze RSS ---
    print("[*] Membaca Bronze RSS Delta table...")
    bronze_rss = spark.read.format("delta").load(BRONZE_RSS_PATH)
    total_raw_rss = bronze_rss.count()
    print(f"[+] Total baris Bronze RSS: {total_raw_rss}")

    stats_rss = {"raw": total_raw_rss}

    # Transformasi 1: Hapus Duplikat berdasarkan ID berita (hash artikel)
    # ALASAN: RSS feed bisa menghasilkan artikel sama dari beberapa ingestion.
    #         Duplikat artikel menyebabkan bias pada frekuensi pemberitaan gempa.
    rss_step1 = bronze_rss.dropDuplicates(["id"])
    stats_rss["after_dedup"] = rss_step1.count()
    print_stats("1-DeduplicateID", stats_rss["raw"], stats_rss["after_dedup"])

    # Transformasi 2: Filter berita tanpa judul (title IS NOT NULL dan tidak kosong)
    # ALASAN: Artikel tanpa judul tidak memiliki nilai informasi dan
    #         tidak dapat dianalisis untuk ekstraksi kata kunci gempa.
    rss_step2 = rss_step1.filter(
        col("title").isNotNull() & (trim(col("title")) != "")
    )
    stats_rss["after_title_filter"] = rss_step2.count()
    print_stats("2-FilterNullTitle", stats_rss["after_dedup"], stats_rss["after_title_filter"])

    # Transformasi 3: Cast published_time String ISO → TimestampType
    # ALASAN: Sama seperti API, timestamp harus bertipe Timestamp agar bisa
    #         digunakan dalam analisis tren waktu publikasi berita.
    rss_step3 = rss_step2 \
        .withColumn("published_time", to_timestamp(col("published_time"))) \
        .withColumn("timestamp",      to_timestamp(col("timestamp")))
    stats_rss["after_cast"] = rss_step3.count()
    print_stats("3-CastTimestamp", stats_rss["after_title_filter"], stats_rss["after_cast"])

    # Transformasi 4: Bersihkan HTML tag dari kolom summary
    # ALASAN: Kolom summary mengandung tag HTML (<a href...>, <font color...>).
    #         HTML tag mengotori teks dan menghambat NLP / analisis sentimen berita gempa.
    HTML_PATTERN = r"<[^>]+>"
    rss_step4 = rss_step3 \
        .withColumn("summary_clean", regexp_replace(col("summary"), HTML_PATTERN, "")) \
        .withColumn("summary_clean", trim(col("summary_clean")))
    stats_rss["after_clean_html"] = rss_step4.count()
    print_stats("4-CleanHTMLSummary", stats_rss["after_cast"], stats_rss["after_clean_html"])

    # Transformasi 5: Ekstrak tanggal terbit
    # ALASAN: Kolom tanggal terbit memudahkan agregasi berita per hari/minggu
    #         untuk analisis volume pemberitaan gempa seiring waktu.
    silver_rss = rss_step4 \
        .withColumn("tanggal_terbit", to_date(col("published_time"))) \
        .withColumn("_silver_processed_at", current_timestamp()) \
        .withColumn("_layer", lit("silver"))

    total_silver_rss = silver_rss.count()
    stats_rss["final_silver"] = total_silver_rss

    total_hilang_rss = stats_rss["raw"] - stats_rss["final_silver"]
    persen_hilang_rss = (total_hilang_rss / stats_rss["raw"] * 100) if stats_rss["raw"] > 0 else 0.0

    print("\n[+] --- RINGKASAN CLEANING RSS ---")
    print(f"    Baris Bronze (raw) : {stats_rss['raw']}")
    print(f"    Baris Silver bersih: {stats_rss['final_silver']}")
    print(f"    Total baris hilang : {total_hilang_rss} ({persen_hilang_rss:.1f}%)")
    print(f"    Interpretasi: {persen_hilang_rss:.1f}% data dibuang karena duplikat,")
    print(f"                  judul kosong, atau masalah format timestamp.")

    print("\n--- SKEMA SILVER RSS ---")
    silver_rss.printSchema()
    print("\n[*] Contoh 3 baris Silver RSS:")
    silver_rss.select(
        "id", "title", "source", "published_time", "tanggal_terbit", "summary_clean"
    ).show(3, truncate=False)

    # --- Simpan ke Silver Delta (Version 0) ---
    print(f"\n[*] Menulis Silver RSS ke Delta table (Version 0)...")
    silver_rss.write.format("delta").mode("overwrite").save(SILVER_RSS_PATH)
    print(f"[+] Silver RSS tersimpan: {SILVER_RSS_PATH}")

except Exception as e:
    print(f"[-] Gagal memproses Silver RSS: {e}")
    import traceback
    traceback.print_exc()


# ====================================================================
# BAGIAN 3: DEMONSTRASI TIME TRAVEL DELTA LAKE (WAJIB)
# ====================================================================
print("\n" + "="*60)
print("  BAGIAN 3: DEMONSTRASI TIME TRAVEL — DELTA LAKE")
print("="*60)
print("""
[INFO] Time Travel adalah fitur Delta Lake yang mencatat setiap perubahan
       data sebagai 'version' baru dalam _delta_log/. Kita dapat membaca
       data dari versi mana pun tanpa menyimpan salinan fisik terpisah.
""")

try:
    # pyrefly: ignore [missing-import]
    from delta.tables import DeltaTable

    # ------------------------------------------------------------------
    # Langkah A: Buat UPDATE untuk membuat Version 1
    # Kita akan mengubah nilai mag_category pada beberapa baris
    # sebagai simulasi "koreksi label" yang terjadi setelah ingestion.
    # ------------------------------------------------------------------
    print("[*] Langkah A: Membuat Version 1 (UPDATE mag_category pada magnitude >= 5.0)")
    print("    Simulasi: tim analis mengoreksi label 'kuat' → 'sangat_kuat' untuk M≥5.0")

    delta_api = DeltaTable.forPath(spark, SILVER_API_PATH)

    # Update: gempa dengan magnitude >= 5.0 dikategorikan ulang sebagai 'sangat_kuat'
    delta_api.update(
        condition=col("magnitude") >= 5.0,
        set={"mag_category": lit("sangat_kuat")}
    )

    # Verifikasi update berhasil
    ver1_count = spark.read.format("delta") \
        .option("versionAsOf", 1) \
        .load(SILVER_API_PATH) \
        .filter(col("mag_category") == "sangat_kuat") \
        .count()
    print(f"[+] Version 1 dibuat. Baris dengan mag_category='sangat_kuat': {ver1_count}")

    # ------------------------------------------------------------------
    # Langkah B: Baca data Version 0 (sebelum update) vs Version 1 (sesudah)
    # ------------------------------------------------------------------
    print("\n[*] Langkah B: Membandingkan Version 0 vs Version 1")

    df_v0 = spark.read.format("delta") \
        .option("versionAsOf", 0) \
        .load(SILVER_API_PATH)

    df_v1 = spark.read.format("delta") \
        .option("versionAsOf", 1) \
        .load(SILVER_API_PATH)

    print("\n  [VERSION 0] — Data sebelum update (distribusi mag_category):")
    df_v0.groupBy("mag_category").count().orderBy("mag_category").show()

    print("  [VERSION 1] — Data sesudah update (distribusi mag_category):")
    df_v1.groupBy("mag_category").count().orderBy("mag_category").show()

    # ------------------------------------------------------------------
    # Langkah C: Baca berdasarkan Timestamp
    # ------------------------------------------------------------------
    print("[*] Langkah C: Demo membaca data 'versionAsOf' dan 'timestampAsOf'")

    # Tampilkan history lengkap tabel Delta
    print("\n  [DELTA LOG] Riwayat operasi pada Silver API table:")
    delta_api.history().select(
        "version", "timestamp", "operation", "operationParameters"
    ).show(truncate=False)

    # Contoh baca version 0 via timestamp (gunakan timestamp dari history)
    history_df = delta_api.history().orderBy("version")
    v0_ts_row = history_df.filter(col("version") == 0).select("timestamp").first()
    if v0_ts_row:
        import datetime
        # Tambahkan 1 detik untuk menghindari pemotongan milidetik strftime yang membuat timestamp sebelum pembuatan awal
        v0_ts_adjusted = v0_ts_row["timestamp"] + datetime.timedelta(seconds=1)
        v0_timestamp_str = v0_ts_adjusted.strftime("%Y-%m-%d %H:%M:%S")
        print(f"  [*] Membaca ulang Version 0 via timestampAsOf='{v0_timestamp_str}'...")
        df_v0_ts = spark.read.format("delta") \
            .option("timestampAsOf", v0_timestamp_str) \
            .load(SILVER_API_PATH)
        print(f"  [+] Jumlah baris saat dibaca via timestamp: {df_v0_ts.count()}")

    # ------------------------------------------------------------------
    # Langkah D: RESTORE ke Version 0 (opsional demo)
    # ------------------------------------------------------------------
    print("\n[*] Langkah D: Simulasi RESTORE ke Version 0")
    print("    (Mengembalikan data ke kondisi sebelum update mag_category)")
    delta_api.restoreToVersion(0)
    restored_count = spark.read.format("delta").load(SILVER_API_PATH) \
        .filter(col("mag_category") == "sangat_kuat").count()
    print(f"[+] Setelah RESTORE: baris 'sangat_kuat' = {restored_count} (kembali ke 0)")
    print("    Delta sekarang berada di Version 2 (RESTORE mencatat versi baru)")

    # Tampilkan history final
    print("\n  [DELTA LOG] Riwayat final setelah RESTORE:")
    delta_api.history().select("version", "timestamp", "operation").show(truncate=False)

    print("\n[+] Demonstrasi Time Travel selesai!")
    print("    Kesimpulan:")
    print("    - Version 0: Data Silver awal (overwrite pertama dari cleaning)")
    print("    - Version 1: Setelah UPDATE — mag_category M≥5.0 diubah ke 'sangat_kuat'")
    print("    - Version 2: Setelah RESTORE ke Version 0 — data kembali seperti semula")

except Exception as e:
    print(f"[-] Gagal menjalankan Time Travel demo: {e}")
    import traceback
    traceback.print_exc()
    print("[!] Catatan: Time Travel memerlukan delta-spark terinstall dengan benar di JVM.")


# ====================================================================
# VERIFIKASI AKHIR
# ====================================================================
print("\n" + "="*50)
print("  VERIFIKASI SILVER LAYER")
print("="*50)

try:
    print("[*] Membaca ulang Silver API...")
    final_api = spark.read.format("delta").load(SILVER_API_PATH)
    print(f"[+] Total baris Silver API: {final_api.count()}")
    final_api.select(
        "id", "magnitude", "depth_km", "mag_category", "depth_category",
        "event_time", "jam_kejadian", "tanggal_kejadian", "place"
    ).show(5, truncate=False)
except Exception as e:
    print(f"[-] Verifikasi Silver API gagal: {e}")

try:
    print("\n[*] Membaca ulang Silver RSS...")
    final_rss = spark.read.format("delta").load(SILVER_RSS_PATH)
    print(f"[+] Total baris Silver RSS: {final_rss.count()}")
    final_rss.select(
        "id", "title", "published_time", "tanggal_terbit", "summary_clean"
    ).show(5, truncate=False)
except Exception as e:
    print(f"[-] Verifikasi Silver RSS gagal: {e}")


# ====================================================================
# TUTUP SPARK SESSION
# ====================================================================
print("\n[*] Menutup SparkSession...")
spark.stop()

print("\n" + "="*60)
print("  Silver Layer selesai. Anggota 3 & 4 bisa mulai Gold.")
print("="*60)
print("\n  📊 RINGKASAN JUSTIFIKASI TRANSFORMASI (untuk README):")
print("  ┌─────────────────────────────────────────────────────────────────┐")
print("  │ API Layer                                                       │")
print("  │  1. dropDuplicates([id])   → Cegah double-count analisis       │")
print("  │  2. filter(magnitude>=0)   → Nilai fisik tidak valid dibuang   │")
print("  │  3. filter(depth_km>0)     → Kedalaman <=0 tidak masuk akal    │")
print("  │  4. filter(place notNull)  → Lokasi wajib ada untuk analisis   │")
print("  │  5. to_timestamp(event_t.) → Aktifkan operasi time-series      │")
print("  │  6. hour(event_time)       → Pola temporal jam kejadian gempa  │")
print("  ├─────────────────────────────────────────────────────────────────┤")
print("  │ RSS Layer                                                       │")
print("  │  1. dropDuplicates([id])   → Cegah bias frekuensi berita       │")
print("  │  2. filter(title notNull)  → Artikel tanpa judul = tidak valid │")
print("  │  3. to_timestamp(pub_time) → Aktifkan analisis tren publikasi  │")
print("  │  4. regexp_replace(HTML)   → Bersihkan HTML utk NLP/analisis   │")
print("  │  5. to_date(pub_time)      → Agregasi per hari publikasi       │")
print("  └─────────────────────────────────────────────────────────────────┘")
