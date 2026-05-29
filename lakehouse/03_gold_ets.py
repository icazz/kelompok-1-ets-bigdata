"""
Script: 03_gold_ets.py
Deskripsi: Gold Layer — Reproduksi Analisis ETS dari spark_processing.py
           Membaca data bersih dari Silver Delta Layer (bukan HDFS JSON mentah),
           menjalankan 3 analisis wajib + Spark MLlib Linear Regression,
           lalu menyimpan setiap hasil analisis ke format Delta Lake (Gold Layer).

Tugas: Anggota 3 (Gold Reproduksi ETS — paling mudah)
Project: GempaRadar Data Lakehouse | Kelompok 1 ETS Big Data

Analisis yang direproduksi dari kafka/spark_processing.py:
  1. Distribusi Magnitudo   — DataFrame API  (when/otherwise → kategori_mag)
  2. Top 10 Wilayah Aktif   — Spark SQL      (REGEXP_REPLACE + GROUP BY)
  3. Distribusi Kedalaman   — Spark SQL      (SUM CASE WHEN + statistik AVG/MAX/MIN)
  Bonus: Spark MLlib Linear Regression — Prediksi Tren Magnitudo

Perbedaan utama vs ETS asli:
  - INPUT:  Silver Delta (lakehouse_data/silver/gempa_api)  ← bukan HDFS JSON mentah
  - OUTPUT: Gold Delta   (lakehouse_data/gold/...)           ← bukan HDFS JSON hasil
  - Data sudah bersih (deduplicated, no negative magnitude, valid depth, cast timestamp)
"""

import os
import sys
import json
import shutil
from datetime import datetime, timezone

# Pindahkan CWD ke folder lakehouse/ agar path relatif bersifat portabel
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# ====================================================================
# AUTO-REDIRECT KE DOCKER CONTAINER (pola sama dengan 01_bronze.py)
# ====================================================================
if os.name == 'nt' and not os.environ.get('HADOOP_HOME'):
    import subprocess
    print("\n" + "="*60)
    print("  GOLD LAYER (ETS) — GEMPARADAR DATA LAKEHOUSE")
    print("="*60)
    print("[*] Mendeteksi Windows Host tanpa HADOOP_HOME.")
    print("[*] Mengalihkan eksekusi ke dalam container Docker 'spark-master'...")

    container_script = "/app/lakehouse/03_gold_ets.py"
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
print("  GOLD LAYER (ETS) — GEMPARADAR DATA LAKEHOUSE")
print("  Reproduksi 3 Analisis Wajib + MLlib dari spark_processing.py")
print("="*60)

# ====================================================================
# IMPORT LIBRARY
# ====================================================================
try:
    # pyrefly: ignore [missing-import]
    from pyspark.sql import SparkSession
    # pyrefly: ignore [missing-import]
    from pyspark.sql.functions import (
        col, when, count, avg, round as spark_round,
        max as spark_max, min as spark_min, stddev,
        regexp_replace, lit, current_timestamp
    )
    # pyrefly: ignore [missing-import]
    from pyspark.ml.feature import VectorAssembler
    # pyrefly: ignore [missing-import]
    from pyspark.ml.regression import LinearRegression as SparkLR

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
        .appName("Gold-ETS-GempaRadar") \
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

# INPUT: Silver Layer (data sudah bersih)
SILVER_API_PATH = local_path("silver/gempa_api")

# OUTPUT: Gold Layer (hasil analisis ETS)
GOLD_DISTRIBUSI_MAG_PATH  = local_path("gold/ets_distribusi_magnitudo")
GOLD_TOP_WILAYAH_PATH     = local_path("gold/ets_top_wilayah")
GOLD_DISTRIBUSI_DEPTH_PATH = local_path("gold/ets_distribusi_kedalaman")
GOLD_STATISTIK_PATH       = local_path("gold/ets_statistik_ringkasan")
GOLD_MLLIB_PATH           = local_path("gold/ets_mllib_tren")

print(f"\n[*] Sumber Silver API : {SILVER_API_PATH}")
print(f"[*] Output Gold ETS  : {local_path('gold/')}")

# ====================================================================
# HELPER: Bersihkan folder Gold lama agar versi Delta bersih
# ====================================================================
def clean_gold_path(delta_uri):
    """Hapus folder Delta lama sebelum menulis ulang (reset version log)."""
    clean = delta_uri.replace("file://", "")
    if os.path.exists(clean):
        try:
            shutil.rmtree(clean)
        except Exception:
            pass

# ====================================================================
# LANGKAH 1: BACA DATA SILVER API (Delta)
# ====================================================================
print("\n" + "="*50)
print("  LANGKAH 1: MEMBACA DATA SILVER API (DELTA)")
print("="*50)

try:
    print("[*] Membaca Silver API Delta table...")
    df = spark.read.format("delta").load(SILVER_API_PATH)
    total = df.count()
    print(f"[+] Total record Silver API: {total}")

    if total == 0:
        print("[-] Data Silver kosong. Jalankan 01_bronze.py dan 02_silver.py terlebih dahulu.")
        spark.stop()
        sys.exit(0)

    print("\n--- SKEMA SILVER API ---")
    df.printSchema()

    # Buat temp view untuk Spark SQL (sama seperti spark_processing.py)
    df.createOrReplaceTempView("gempa")
    print("[+] Temp view 'gempa' berhasil dibuat untuk Spark SQL.")

except Exception as e:
    print(f"[-] Gagal membaca Silver API: {e}")
    import traceback
    traceback.print_exc()
    print("[!] Pastikan 01_bronze.py dan 02_silver.py sudah dijalankan.")
    spark.stop()
    sys.exit(1)


# ====================================================================
# LANGKAH 2: ANALISIS WAJIB 1 — Distribusi Magnitudo (DataFrame API)
# ====================================================================
print("\n" + "="*50)
print("  ANALISIS 1: Distribusi Magnitudo (DataFrame API)")
print("="*50)

try:
    # Reproduksi exact logic dari spark_processing.py baris 45-50
    df_mag = df.withColumn("kategori_mag",
        when(col("magnitude") < 3, "Mikro (<3)")
        .when((col("magnitude") >= 3) & (col("magnitude") < 4), "Minor (3-4)")
        .when((col("magnitude") >= 4) & (col("magnitude") < 5), "Sedang (4-5)")
        .otherwise("Kuat (>5)")
    ).groupBy("kategori_mag").count().orderBy("count", ascending=False)

    print("[+] Hasil Distribusi Magnitudo:")
    df_mag.show()

    print("  [Interpretasi] Gempa kategori Sedang (4-5) mendominasi karena Indonesia berada")
    print("  di Ring of Fire — zona pertemuan lempeng Indo-Australia, Eurasia, dan Pasifik.")
    print("  Frekuensi tinggi gempa menengah mengindikasikan tekanan lempeng yang terus aktif,")
    print("  namun jarang menyebabkan kerusakan besar dibanding gempa kuat (M>5).\n")

    # Kumpulkan data untuk JSON dashboard
    mag_dist_raw = {row["kategori_mag"]: row["count"] for row in df_mag.collect()}
    mag_ordered = {
        "Mikro (<3)":   mag_dist_raw.get("Mikro (<3)", 0),
        "Minor (3-4)":  mag_dist_raw.get("Minor (3-4)", 0),
        "Sedang (4-5)": mag_dist_raw.get("Sedang (4-5)", 0),
        "Kuat (>5)":    mag_dist_raw.get("Kuat (>5)", 0),
    }

    # Simpan ke Gold Delta
    print("[*] Menyimpan Distribusi Magnitudo ke Gold Delta...")
    clean_gold_path(GOLD_DISTRIBUSI_MAG_PATH)
    gold_mag = df_mag \
        .withColumn("_gold_processed_at", current_timestamp()) \
        .withColumn("_layer", lit("gold")) \
        .withColumn("_analysis", lit("distribusi_magnitudo"))
    gold_mag.write.format("delta").mode("overwrite").save(GOLD_DISTRIBUSI_MAG_PATH)
    print(f"[+] Gold Distribusi Magnitudo tersimpan: {GOLD_DISTRIBUSI_MAG_PATH}")

except Exception as e:
    print(f"[-] Gagal menganalisis distribusi magnitudo: {e}")
    import traceback
    traceback.print_exc()
    mag_ordered = {}


# ====================================================================
# LANGKAH 3: ANALISIS WAJIB 2 — Top 10 Wilayah Aktif (Spark SQL)
# ====================================================================
print("\n" + "="*50)
print("  ANALISIS 2: Top 10 Wilayah Paling Aktif (Spark SQL)")
print("="*50)

try:
    # Reproduksi exact logic dari spark_processing.py baris 67-77
    df_wilayah = spark.sql("""
        SELECT
            REGEXP_REPLACE(place, '.* of ', '') AS wilayah,
            COUNT(*) AS jumlah_gempa,
            ROUND(AVG(magnitude), 2) AS rata_mag,
            ROUND(AVG(depth_km), 1) AS rata_depth_km
        FROM gempa
        GROUP BY wilayah
        ORDER BY jumlah_gempa DESC
        LIMIT 10
    """)

    print("[+] Hasil Top 10 Wilayah Paling Aktif:")
    df_wilayah.show(truncate=False)

    print("  [Interpretasi] Wilayah sekitar Laut Banda, Sulawesi Utara, dan Timor Leste")
    print("  konsisten menjadi zona paling aktif karena pertemuan tiga lempeng tektonik besar.")
    print("  Data ini membantu BPBD memprioritaskan penempatan sensor dan tim respons cepat")
    print("  di wilayah dengan frekuensi gempa tertinggi.\n")

    # Kumpulkan data untuk JSON dashboard
    top_wilayah = [
        {"wilayah": row["wilayah"], "count": row["jumlah_gempa"]}
        for row in df_wilayah.collect()
    ]

    # Simpan ke Gold Delta — juga simpan semua wilayah (bukan hanya top 10)
    print("[*] Menyimpan Top Wilayah ke Gold Delta...")
    clean_gold_path(GOLD_TOP_WILAYAH_PATH)

    # Simpan full aggregation untuk referensi, bukan hanya LIMIT 10
    df_wilayah_full = spark.sql("""
        SELECT
            REGEXP_REPLACE(place, '.* of ', '') AS wilayah,
            COUNT(*) AS jumlah_gempa,
            ROUND(AVG(magnitude), 2) AS rata_mag,
            ROUND(AVG(depth_km), 1) AS rata_depth_km
        FROM gempa
        GROUP BY wilayah
        ORDER BY jumlah_gempa DESC
    """)
    gold_wilayah = df_wilayah_full \
        .withColumn("_gold_processed_at", current_timestamp()) \
        .withColumn("_layer", lit("gold")) \
        .withColumn("_analysis", lit("top_wilayah"))
    gold_wilayah.write.format("delta").mode("overwrite").save(GOLD_TOP_WILAYAH_PATH)
    print(f"[+] Gold Top Wilayah tersimpan: {GOLD_TOP_WILAYAH_PATH}")

except Exception as e:
    print(f"[-] Gagal menganalisis wilayah aktif: {e}")
    import traceback
    traceback.print_exc()
    top_wilayah = []


# ====================================================================
# LANGKAH 4: ANALISIS WAJIB 3 — Distribusi & Statistik Kedalaman (Spark SQL)
# ====================================================================
print("\n" + "="*50)
print("  ANALISIS 3: Distribusi & Statistik Kedalaman (Spark SQL)")
print("="*50)

try:
    # Reproduksi exact logic dari spark_processing.py baris 91-99
    df_depth = spark.sql("""
        SELECT
            SUM(CASE WHEN depth_km < 70  THEN 1 ELSE 0 END) AS dangkal,
            SUM(CASE WHEN depth_km >= 70 AND depth_km < 300 THEN 1 ELSE 0 END) AS menengah,
            SUM(CASE WHEN depth_km >= 300 THEN 1 ELSE 0 END) AS dalam,
            ROUND(AVG(depth_km), 1) AS rata_rata_depth,
            ROUND(MAX(depth_km), 1) AS depth_max,
            ROUND(MIN(depth_km), 1) AS depth_min
        FROM gempa
    """)

    print("[+] Hasil Distribusi & Statistik Kedalaman:")
    df_depth.show()

    print("  [Interpretasi] Dominasi gempa dangkal (<70 km) menunjukkan subduksi aktif di")
    print("  sepanjang Palung Jawa dan Palung Timor. Gempa dangkal lebih berbahaya karena")
    print("  melepas energi lebih dekat ke permukaan — berpotensi memicu tsunami dan")
    print("  kerusakan infrastruktur yang lebih besar dibanding gempa dalam.\n")

    # Kumpulkan data untuk JSON dashboard
    d = df_depth.collect()[0]
    depth_stats = {
        "Dangkal (<70 km)":    int(d["dangkal"]),
        "Menengah (70-300 km)": int(d["menengah"]),
        "Dalam (>300 km)":     int(d["dalam"]),
    }
    avg_depth = float(d["rata_rata_depth"]) if d["rata_rata_depth"] else 0
    max_depth = float(d["depth_max"]) if d["depth_max"] else 0

    # Simpan ke Gold Delta
    print("[*] Menyimpan Distribusi Kedalaman ke Gold Delta...")
    clean_gold_path(GOLD_DISTRIBUSI_DEPTH_PATH)
    gold_depth = df_depth \
        .withColumn("_gold_processed_at", current_timestamp()) \
        .withColumn("_layer", lit("gold")) \
        .withColumn("_analysis", lit("distribusi_kedalaman"))
    gold_depth.write.format("delta").mode("overwrite").save(GOLD_DISTRIBUSI_DEPTH_PATH)
    print(f"[+] Gold Distribusi Kedalaman tersimpan: {GOLD_DISTRIBUSI_DEPTH_PATH}")

except Exception as e:
    print(f"[-] Gagal menganalisis distribusi kedalaman: {e}")
    import traceback
    traceback.print_exc()
    depth_stats = {}
    avg_depth = 0
    max_depth = 0


# ====================================================================
# LANGKAH 5: STATISTIK RINGKASAN
# ====================================================================
print("\n" + "="*50)
print("  STATISTIK RINGKASAN")
print("="*50)

try:
    stats_row = spark.sql("""
        SELECT
            COUNT(*) AS total,
            ROUND(AVG(magnitude), 2) AS avg_mag,
            ROUND(MAX(magnitude), 1) AS max_mag,
            ROUND(MIN(magnitude), 2) AS min_mag,
            ROUND(STDDEV(magnitude), 4) AS stddev_mag
        FROM gempa
    """).collect()[0]

    print(f"  Total gempa   : {stats_row['total']}")
    print(f"  Avg magnitude : {stats_row['avg_mag']}")
    print(f"  Max magnitude : {stats_row['max_mag']}")
    print(f"  Min magnitude : {stats_row['min_mag']}")
    print(f"  Stddev mag    : {stats_row['stddev_mag']}")

    # Simpan statistik ringkasan ke Gold Delta
    print("\n[*] Menyimpan Statistik Ringkasan ke Gold Delta...")
    clean_gold_path(GOLD_STATISTIK_PATH)
    df_stats = spark.sql("""
        SELECT
            COUNT(*) AS total_gempa,
            ROUND(AVG(magnitude), 2) AS avg_magnitude,
            ROUND(MAX(magnitude), 1) AS max_magnitude,
            ROUND(MIN(magnitude), 2) AS min_magnitude,
            ROUND(STDDEV(magnitude), 4) AS stddev_magnitude,
            ROUND(AVG(depth_km), 1) AS avg_depth_km,
            ROUND(MAX(depth_km), 1) AS max_depth_km,
            ROUND(MIN(depth_km), 1) AS min_depth_km
        FROM gempa
    """)
    gold_stats = df_stats \
        .withColumn("_gold_processed_at", current_timestamp()) \
        .withColumn("_layer", lit("gold")) \
        .withColumn("_analysis", lit("statistik_ringkasan"))
    gold_stats.write.format("delta").mode("overwrite").save(GOLD_STATISTIK_PATH)
    print(f"[+] Gold Statistik Ringkasan tersimpan: {GOLD_STATISTIK_PATH}")

except Exception as e:
    print(f"[-] Gagal menghitung statistik ringkasan: {e}")
    import traceback
    traceback.print_exc()
    stats_row = {"total": 0, "avg_mag": 0, "max_mag": 0}


# ====================================================================
# LANGKAH 6: SPARK MLLIB — Linear Regression Tren Magnitudo (Bonus)
# ====================================================================
print("\n" + "="*50)
print("  SPARK MLLIB — Prediksi Tren Magnitudo (Linear Regression)")
print("="*50)

mllib_results = {}
try:
    # Reproduksi exact logic dari spark_processing.py baris 131-163
    min_row = spark.sql("SELECT MIN(event_time_epoch) AS min_t FROM gempa").collect()[0]
    min_t = min_row["min_t"] or 0

    df_ml = spark.sql(f"""
        SELECT
            (event_time_epoch - {min_t}) / 3600000.0 AS jam_ke,
            magnitude
        FROM gempa
        WHERE event_time_epoch IS NOT NULL AND magnitude IS NOT NULL
    """).dropna()

    ml_count = df_ml.count()
    print(f"[*] Data untuk regresi: {ml_count} baris")

    if ml_count > 1:
        assembler = VectorAssembler(inputCols=["jam_ke"], outputCol="features")
        ml_data = assembler.transform(df_ml)
        lr_model = SparkLR(featuresCol="features", labelCol="magnitude", maxIter=10).fit(ml_data)

        koefisien = float(lr_model.coefficients[0])
        rmse = float(lr_model.summary.rootMeanSquaredError)
        r2 = float(lr_model.summary.r2)
        tren = "naik" if koefisien > 0 else "turun"

        print(f"\n  Koefisien : {koefisien:.6f} (per jam)")
        print(f"  RMSE      : {rmse:.4f}")
        print(f"  R²        : {r2:.4f}")
        print(f"\n  [Interpretasi] Tren magnitudo cenderung {tren} seiring waktu.")
        print("  Pola ini membantu BPBD mengantisipasi eskalasi aktivitas seismik dan")
        print("  merencanakan kesiapan sumber daya respons bencana secara lebih proaktif.\n")

        mllib_results = {
            "tren": tren,
            "koefisien_per_jam": round(koefisien, 6),
            "rmse": round(rmse, 4),
            "r2": round(r2, 4),
        }

        # Simpan hasil MLlib ke Gold Delta (sebagai tabel kecil 1 baris)
        print("[*] Menyimpan hasil MLlib ke Gold Delta...")
        clean_gold_path(GOLD_MLLIB_PATH)
        df_mllib = spark.createDataFrame([{
            "tren": tren,
            "koefisien_per_jam": round(koefisien, 6),
            "rmse": round(rmse, 4),
            "r2": round(r2, 4),
            "jumlah_data_regresi": ml_count,
        }])
        gold_mllib = df_mllib \
            .withColumn("_gold_processed_at", current_timestamp()) \
            .withColumn("_layer", lit("gold")) \
            .withColumn("_analysis", lit("mllib_tren_magnitudo"))
        gold_mllib.write.format("delta").mode("overwrite").save(GOLD_MLLIB_PATH)
        print(f"[+] Gold MLlib tersimpan: {GOLD_MLLIB_PATH}")
    else:
        print("  [MLlib] Data tidak cukup untuk regresi.\n")

except Exception as e:
    print(f"  [MLlib] Gagal menjalankan regresi: {e}")
    import traceback
    traceback.print_exc()


# ====================================================================
# LANGKAH 7: SIMPAN spark_results.json UNTUK DASHBOARD
# ====================================================================
print("\n" + "="*50)
print("  EXPORT: spark_results.json untuk Dashboard")
print("="*50)

try:
    spark_results = {
        "source":                "gold_delta_ets",
        "total_gempa":           int(stats_row["total"]),
        "avg_magnitude":         float(stats_row["avg_mag"]) if stats_row["avg_mag"] else 0,
        "max_magnitude":         float(stats_row["max_mag"]) if stats_row["max_mag"] else 0,
        "rata_rata_kedalaman":   avg_depth,
        "distribusi_magnitudo":  mag_ordered,
        "top_wilayah":           top_wilayah,
        "distribusi_kedalaman":  depth_stats,
        "wilayah_teraktif":      top_wilayah[0]["wilayah"] if top_wilayah else "N/A",
        "mllib":                 mllib_results,
        "last_updated":          datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }

    # Tulis ke dashboard/data/spark_results.json (sama seperti spark_processing.py)
    LOCAL_PATH = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "dashboard", "data", "spark_results.json"
    )
    with open(LOCAL_PATH, "w", encoding="utf-8") as f:
        json.dump(spark_results, f, ensure_ascii=False, indent=2)

    print(f"[+] Dashboard JSON tersimpan: {LOCAL_PATH}")

except Exception as e:
    print(f"[-] Gagal menyimpan spark_results.json: {e}")
    import traceback
    traceback.print_exc()


# ====================================================================
# VERIFIKASI AKHIR — Baca ulang semua Gold Delta
# ====================================================================
print("\n" + "="*50)
print("  VERIFIKASI GOLD LAYER (ETS)")
print("="*50)

gold_tables = {
    "Distribusi Magnitudo":  GOLD_DISTRIBUSI_MAG_PATH,
    "Top Wilayah":           GOLD_TOP_WILAYAH_PATH,
    "Distribusi Kedalaman":  GOLD_DISTRIBUSI_DEPTH_PATH,
    "Statistik Ringkasan":   GOLD_STATISTIK_PATH,
    "MLlib Tren":            GOLD_MLLIB_PATH,
}

for name, path in gold_tables.items():
    try:
        clean = path.replace("file://", "")
        if os.path.exists(clean):
            df_verify = spark.read.format("delta").load(path)
            row_count = df_verify.count()
            print(f"[+] {name:25s} : {row_count} baris  ✅")
            df_verify.show(3, truncate=False)
        else:
            print(f"[-] {name:25s} : tidak ditemukan  ⚠️")
    except Exception as e:
        print(f"[-] {name:25s} : error ({e})")


# ====================================================================
# TUTUP SPARK SESSION
# ====================================================================
print("\n[*] Menutup SparkSession...")
spark.stop()

print("\n" + "="*60)
print("  Gold Layer (ETS) selesai!")
print("="*60)
print(f"\n  Ringkasan:")
print(f"  Total gempa    : {spark_results.get('total_gempa', 'N/A')}")
print(f"  Avg magnitude  : {spark_results.get('avg_magnitude', 'N/A')}")
print(f"  Max magnitude  : {spark_results.get('max_magnitude', 'N/A')}")
print(f"  Avg kedalaman  : {spark_results.get('rata_rata_kedalaman', 'N/A')} km")
print(f"  Wilayah aktif  : {spark_results.get('wilayah_teraktif', 'N/A')}")
print(f"  Dashboard JSON : {LOCAL_PATH}")
print()
print("  📊 OUTPUT GOLD DELTA TABLES:")
print("  ┌─────────────────────────────────────────────────────────────────┐")
print("  │  1. gold/ets_distribusi_magnitudo  — Kategori Mikro~Kuat       │")
print("  │  2. gold/ets_top_wilayah           — Ranking wilayah aktif     │")
print("  │  3. gold/ets_distribusi_kedalaman  — Dangkal/Menengah/Dalam    │")
print("  │  4. gold/ets_statistik_ringkasan   — Summary stats             │")
print("  │  5. gold/ets_mllib_tren            — Linear Regression result  │")
print("  └─────────────────────────────────────────────────────────────────┘")
print("\n  💡 Restart Flask dashboard untuk melihat update di localhost:5000")
print("="*60 + "\n")
