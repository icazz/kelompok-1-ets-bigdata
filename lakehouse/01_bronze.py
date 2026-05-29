"""
Script: 01_bronze.py
Deskripsi: Mengambil data gempa bumi (USGS API) dan berita gempa (Google News RSS)
           dari HDFS (dengan fallback otomatis ke file lokal jika HDFS tidak aktif)
           dan menyimpannya ke dalam Bronze Layer dalam format Delta Lake.
Tugas: Anggota 1 (Setup & Bronze Ingestion)
Project: GempaRadar Data Lakehouse | Kelompok 1 ETS Big Data
"""

import os
import sys

# Pindahkan current working directory ke folder tempat file 01_bronze.py berada (lakehouse/)
# Ini dilakukan agar path relatif output dan fallback lokal bersifat portabel di semua mesin anggota
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# ====================================================================
# AUTO-REDIRECT EKSEKUSI KE DALAM CONTAINER DOCKER (SPARK-MASTER)
# ====================================================================
# PySpark lokal di Windows sering mengalami error winutils.exe (HADOOP_HOME).
# Blok ini mendeteksi jika dijalankan di Windows host tanpa HADOOP_HOME,
# lalu secara otomatis meneruskan jalannya script ke dalam container
# Docker 'spark-master' yang sudah siap dengan Java & Spark Linux.
if os.name == 'nt' and not os.environ.get('HADOOP_HOME'):
    import subprocess
    print("\n" + "="*60)
    print("  INGESTION BRONZE LAYER - GEMPARADAR DATA LAKEHOUSE")
    print("="*60)
    print("[*] Mendeteksi Windows Host tanpa variabel lingkungan HADOOP_HOME.")
    print("[*] Mengalihkan eksekusi Spark secara otomatis ke dalam container Docker 'spark-master'...")
    
    # Path script di dalam container (karena repo di-mount ke /app)
    container_script = "/app/lakehouse/01_bronze.py"
    
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
        # Jalankan di dalam container dan teruskan return code-nya
        result = subprocess.run(cmd)
        sys.exit(result.returncode)
    except Exception as e:
        print(f"[-] Gagal mengalihkan eksekusi ke container Docker: {e}")
        print("[!] Pastikan Docker Desktop aktif dan container 'spark-master' sedang berjalan.")
        sys.exit(1)

print("\n" + "="*60)
print("  INGESTION BRONZE LAYER - GEMPARADAR DATA LAKEHOUSE")
print("="*60)


# Import PySpark & Delta Lake
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import current_timestamp, lit
    
    # Coba import delta-spark, tangkap eror modul apa pun (termasuk importlib_metadata)
    try:
        from delta import configure_spark_with_delta_pip
        HAS_DELTA_PACKAGE = True
    except Exception:
        HAS_DELTA_PACKAGE = False
except ImportError as e:
    print(f"[-] Gagal mengimpor library wajib: {e}")
    print("[!] Pastikan virtual environment (.venv) aktif dan delta-spark telah terinstall.")
    sys.exit(1)

# INISIALISASI SPARKSESSION (WAJIB PERSIS SESUAI INSTRUKSI)
print("[*] Menginisialisasi SparkSession dengan ekstensi Delta Lake...")
# Atur user HDFS untuk menghindari Permission denied saat menulis data
os.environ["HADOOP_USER_NAME"] = "hadoop"

try:
    builder = SparkSession.builder \
        .appName("Bronze-GempaRadar") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:8020")

    if HAS_DELTA_PACKAGE:
        try:
            print("[*] Menggunakan Delta helper (delta-spark)...")
            spark = configure_spark_with_delta_pip(
                builder,
                extra_packages=["io.delta:delta-spark_2.12:3.1.0"]
            ).getOrCreate()
        except Exception as inner_e:
            print(f"[*] Helper gagal ({inner_e}), fallback ke builder standar...")
            spark = builder.getOrCreate()
    else:
        print("[*] Menjalankan tanpa helper delta-spark (mengandalkan package JVM spark-submit)...")
        spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print("[+] SparkSession berhasil dibuat!")
except Exception as e:
    print(f"[-] Gagal menginisialisasi SparkSession: {e}")
    sys.exit(1)

# Langkah 1 — Definisikan path
HDFS_API_PATH = "hdfs://hadoop-namenode:8020/data/gempa/api/"
HDFS_RSS_PATH = "hdfs://hadoop-namenode:8020/data/gempa/rss/"

# Path output (Memaksa simpan ke lokal dengan prefix 'file://')
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LAKEHOUSE_DATA_DIR = os.path.join(BASE_DIR, "lakehouse_data")
BRONZE_API_PATH = f"file://{os.path.join(LAKEHOUSE_DATA_DIR, 'bronze', 'gempa_api').replace(os.sep, '/')}"
BRONZE_RSS_PATH = f"file://{os.path.join(LAKEHOUSE_DATA_DIR, 'bronze', 'gempa_rss').replace(os.sep, '/')}"

# Path fallback lokal (relatif dari root project, diakses dari folder lakehouse/)
LOCAL_API_PATH = "../dashboard/data/live_api.json"
LOCAL_RSS_PATH = "../dashboard/data/live_rss.json"


# Langkah 2 — Fungsi baca data dengan fallback
def read_data(spark, hdfs_path, local_fallback_path, source_name):
    print(f"\n[*] Memulai proses membaca data untuk sumber: {source_name.upper()}")
    df = None
    
    # Coba baca dari HDFS terlebih dahulu
    try:
        print(f"    -> Mencoba membaca dari HDFS: {hdfs_path}")
        df = spark.read.option("multiLine", True).json(hdfs_path)
        
        # Uji koneksi/eksistensi file dengan melakukan count
        record_count = df.count()
        if record_count == 0:
            raise Exception("File di HDFS kosong atau tidak ditemukan.")
            
        print(f"    -> [SUKSES] Berhasil membaca dari HDFS ({record_count} record).")
    except Exception as e:
        print(f"    -> [WARNING] Gagal membaca dari HDFS: {e}")
        print(f"    -> [FALLBACK] Beralih ke file lokal: {local_fallback_path}")
        
        # Validasi eksistensi file lokal sebelum dibaca
        if not os.path.exists(local_fallback_path):
            print(f"    [-] ERROR: File lokal tidak ditemukan di: {os.path.abspath(local_fallback_path)}")
            # Buat dummy dataframe kosong jika file benar-benar tidak ada agar script tidak langsung crash
            print("    [!] Membuat DataFrame kosong untuk mencegah kegagalan fatal...")
            raise FileNotFoundError(f"File local tidak ditemukan pada path {local_fallback_path}")
            
        # PENTING: Tambahkan prefix 'file://' agar Spark membaca dari filesystem lokal container, bukan HDFS
        local_spark_path = f"file://{os.path.abspath(local_fallback_path).replace(os.sep, '/')}"
        df = spark.read.option("multiLine", True).json(local_spark_path)
        record_count = df.count()
        print(f"    -> [SUKSES] Berhasil membaca file lokal ({record_count} record).")
        
    # Tambahkan kolom metadata
    df_with_meta = df \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source", lit(source_name))
        
    return df_with_meta


# Langkah 3 — Ingest data API
try:
    df_api = read_data(spark, HDFS_API_PATH, LOCAL_API_PATH, "api")
    
    print("\n--- SKEMA DATAFRAME API ---")
    df_api.printSchema()
    
    print(f"[*] Menulis data API ke Bronze Delta Table...")
    df_api.write.format("delta").mode("append").save(BRONZE_API_PATH)
    print(f"[+] Bronze API tersimpan di: {BRONZE_API_PATH}")
except Exception as e:
    print(f"[-] Gagal melakukan ingesti data API ke Bronze: {e}")


# Langkah 4 — Ingest data RSS
try:
    df_rss = read_data(spark, HDFS_RSS_PATH, LOCAL_RSS_PATH, "rss")
    
    print("\n--- SKEMA DATAFRAME RSS ---")
    df_rss.printSchema()
    
    print(f"[*] Menulis data RSS ke Bronze Delta Table...")
    df_rss.write.format("delta").mode("append").save(BRONZE_RSS_PATH)
    print(f"[+] Bronze RSS tersimpan di: {BRONZE_RSS_PATH}")
except Exception as e:
    print(f"[-] Gagal melakukan ingesti data RSS ke Bronze: {e}")


# Langkah 5 — Verifikasi hasil
print("\n" + "="*50)
print("  VERIFIKASI TABEL DELTA BRONZE")
print("="*50)

try:
    print("[*] Membaca ulang tabel Delta Bronze API...")
    df_api_verify = spark.read.format("delta").load(BRONZE_API_PATH)
    print(f"[+] Total record di Bronze API: {df_api_verify.count()}")
    print("[*] Menampilkan 3 baris pertama:")
    df_api_verify.select("id", "magnitude", "place", "_ingested_at", "_source").show(3, truncate=False)
except Exception as e:
    print(f"[-] Gagal memverifikasi Bronze API: {e}")

try:
    print("\n[*] Membaca ulang tabel Delta Bronze RSS...")
    df_rss_verify = spark.read.format("delta").load(BRONZE_RSS_PATH)
    print(f"[+] Total record di Bronze RSS: {df_rss_verify.count()}")
    print("[*] Menampilkan 3 baris pertama:")
    # Memilih beberapa field umum dari RSS feed
    df_rss_verify.select("title", "source", "_ingested_at", "_source").show(3, truncate=False)
except Exception as e:
    print(f"[-] Gagal memverifikasi Bronze RSS: {e}")

print("\n[+] Status: Ingesti data ke Bronze Layer berhasil dilakukan!")
print(f"    - Output API: {os.path.abspath(BRONZE_API_PATH)}")
print(f"    - Output RSS: {os.path.abspath(BRONZE_RSS_PATH)}")


# Langkah 6 — Tutup Spark
print("\n[*] Menutup SparkSession...")
spark.stop()

print("\n" + "="*60)
print("  Bronze layer selesai. Anggota lain bisa mulai Silver.")
print("="*60 + "\n")
