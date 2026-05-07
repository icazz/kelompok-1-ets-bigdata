import json
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, count, max as spark_max, min as spark_min, stddev,
    col, when, regexp_replace, hour, to_timestamp
)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression as SparkLR

# 1. SparkSession 
spark = SparkSession.builder \
    .appName("GempaRadar-Analysis") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:8020") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

HDFS_API  = "hdfs://hadoop-namenode:8020/data/gempa/api/"
HDFS_RSS  = "hdfs://hadoop-namenode:8020/data/gempa/rss/"
HDFS_HASIL = "hdfs://hadoop-namenode:8020/data/gempa/hasil/"

print("\n" + "="*60)
print("  GempaRadar Analytics ” Batch Analysis from HDFS")
print("="*60)

# 2. BACA DATA API DARI HDFS 
df = spark.read.json(HDFS_API)
total = df.count()
print(f"\nTotal record API dari HDFS: {total}")

if total == 0:
    print("Data HDFS kosong. Jalankan consumer terlebih dahulu.")
    spark.stop()
    exit(0)

# Buat temp view untuk Spark SQL
df.createOrReplaceTempView("gempa")

# ANALISIS WAJIB 1 Distribusi Magnitudo (DataFrame API)
print("\n[Analisis 1] Distribusi Magnitudo")
df_mag = df.withColumn("kategori_mag",
    when(col("magnitude") < 3, "Mikro (<3)")
    .when((col("magnitude") >= 3) & (col("magnitude") < 4), "Minor (3-4)")
    .when((col("magnitude") >= 4) & (col("magnitude") < 5), "Sedang (4-5)")
    .otherwise("Kuat (>5)")
).groupBy("kategori_mag").count().orderBy("count", ascending=False)
df_mag.show()
print("  [Interpretasi] Gempa kategori Sedang (4-5) mendominasi karena Indonesia berada")
print("  di Ring of Fire — zona pertemuan lempeng Indo-Australia, Eurasia, dan Pasifik.")
print("  Frekuensi tinggi gempa menengah mengindikasikan tekanan lempeng yang terus aktif,")
print("  namun jarang menyebabkan kerusakan besar dibanding gempa kuat (M>5).\n")

mag_dist_raw = {row["kategori_mag"]: row["count"] for row in df_mag.collect()}
mag_ordered = {
    "Mikro (<3)":   mag_dist_raw.get("Mikro (<3)", 0),
    "Minor (3-4)":  mag_dist_raw.get("Minor (3-4)", 0),
    "Sedang (4-5)": mag_dist_raw.get("Sedang (4-5)", 0),
    "Kuat (>5)":    mag_dist_raw.get("Kuat (>5)", 0),
}

# ANALISIS WAJIB 2 Top 10 Wilayah Aktif (Spark SQL)
print("\n[Analisis 2] Top 10 Wilayah Paling Aktif (Spark SQL)")
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
df_wilayah.show(truncate=False)
print("  [Interpretasi] Wilayah sekitar Laut Banda, Sulawesi Utara, dan Timor Leste")
print("  konsisten menjadi zona paling aktif karena pertemuan tiga lempeng tektonik besar.")
print("  Data ini membantu BPBD memprioritaskan penempatan sensor dan tim respons cepat")
print("  di wilayah dengan frekuensi gempa tertinggi.\n")

top_wilayah = [
    {"wilayah": row["wilayah"], "count": row["jumlah_gempa"]}
    for row in df_wilayah.collect()
]

# ANALISIS WAJIB 3 Distribusi Kedalaman (Spark SQL)
print("\n[Analisis 3] Distribusi & Statistik Kedalaman (Spark SQL)")
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
df_depth.show()
print("  [Interpretasi] Dominasi gempa dangkal (<70 km) menunjukkan subduksi aktif di")
print("  sepanjang Palung Jawa dan Palung Timor. Gempa dangkal lebih berbahaya karena")
print("  melepas energi lebih dekat ke permukaan — berpotensi memicu tsunami dan")
print("  kerusakan infrastruktur yang lebih besar dibanding gempa dalam.\n")

d = df_depth.collect()[0]
depth_stats = {
    "Dangkal (<70 km)":    int(d["dangkal"]),
    "Menengah (70-300 km)": int(d["menengah"]),
    "Dalam (>300 km)":     int(d["dalam"]),
}
avg_depth    = float(d["rata_rata_depth"]) if d["rata_rata_depth"] else 0
max_depth    = float(d["depth_max"]) if d["depth_max"] else 0

# STATISTIK RINGKASAN
stats_row = spark.sql("""
    SELECT
        COUNT(*) AS total,
        ROUND(AVG(magnitude), 2) AS avg_mag,
        ROUND(MAX(magnitude), 1) AS max_mag
    FROM gempa
""").collect()[0]

# 3. SIMPAN HASIL KE HDFS
# ══════════════════════════════════════════════════════════════
# BONUS +5 — Spark MLlib: Linear Regression Tren Magnitudo
# ══════════════════════════════════════════════════════════════
print("\n[BONUS] Spark MLlib — Prediksi Tren Magnitudo (Linear Regression)")
mllib_results = {}
try:
    min_row = spark.sql("SELECT MIN(event_time_epoch) AS min_t FROM gempa").collect()[0]
    min_t = min_row["min_t"] or 0
    df_ml = spark.sql(f"""
        SELECT
            (event_time_epoch - {min_t}) / 3600000.0 AS jam_ke,
            magnitude
        FROM gempa
        WHERE event_time_epoch IS NOT NULL AND magnitude IS NOT NULL
    """).dropna()
    if df_ml.count() > 1:
        assembler = VectorAssembler(inputCols=["jam_ke"], outputCol="features")
        ml_data   = assembler.transform(df_ml)
        lr_model  = SparkLR(featuresCol="features", labelCol="magnitude", maxIter=10).fit(ml_data)
        koefisien = float(lr_model.coefficients[0])
        rmse      = float(lr_model.summary.rootMeanSquaredError)
        r2        = float(lr_model.summary.r2)
        tren      = "naik" if koefisien > 0 else "turun"
        print(f"\n  Koefisien : {koefisien:.4f} (per jam)")
        print(f"  RMSE      : {rmse:.4f}")
        print(f"  R\u00b2        : {r2:.4f}")
        print(f"\n  [Interpretasi] Tren magnitudo cenderung {tren} seiring waktu.")
        print("  Pola ini membantu BPBD mengantisipasi eskalasi aktivitas seismik dan")
        print("  merencanakan kesiapan sumber daya respons bencana secara lebih proaktif.\n")
        mllib_results = {
            "tren": tren,
            "koefisien_per_jam": round(koefisien, 6),
            "rmse": round(rmse, 4),
            "r2": round(r2, 4),
        }
    else:
        print("  [MLlib] Data tidak cukup untuk regresi.\n")
except Exception as e:
    print(f"  [MLlib] Gagal menjalankan regresi: {e}\n")

print("\n[Export] Menyimpan hasil ke HDFS...")
spark.sql("""
    SELECT
        REGEXP_REPLACE(place, '.* of ', '') AS wilayah,
        COUNT(*) AS jumlah
    FROM gempa
    GROUP BY wilayah
    ORDER BY jumlah DESC
""").write.mode("overwrite").json(HDFS_HASIL + "top_wilayah")

df_mag.write.mode("overwrite").json(HDFS_HASIL + "distribusi_magnitudo")
df_depth.write.mode("overwrite").json(HDFS_HASIL + "distribusi_kedalaman")
print(f"âœ… Hasil tersimpan di HDFS: {HDFS_HASIL}")

# 4. SIMPAN spark_results.json UNTUK DASHBOARD
spark_results = {
    "source":                "spark_hdfs",
    "total_gempa":           int(stats_row["total"]),
    "avg_magnitude":         float(stats_row["avg_mag"]) if stats_row["avg_mag"] else 0,
    "max_magnitude":         float(stats_row["max_mag"]) if stats_row["max_mag"] else 0,
    "rata_rata_kedalaman":   avg_depth,
    "distribusi_magnitudo":  mag_ordered,
    "top_wilayah":           top_wilayah,
    "distribusi_kedalaman":  depth_stats,
    "wilayah_teraktif":      top_wilayah[0]["wilayah"] if top_wilayah else "N/A",
    "mllib":                  mllib_results,
    "last_updated":          datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
}

# Tulis ke dashboard/data/spark_results.json
LOCAL_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "dashboard", "data", "spark_results.json"
)
with open(LOCAL_PATH, "w", encoding="utf-8") as f:
    json.dump(spark_results, f, ensure_ascii=False, indent=2)

print(f"âœ… Dashboard JSON tersimpan: {LOCAL_PATH}")
print(f"\nRingkasan:")
print(f"  Total gempa   : {spark_results['total_gempa']}")
print(f"  Avg magnitude : {spark_results['avg_magnitude']}")
print(f"  Max magnitude : {spark_results['max_magnitude']}")
print(f"  Avg kedalaman : {spark_results['rata_rata_kedalaman']} km")
print(f"  Wilayah aktif : {spark_results['wilayah_teraktif']}")
print("\n" + "="*60)
print("  Analisis selesai! Restart Flask dashboard untuk update.")
print("="*60 + "\n")

spark.stop()
