import json
import time
import os
from datetime import datetime
from kafka import KafkaConsumer, TopicPartition
from hdfs import InsecureClient

# --- Setup HDFS Client ---
hdfs_client = InsecureClient('http://localhost:9870', user='hadoop')

# Siapkan folder lokal untuk Dashboard
DASHBOARD_DIR = '../dashboard/data'
os.makedirs(DASHBOARD_DIR, exist_ok=True)

FLUSH_INTERVAL_SECONDS = 120  # flush ke HDFS setiap 2 menit

def save_to_hdfs_and_local(data, hdfs_path, local_path):
    try:
        # Cek apakah file sudah ada di HDFS (overwrite jika ada)
        try:
            hdfs_client.delete(hdfs_path)
        except Exception:
            pass
        hdfs_client.write(hdfs_path, data=json.dumps(data), encoding='utf-8')
        with open(local_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"  [ERROR] Gagal simpan ke {hdfs_path}: {e}")
        return False


if __name__ == "__main__":
    print("=" * 55)
    print("  GempaRadar — Consumer to HDFS")
    print(f"  Subscribe: gempa-api, gempa-rss")
    print(f"  Flush interval: {FLUSH_INTERVAL_SECONDS} detik")
    print("=" * 55)

    # Gunakan assign() + seek_to_beginning() — BYPASS group coordinator
    # Ini menghindari bug kafka-python-ng pada Python 3.13 Windows
    # (Invalid file descriptor: -1 di selectors saat ensure_active_group)
    # group_id tetap diset agar consumer group terdaftar di broker (untuk --describe LAG)
    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        group_id='gempa-consumer-group',
        auto_offset_reset='earliest',
        enable_auto_commit=False,          # manual commit setelah setiap flush
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    )
    tp_api = TopicPartition('gempa-api', 0)
    tp_rss = TopicPartition('gempa-rss', 0)
    consumer.assign([tp_api, tp_rss])
    consumer.seek_to_beginning(tp_api, tp_rss)
    print("Terhubung ke Kafka. Mulai membaca pesan...\n")

    buffer_api = []
    buffer_rss = []
    last_flush = time.time()

    try:
        while True:
            # Poll dengan timeout 1 detik
            records = consumer.poll(timeout_ms=1000)

            for tp, messages in records.items():
                for msg in messages:
                    if tp.topic == 'gempa-api':
                        buffer_api.append(msg.value)
                    elif tp.topic == 'gempa-rss':
                        buffer_rss.append(msg.value)

            # Tampilkan buffer size setiap 10 detik
            elapsed = time.time() - last_flush
            if int(elapsed) % 10 == 0 and int(elapsed) > 0:
                print(f"  Buffer: {len(buffer_api)} API msgs, {len(buffer_rss)} RSS msgs "
                      f"| Flush dalam {int(FLUSH_INTERVAL_SECONDS - elapsed)}s", end='\r')

            # Flush ke HDFS setiap FLUSH_INTERVAL_SECONDS
            if elapsed >= FLUSH_INTERVAL_SECONDS:
                timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M")
                print(f"\n[{timestamp_str}] Flushing buffer ke HDFS...")

                if buffer_api:
                    hdfs_path = f"/data/gempa/api/{timestamp_str}.json"
                    local_path = os.path.join(DASHBOARD_DIR, 'live_api.json')
                    if save_to_hdfs_and_local(buffer_api, hdfs_path, local_path):
                        print(f"  ✓ API: {len(buffer_api)} record → {hdfs_path}")
                    buffer_api = []
                else:
                    print("  ℹ API buffer kosong, skip.")

                if buffer_rss:
                    hdfs_path = f"/data/gempa/rss/{timestamp_str}.json"
                    local_path = os.path.join(DASHBOARD_DIR, 'live_rss.json')
                    if save_to_hdfs_and_local(buffer_rss, hdfs_path, local_path):
                        print(f"  ✓ RSS: {len(buffer_rss)} artikel → {hdfs_path}")
                    buffer_rss = []
                else:
                    print("  ℹ RSS buffer kosong, skip.")

                last_flush = time.time()
                # Commit offset ke broker agar LAG terpantau via kafka-consumer-groups.sh
                try:
                    consumer.commit()
                except Exception as ce:
                    print(f"  [WARN] Commit offset gagal (non-fatal): {ce}")

    except KeyboardInterrupt:
        print("\n\nConsumer dihentikan.")
    finally:
        consumer.close()