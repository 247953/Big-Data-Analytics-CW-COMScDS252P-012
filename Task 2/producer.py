import json
import time
import urllib.request
import urllib.error
from kafka import KafkaProducer

# Direct REST endpoint with standard SODA parameters
REST_API_URL = "https://data.austintexas.gov/resource/sh59-i6y9.json?$limit=50&$order=read_date%20DESC"
KAFKA_BROKER = ["127.0.0.1:9094"]
TOPIC_NAME = "traffic-telemetry"

def create_producer():
    """Initialise the Kafka Producer Client"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )

def fetch_telemetry_data():
    """Fetches traffic telemetry directly from Socrata REST API"""
    req = urllib.request.Request(
        REST_API_URL,
        headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Accept': 'application/json'
        }
    )

    try: 
        with urllib.request.urlopen(req) as response:
            if response.status == 200:
                return json.loads(response.read().decode('utf-8'))
            return []
    except Exception as e:
        print(f"[!] Fetch Error: {e}")
        return []

def stream_telemetry():
    producer = create_producer()
    print(f"[*] Starting telemetry stream to Kafka topic '{TOPIC_NAME}'...")

    while True:
        try:
            records = fetch_telemetry_data()
            if not records:
                print("[!] No records returned. Retrying in 5s...")
                time.sleep(5)
                continue

            print(f"[*] Fetched {len(records)} records. Sending to Kafka...")

            for row in records:
                if isinstance(row, dict):
                    # Check field name variations returned by the REST endpoint
                    raw_cam = (
                        row.get("camera_id") or 
                        row.get("detector_id") or 
                        row.get("atd_location_id") or 
                        row.get("sensor_id")
                    )
                    
                    location = str(row.get("location_name") or "Austin, TX")

                    # Generate camera ID based on location hash if raw_cam is empty
                    if raw_cam is not None and str(raw_cam).strip() != "":
                        camera_id = str(raw_cam)
                    else:
                        camera_id = f"CAM_{abs(hash(location)) % 1000:03d}"

                    volume_val = row.get("volume") or row.get("count") or row.get("v")
                else:
                    continue

                # Live epoch timestamp so Flink event-time watermarks advance
                timestamp_ms = int(time.time() * 1000)

                # Parse volume safely
                try:
                    volume = int(volume_val)
                except (ValueError, TypeError):
                    volume = 0

                # key ingestion and serialization 
                payload = {
                    "camera_id": camera_id,
                    "timestamp_ms": timestamp_ms,
                    "volume": volume,
                    "location": location
                }

                producer.send(TOPIC_NAME, key=camera_id, value=payload)
                print(f"[->] Kafka Event: Camera={camera_id} | Vol={volume} | Time={timestamp_ms}")

                time.sleep(1) # Emit 1 record per second

        except Exception as e:
            print(f"[!] Error in stream loop: {e}")
            time.sleep(5)

if __name__ == "__main__":
    stream_telemetry()