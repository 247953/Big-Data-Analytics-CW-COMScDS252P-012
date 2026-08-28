import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# Configuration Parameters
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "my-super-secret-admin-token"
INFLUX_ORG = "szeged_org"
INFLUX_BUCKET = "szeged_weather"
CSV_FILE_PATH = r"C:\Users\mdrdk\MSc 25.2 Big Data Analytics\Task 1\weatherHistory.csv"
#"weatherHistory.csv"

def parse_and_ingest():
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    print("Reading Szeged Weather dataset...")
    df = pd.read_csv(CSV_FILE_PATH)

    # Convert timestamps to ISO UTC standard
    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'], utc=True)

    points = []
    print("Parsing rows into InfluxDB Line Protocol...")

    for _, row in df.iterrows():
        # Map timestamp
        timestamp = row['Formatted Date']

        # Construct Line Protocol Point
        point = Point("weather_metrics") \
            .tag("location", "Szeged_Hungary") \
            .tag("summary", str(row['Summary'])) \
            .tag("precip_type", str(row['Precip Type']) if pd.notna(row['Precip Type']) else "none") \
            .field("temperature", float(row['Temperature (C)'])) \
            .field("apparent_temperature", float(row['Apparent Temperature (C)'])) \
            .field("humidity", float(row['Humidity'])) \
            .field("wind_speed", float(row['Wind Speed (km/h)'])) \
            .field("wind_bearing", float(row['Wind Bearing (degrees)'])) \
            .field("visibility", float(row['Visibility (km)'])) \
            .field("pressure", float(row['Pressure (millibars)'])) \
            .time(timestamp, WritePrecision.NS)

        points.append(point)

        # Batch write every 1000 points for throughput efficiency
        if len(points) >= 1000:
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
            points = []
            print(f"Ingested batch up to: {timestamp}")

    # Write remaining points
    if points:
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)

    print("Data ingestion complete!")
    client.close()

if __name__ == "__main__":
    parse_and_ingest()