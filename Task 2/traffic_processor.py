import os
from pyflink.table import TableEnvironment, EnvironmentSettings

def main():
    # 1. Initialize Table Environment
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    # 2. Register Kafka SQL Connector JAR
    table_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        "file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar"
    )

    print("[*] Initialized PyFlink Table Environment.")

    # 3. Create Kafka Source Table
    source_ddl = """
        CREATE TABLE kafka_telemetry (
            camera_id STRING,
            timestamp_ms BIGINT,
            volume INT,
            location STRING,
            event_time AS TO_TIMESTAMP(FROM_UNIXTIME(timestamp_ms / 1000)),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'traffic-telemetry',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink-traffic-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """
    table_env.execute_sql(source_ddl)
    print("[*] Created Kafka Source Table 'kafka_telemetry'.")

    # 4. Create Print Sink Table for TaskManager Output
    sink_ddl = """
        CREATE TABLE print_sink (
            camera_id STRING,
            location STRING,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            total_volume INT,
            total_records BIGINT
        ) WITH (
            'connector' = 'print'
        )
    """
    table_env.execute_sql(sink_ddl)
    print("[*] Created Print Sink Table.")

    # 5. Submit 1-Minute Tumbling Window Aggregation to Sink
    insert_sql = """
        INSERT INTO print_sink
        SELECT
            camera_id,
            location,
            TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end,
            SUM(volume) AS total_volume,
            COUNT(1) AS total_records
        FROM kafka_telemetry
        GROUP BY
            camera_id,
            location,
            TUMBLE(event_time, INTERVAL '1' MINUTE)
    """

    print("[*] Submitting PyFlink job to cluster...")
    table_env.execute_sql(insert_sql)

if __name__ == "__main__":
    main()