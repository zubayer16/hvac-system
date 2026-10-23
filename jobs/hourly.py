"""Spark batch backfill and actual Iceberg maintenance against the shared REST catalog."""

import os
import re

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("buildingpilot-hourly")
    .config(
        "spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lake.type", "rest")
    .config("spark.sql.catalog.lake.uri", os.environ["BP_CATALOG_URI"])
    .config("spark.sql.catalog.lake.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.lake.s3.endpoint", os.environ["BP_S3_ENDPOINT"])
    .config("spark.sql.catalog.lake.s3.path-style-access", "true")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
namespace = os.getenv("BP_LAKE_NAMESPACE", "buildingpilot")
if re.fullmatch(r"[a-z][a-z0-9_]*", namespace) is None:
    raise ValueError("Invalid namespace")
try:
    count = spark.table(f"lake.{namespace}.cleaned_telemetry").count()
    assert count > 0, "Python must write cleaned telemetry before Spark transforms it"
    spark.sql(f"""
        CREATE OR REPLACE TABLE lake.{namespace}.hourly_measurements USING iceberg AS
        SELECT building_id, equipment_id, source, date_trunc('hour', timestamp) AS hour,
               sum(energy_kwh) AS energy_kwh, avg(indoor_temperature_c) AS indoor_temperature_c,
               avg(outdoor_temperature_c) AS outdoor_temperature_c, avg(occupancy) AS occupancy,
               count(*) AS observations
        FROM lake.{namespace}.cleaned_telemetry
        GROUP BY building_id, equipment_id, source, date_trunc('hour', timestamp)
    """)
    spark.sql(
        f"CALL lake.system.rewrite_data_files(table => '{namespace}.cleaned_telemetry')"
    ).show()
    # Conservative smoke-test expiration: retain at least five hourly snapshots.
    spark.sql(
        f"CALL lake.system.expire_snapshots(table => '{namespace}.hourly_measurements', "
        "older_than => TIMESTAMP '2000-01-01 00:00:00', retain_last => 5)"
    ).show()
    print(f"SPARK_CROSS_ENGINE_OK observations={count}")
finally:
    spark.stop()
