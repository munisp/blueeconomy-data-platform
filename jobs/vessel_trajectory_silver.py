#!/usr/bin/env python3
"""Sedona batch job: bronze.vessel_observations -> silver.vessel_trajectories.

Deployed through the governed ``sedona-spark-jobs`` gitops chart with
``mainApplicationFile: local:///opt/blueeco/jobs/vessel_trajectory_silver.py``.
Apache Sedona is batch-only on this platform; the geo hot path stays on
PostGIS 3.3.

Engines (``--engine``):

- ``spark``  — the production path. Requires pyspark, apache-sedona and the
  Delta Spark connector in the batch image. Tracks are assembled with
  Sedona SQL (``ST_MakeLine`` over per-MMSI ordered ``ST_Point``
  geometries, ``ST_SimplifyPreserveTopology`` simplification, window-based
  segmentation on time gaps greater than two hours). All geometries are
  EPSG:4326. If Spark/Sedona is unavailable this engine exits non-zero with
  a clear message — there is no fake "spark mode".
- ``python`` — the bounded pure-Python reference path
  (:mod:`blueeconomy_data_platform.vessel_lakehouse`) for small backfills;
  refuses inputs larger than ``--max-python-points`` so it cannot be
  misused as an unbounded production engine.
- ``auto``   — Spark when the Sedona context is importable, otherwise the
  bounded Python path.

Both engines atomically rebuild ``silver.vessel_trajectories`` from
``bronze.vessel_observations``; the silver table is derived state, so
replays are idempotent.

Telemetry (Phase-7 OTel, OTEL_DESIGN.md §3 Sedona row): Sedona runs
on-Spark here, so coverage is split. The Python driver emits
``lakehouse.pipeline.vessel_trajectory_silver`` DAG spans around the
bronze-read / Sedona-SQL / silver-write phases (no-op unless
``OTEL_EXPORTER_OTLP_ENDPOINT`` is set). Executor/JVM coverage is a
deployment concern of the ``sedona-spark-jobs`` gitops chart: the OTel
Java agent on the Spark driver/executors plus the Spark Prometheus (JMX)
sink — that config does not live in this repository and is not fabricated
here.
"""

from __future__ import annotations

import argparse

GAP_HOURS_DEFAULT = 2.0
SIMPLIFY_TOLERANCE_DEFAULT = 0.0005
MAX_PYTHON_POINTS_DEFAULT = 200_000

SPARK_UNAVAILABLE_MESSAGE = (
    "Spark/Sedona engine requested but pyspark or apache-sedona is not importable. "
    "Run inside the governed sedona-spark-jobs batch image, or use "
    "--engine python for a bounded pure-Python backfill."
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Assemble silver.vessel_trajectories from bronze.vessel_observations "
            "(EPSG:4326) with Apache Sedona, or the bounded Python reference path."
        )
    )
    parser.add_argument("--bronze-uri", required=True, help="bronze.vessel_observations URI")
    parser.add_argument("--silver-uri", required=True, help="silver.vessel_trajectories URI")
    parser.add_argument(
        "--engine",
        choices=("auto", "spark", "python"),
        default="auto",
        help="Execution engine; 'spark' fails non-zero when Sedona is unavailable.",
    )
    parser.add_argument("--gap-hours", type=float, default=GAP_HOURS_DEFAULT)
    parser.add_argument("--simplify-tolerance", type=float, default=SIMPLIFY_TOLERANCE_DEFAULT)
    parser.add_argument("--max-python-points", type=int, default=MAX_PYTHON_POINTS_DEFAULT)
    return parser.parse_args()


def run_python_engine(arguments: argparse.Namespace) -> None:
    from datetime import timedelta

    from blueeconomy_data_platform.vessel_lakehouse import (
        read_bronze_observations,
        rebuild_silver_trajectories,
    )

    observations = read_bronze_observations(arguments.bronze_uri)
    if len(observations) > arguments.max_python_points:
        raise SystemExit(
            f"python engine is bounded to {arguments.max_python_points} observations "
            f"(input has {len(observations)}); use --engine spark for this volume"
        )
    version, rows = rebuild_silver_trajectories(
        arguments.bronze_uri,
        arguments.silver_uri,
        gap_threshold=timedelta(hours=arguments.gap_hours),
        simplify_tolerance=arguments.simplify_tolerance,
    )
    print(f"silver.vessel_trajectories rebuilt: rows={rows} table_version={version}")


def run_spark_engine(arguments: argparse.Namespace) -> None:
    try:
        from pyspark.sql import SparkSession, Window
        from pyspark.sql import functions as F
        from sedona.spark import SedonaContext
    except ImportError:
        raise SystemExit(SPARK_UNAVAILABLE_MESSAGE) from None

    spark = (
        SparkSession.builder.appName("blueeconomy-vessel-trajectory-silver")
        # Delta Lake support is provided by the batch image's delta-spark package.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    from blueeconomy_data_platform.telemetry import get_tracer

    sedona = SedonaContext.create(spark)
    sedona.conf.set("sedona.global.charset", "utf8")

    tracer = get_tracer()
    with tracer.start_as_current_span("lakehouse.sedona.read_bronze") as span:
        span.set_attribute("lakehouse.table", "bronze.vessel_observations")
        bronze = sedona.read.format("delta").load(arguments.bronze_uri)
        bronze.createOrReplaceTempView("bronze_vessel_observations")

    gap_seconds = arguments.gap_hours * 3600.0
    partition = Window.partitionBy("mmsi").orderBy("occurred_at", "event_id")

    flagged = (
        sedona.table("bronze_vessel_observations")
        .withColumn("geom", F.expr("ST_Point(longitude, latitude)"))
        .withColumn("epoch", F.col("occurred_at").cast("long"))
        .withColumn("previous_epoch", F.lag("epoch").over(partition))
        .withColumn(
            "new_segment",
            F.when(F.col("previous_epoch").isNull(), F.lit(1))
            .when(F.col("epoch") - F.col("previous_epoch") > F.lit(gap_seconds), F.lit(1))
            .otherwise(F.lit(0)),
        )
        .withColumn("segment_index", F.sum("new_segment").over(partition) - F.lit(1))
    )
    flagged.createOrReplaceTempView("flagged_observations")

    # Points are collected per segment and deterministically ordered by
    # (epoch, event_id) via sort_array before ST_MakeLine; ST_MakeLine over
    # the sorted array is the governed ST_MakeLine path, and
    # ST_SimplifyPreserveTopology applies the topology-preserving
    # simplification, both in EPSG:4326.
    with tracer.start_as_current_span("lakehouse.sedona.sql_transform") as span:
        span.set_attribute("lakehouse.table", "silver.vessel_trajectories")
        trajectories = sedona.sql(
            f"""
        WITH ordered AS (
          SELECT
            mmsi,
            segment_index,
            sort_array(
              collect_list(
                named_struct(
                  'epoch', epoch, 'event_id', event_id,
                  'lon', longitude, 'lat', latitude
                )
              )
            ) AS points,
            min(occurred_at) AS started_at,
            max(occurred_at) AS ended_at,
            count(1) AS point_count
          FROM flagged_observations
          GROUP BY mmsi, segment_index
        )
        SELECT
          sha2(
            concat(
              'vessel-trajectory/', mmsi, '/',
              element_at(points, 1).event_id, '/',
              element_at(points, -1).event_id
            ),
            256
          ) AS trajectory_id,
          mmsi,
          segment_index,
          started_at,
          ended_at,
          point_count,
          element_at(points, 1).event_id AS source_first_event_id,
          element_at(points, -1).event_id AS source_last_event_id,
          ST_AsText(ST_MakeLine(transform(points, p -> ST_Point(p.lon, p.lat))))
            AS geometry_wkt,
          ST_AsText(
            ST_SimplifyPreserveTopology(
              ST_MakeLine(transform(points, p -> ST_Point(p.lon, p.lat))),
              {float(arguments.simplify_tolerance)}
            )
          ) AS simplified_wkt,
          'EPSG:4326' AS crs
        FROM ordered
        """
        )

    with tracer.start_as_current_span("lakehouse.sedona.write_silver") as span:
        span.set_attribute("lakehouse.table", "silver.vessel_trajectories")
        (
            trajectories.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(arguments.silver_uri)
        )
        count = sedona.read.format("delta").load(arguments.silver_uri).count()
        span.set_attribute("lakehouse.rows", count)
    print(f"silver.vessel_trajectories rebuilt via Sedona: rows={count}")
    spark.stop()


def sedona_available() -> bool:
    try:
        import pyspark  # noqa: F401
        import sedona.spark  # noqa: F401
    except ImportError:
        return False
    return True


def main() -> None:
    arguments = parse_arguments()
    if not 0 < arguments.gap_hours <= 24 * 7:
        raise SystemExit("gap-hours must be within (0, 168]")
    if not 0 < arguments.simplify_tolerance <= 1.0:
        raise SystemExit("simplify-tolerance must be within (0, 1] degrees")
    engine = arguments.engine
    if engine == "spark" and not sedona_available():
        raise SystemExit(SPARK_UNAVAILABLE_MESSAGE)
    # Phase-7 OTel: DAG-level driver span for this batch run; no-op unless
    # OTEL_EXPORTER_OTLP_ENDPOINT is set (sanctioned fail-open).
    from blueeconomy_data_platform.telemetry import (
        get_tracer,
        init_telemetry,
        shutdown_telemetry,
    )

    init_telemetry(
        service_name="blueeconomy-data-platform-vessel-trajectory-silver", version="0.1.0"
    )
    try:
        with get_tracer().start_as_current_span(
            "lakehouse.pipeline.vessel_trajectory_silver",
            attributes={"lakehouse.engine": engine},
        ):
            if engine == "spark" or (engine == "auto" and sedona_available()):
                run_spark_engine(arguments)
            else:
                run_python_engine(arguments)
    finally:
        shutdown_telemetry()


if __name__ == "__main__":
    main()
