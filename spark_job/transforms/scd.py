from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable


from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
import logging

logger = logging.getLogger(__name__)


def generate_dim_time_from_values(date_strs):
    logger.info("⏳ Generating DimTime from pointInTime values...")

    dates = set()

    for s in date_strs:
        if isinstance(s, (date, datetime)):
            dates.add(s.date() if isinstance(s, datetime) else s)
        elif isinstance(s, str):
            try:
                dates.add(datetime.fromisoformat(s.strip()).date())
            except Exception:
                logger.warning(f"⚠️ Skipped invalid date: {s}")

    spark = SparkSession.getActiveSession()

    if not dates:
        logger.warning("⚠️ No valid pointInTime values found. Returning empty DimTime DataFrame.")
        schema = StructType([
            StructField("DateKey", LongType(), True),
            StructField("FullDate", StringType(), True),
            StructField("Day", IntegerType(), True),
            StructField("Month", IntegerType(), True),
            StructField("MonthName", StringType(), True),
            StructField("Quarter", IntegerType(), True),
            StructField("Year", IntegerType(), True),
            StructField("DayOfWeek", IntegerType(), True),
            StructField("WeekOfYear", IntegerType(), True),
            StructField("IsWeekend", BooleanType(), True)
        ])
        return spark.createDataFrame([], schema)

    rows = [{
        "DateKey": int(d.strftime("%Y%m%d")),
        "FullDate": d.isoformat(),
        "Day": d.day,
        "Month": d.month,
        "MonthName": d.strftime("%B"),
        "Quarter": (d.month - 1) // 3 + 1,
        "Year": d.year,
        "DayOfWeek": d.isoweekday(),
        "WeekOfYear": d.isocalendar()[1],
        "IsWeekend": d.weekday() >= 5
    } for d in sorted(dates)]

    df = spark.createDataFrame(rows)
    logger.info(f"✅ DimTime generated with {df.count()} rows")
    return df


def merge_scd_type2(spark: SparkSession, incoming_df: DataFrame, path: str, keys: list, compare_columns: list = None):
    """
    Performs a Type 2 SCD merge of incoming_df into the Delta table at path.

    Args:
        spark: Active SparkSession
        incoming_df (DataFrame): New data to merge.
        path (str): Delta table path.
        keys (list): Columns that uniquely identify a business entity.
        compare_columns (list): Columns to detect changes. If None, all non-key columns are compared.
    """
    logger.info("🔑 Adding SCD Type 2 columns: SurrogateKey, ValidFrom, ValidTo, IsCurrent")
    incoming_df = incoming_df \
        .withColumn("SurrogateKey", F.monotonically_increasing_id()) \
        .withColumn("ValidFrom", F.current_timestamp()) \
        .withColumn("ValidTo", F.lit("9999-12-31 23:59:59").cast("timestamp")) \
        .withColumn("IsCurrent", F.lit(True))

    try:
        existing_df = spark.read.format("delta").load(path)
        logger.info(f"📂 Found existing Delta table at {path}")
    except AnalysisException:
        logger.info(f"📦 Delta table not found at {path}. Creating new table with SCD columns.")
        incoming_df.write.format("delta") \
            .option("mergeSchema", "true") \
            .option("overwriteSchema", "true") \
            .mode("overwrite").save(path)
        return

    # 🔥 Patch: Add missing SCD columns to existing_df
    added = False
    if "SurrogateKey" not in existing_df.columns:
        logger.warning("⚠️ Existing table missing 'SurrogateKey', adding default column.")
        existing_df = existing_df.withColumn("SurrogateKey", F.monotonically_increasing_id())
        added = True
    if "ValidFrom" not in existing_df.columns:
        logger.warning("⚠️ Existing table missing 'ValidFrom', adding default column.")
        existing_df = existing_df.withColumn("ValidFrom", F.lit("1900-01-01 00:00:00").cast("timestamp"))
        added = True
    if "ValidTo" not in existing_df.columns:
        logger.warning("⚠️ Existing table missing 'ValidTo', adding default column.")
        existing_df = existing_df.withColumn("ValidTo", F.lit("9999-12-31 23:59:59").cast("timestamp"))
        added = True
    if "IsCurrent" not in existing_df.columns:
        logger.warning("⚠️ Existing table missing 'IsCurrent', adding default column.")
        existing_df = existing_df.withColumn("IsCurrent", F.lit(True))
        added = True

    if added:
        logger.info(f"📦 Updating Delta table schema at {path} to include SCD columns.")
        existing_df.write.format("delta") \
            .option("mergeSchema", "true") \
            .option("overwriteSchema", "true") \
            .mode("overwrite").save(path)

    # 📌 Select columns for change detection
    if compare_columns is None:
        compare_columns = [c for c in incoming_df.columns if c not in keys + ["SurrogateKey", "ValidFrom", "ValidTo", "IsCurrent"]]
    logger.info(f"📌 Change detection columns: {compare_columns}")

    # 🔁 Build join and change detection conditions
    join_cond = " AND ".join([f"existing.{k} = incoming.{k}" for k in keys])
    if compare_columns:
        change_cond = " OR ".join([f"existing.{c} <> incoming.{c}" for c in compare_columns])
    else:
        change_cond = "false"  # No non-key columns to compare

    delta_table = DeltaTable.forPath(spark, path)
    logger.info(f"🔁 MERGE condition: {join_cond}")

    # 🚀 Perform SCD Type 2 merge
    delta_table.alias("existing").merge(
        source=incoming_df.alias("incoming"),
        condition=f"{join_cond} AND existing.IsCurrent = true"
    ).whenMatchedUpdate(
        condition=change_cond,
        set={
            "ValidTo": F.current_timestamp(),
            "IsCurrent": F.lit(False)
        }
    ).whenNotMatchedInsertAll().execute()

    logger.info(f"✅ SCD Type 2 merge complete for {path}")