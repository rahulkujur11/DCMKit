from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
import logging
import os

from .scd import merge_scd_type2
from .scd import generate_dim_time_from_values


logger = logging.getLogger(__name__)

GOLD_PATH = os.path.join(os.getcwd(), "datalake", "gold")


def transform_demand_gold(input_data):
    logger.info("🔧 Starting transform_demand_gold...")

    dim_material, fact_material_demand = [], []
    dim_customer, dim_supplier, dim_demand_category = set(), set(), set()
    point_in_time_values = set()

    for record in input_data:
        material_number_customer = record.get("materialNumberCustomer")
        material_number_supplier = record.get("materialNumberSupplier")
        material_desc = record.get("materialDescriptionCustomer")
        global_id = record.get("materialGlobalAssetId")
        unit = record.get("unitOfMeasure")
        unit_omit = record.get("unitOfMeasureIsOmitted")

        customer = record.get("customer")
        supplier = record.get("supplier")
        category_code = record.get("DemandCategoryCode") or record.get("demandCategoryCode")
        demand_id = record.get("materialDemandId")
        changed_at = record.get("changedAt")

        point_in_time = record.get("pointInTime")
        demand_qty = record.get("demand")
        inactive = record.get("materialDemandIsInactive")

        dim_material.append({
            "MaterialNumberCustomer": material_number_customer,
            "MaterialNumberSupplier": material_number_supplier,
            "MaterialDescription": material_desc,
            "MaterialGlobalAssetId": global_id,
            "UnitOfMeasure": unit,
            "UnitOfMeasureIsOmitted": unit_omit
        })

        if customer: dim_customer.add(customer)
        if supplier: dim_supplier.add(supplier)
        if category_code: dim_demand_category.add(category_code)
        if point_in_time: point_in_time_values.add(point_in_time)

        if isinstance(point_in_time, (date, datetime)):
            point_date = point_in_time.isoformat() if isinstance(point_in_time, datetime) else point_in_time
        else:
            point_date = str(point_in_time)


        fact_material_demand.append({
            "DemandID": demand_id,
            "MaterialNumberCustomer": material_number_customer,
            "CustomerBPNL": customer,
            "SupplierBPNL": supplier,
            "DemandCategoryCode": category_code,
            "PointInTime": point_date,
            "DemandQuantity": demand_qty,
            "MaterialDemandIsInactive": inactive,
            "ChangedAt": changed_at
        })

    spark = SparkSession.getActiveSession()

    outputs = {
        "DimMaterial": spark.createDataFrame(dim_material),
        "DimCustomer": spark.createDataFrame([{"CustomerBPNL": bpn} for bpn in dim_customer]),
        "DimSupplier": spark.createDataFrame([{"SupplierBPNL": bpn} for bpn in dim_supplier]),
        "DimDemandCategory": spark.createDataFrame([{"DemandCategoryCode": code} for code in dim_demand_category]),
        "FactMaterialDemand": spark.createDataFrame(fact_material_demand),
        "DimTime": generate_dim_time_from_values(point_in_time_values)
    }

    gold_base_path = os.path.join(os.getcwd(), "datalake", "gold", "demand")

    for name, df in outputs.items():
        target_path = os.path.join(gold_base_path, name)

        if name.startswith("Dim"):
            logger.info(f"📦 Merging Dimension table {name} with SCD Type 2 logic.")
            # Explicit keys for each dimension
            if name == "DimMaterial":
                keys = ["MaterialNumberCustomer", "MaterialNumberSupplier"]
                compare_columns = ["MaterialDescription", "MaterialGlobalAssetId", "UnitOfMeasure", "UnitOfMeasureIsOmitted"]
            elif name == "DimCustomer":
                keys = ["CustomerBPNL"]
                compare_columns = []
            elif name == "DimSupplier":
                keys = ["SupplierBPNL"]
                compare_columns = []
            elif name == "DimDemandCategory":
                keys = ["DemandCategoryCode"]
                compare_columns = []
            else:
                logger.warning(f"⚠️ Unknown dimension {name}. Skipping SCD merge.")
                continue

            merge_scd_type2(
                spark=spark,
                incoming_df=df,
                path=target_path,
                keys=keys,
                compare_columns=compare_columns
            )
        else:
            logger.info(f"💾 Writing Fact table: {name}")
            df.write.format("delta").mode("overwrite").save(target_path)

    return outputs

def transform_capacity_gold(input_data):
    logger.info("🔧 Starting transform_capacity_gold...")

    fact_capacity_group, dim_material = [], []
    dim_customer, dim_supplier, dim_demand_category = set(), set(), set()
    point_in_time_values = set()

    for record in input_data:
        dim_customer.add(record.get("CustomerBPNL"))
        dim_supplier.add(record.get("SupplierBPNL"))
        dim_demand_category.add(record.get("DemandCategoryCode"))

        dim_material.append({
            "MaterialNumberCustomer": record.get("MaterialNumberCustomer"),
            "MaterialNumberSupplier": record.get("MaterialNumberSupplier"),
            "MaterialDescription": record.get("MaterialDescription"),
            "MaterialGlobalAssetId": record.get("MaterialGlobalAssetId"),
            "UnitOfMeasure": record.get("UnitOfMeasure"),
            "UnitOfMeasureIsOmitted": record.get("UnitOfMeasureIsOmitted")
        })

        point_in_time = record.get("PointInTime")
        point_in_time_values.add(point_in_time)

        fact_capacity_group.append({
            "CapacityGroupID": record.get("CapacityGroupID"),
            "MaterialNumberCustomer": record.get("MaterialNumberCustomer"),
            "CustomerBPNL": record.get("CustomerBPNL"),
            "SupplierBPNL": record.get("SupplierBPNL"),
            "DemandCategoryCode": record.get("DemandCategoryCode"),
            "PointInTime": point_in_time,
            "ActualCapacity": record.get("ActualCapacity"),
            "MaxCapacity": record.get("MaxCapacity"),
            "AgreedCapacity": record.get("AgreedCapacity"),
            "DeltaProductionResult": record.get("DeltaProductionResult"),
            "ChangedAt": record.get("ChangedAt"),
            "StartReferenceDateTime": record.get("StartReferenceDateTime"),
            "MeasurementIntervalWeeks": record.get("MeasurementIntervalWeeks"),
            "ParentCapacityGroupID": record.get("ParentCapacityGroupID")
        })

    spark = SparkSession.getActiveSession()

    outputs = {
        "FactCapacityGroup": spark.createDataFrame(fact_capacity_group),
        "DimCustomer": spark.createDataFrame([{"CustomerBPNL": c} for c in dim_customer if c]),
        "DimSupplier": spark.createDataFrame([{"SupplierBPNL": s} for s in dim_supplier if s]),
        "DimDemandCategory": spark.createDataFrame([{"DemandCategoryCode": c} for c in dim_demand_category if c]),
        "DimMaterial": spark.createDataFrame(dim_material),
        "DimTime": generate_dim_time_from_values(point_in_time_values)
    }

    for name, df in outputs.items():
        logger.info(f"✅ {name} created with {df.count()} rows")
        df.printSchema()

    return outputs


def transform_comment_gold(input_data):
    logger.debug(f"📝 Transforming bronze comment data with {len(input_data)} records")
    return input_data  # Assuming it's a pass-through
