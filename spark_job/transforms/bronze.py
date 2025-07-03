import traceback
import logging
logger = logging.getLogger(__name__)


def transform_capacity_bronze(input_data):
    logger.debug(f"Transforming bronze capacity data: {input_data}")
    flattened_records = []

    try:
        base_fields = {
            "capacityGroupId": input_data.get("capacityGroupId"),
            "name": input_data.get("name"),
            "supplier": input_data.get("supplier"),
            "customer": input_data.get("customer"),
            "unitOfMeasure": input_data.get("unitOfMeasure"),
            "unitOfMeasureIsOmitted": input_data.get("unitOfMeasureIsOmitted"),
            "capacityGroupIsInactive": input_data.get("capacityGroupIsInactive"),
            "changedAt": input_data.get("changedAt"),
            "linkedCapacityGroups": input_data.get("linkedCapacityGroups", []),
            "supplierLocations": input_data.get("supplierLocations", []),
        }

        # Optional: extract volatility params
        volatility = input_data.get("demandVolatilityParameters", {})
        base_fields["measurementInterval"] = volatility.get("measurementInterval")
        base_fields["startReferenceDateTime"] = volatility.get("startReferenceDateTime")

        # Only keep first threshold for simplicity (or flatten more if needed)
        thresholds = volatility.get("rollingHorizonAlertThresholds", [])
        if thresholds:
            threshold = thresholds[0]
            base_fields.update({
                "sequenceNumber": threshold.get("sequenceNumber"),
                "absoluteNegativeDeviation": threshold.get("absoluteNegativeDeviation"),
                "relativeNegativeDeviation": threshold.get("relativeNegativeDeviation"),
                "absolutePositiveDeviation": threshold.get("absolutePositiveDeviation"),
                "relativePositiveDeviation": threshold.get("relativePositiveDeviation"),
                "subhorizonLength": threshold.get("subhorizonLength"),
            })

        # Optional: extract one linked demand series if needed
        linked_series = input_data.get("linkedDemandSeries", [])
        if linked_series:
            series = linked_series[0]
            base_fields.update({
                "materialNumberCustomer": series.get("materialNumberCustomer"),
                "materialNumberSupplier": series.get("materialNumberSupplier"),
                "customerLocation": series.get("customerLocation"),
                "demandCategoryCode": series.get("demandCategory", {}).get("demandCategoryCode"),
                "loadFactor": series.get("loadFactor")
            })

        for cap in input_data.get("capacities", []):
            flattened_record = {
                **base_fields,
                "pointInTime": cap.get("pointInTime"),
                "agreedCapacity": cap.get("agreedCapacity"),
                "actualCapacity": cap.get("actualCapacity"),
                "maximumCapacity": cap.get("maximumCapacity"),
                "deltaProductionResult": cap.get("deltaProductionResult"),
            }
            flattened_records.append(flattened_record)

        logger.debug(f"Flattened {len(flattened_records)} capacity records from input")
        return flattened_records

    except Exception as e:
        logger.error(f"[ERROR] Failed to transform capacity data: {e}")
        logger.debug(traceback.format_exc())
        return []


# === TRANSFORM FUNCTION ===
def transform_comment_bronze(input_data):
    logger.debug(f"Transforming bronze comments data: {input_data}")
    return input_data
    

def transform_demand_bronze(input_data):
    logger.debug(f"Transforming bronze demand data: {input_data}")
    flattened_records = []
    try:
        base_fields = {
            "materialDemandId": input_data.get("materialDemandId"),
            "materialNumberCustomer": input_data.get("materialNumberCustomer"),
            "materialNumberSupplier": input_data.get("materialNumberSupplier"),
            "materialDescriptionCustomer": input_data.get("materialDescriptionCustomer"),
            "materialGlobalAssetId": input_data.get("materialGlobalAssetId"),
            "customer": input_data.get("customer"),
            "supplier": input_data.get("supplier"),
            "changedAt": input_data.get("changedAt"),
            "unitOfMeasure": input_data.get("unitOfMeasure"),
            "unitOfMeasureIsOmitted": input_data.get("unitOfMeasureIsOmitted"),
            "materialDemandIsInactive": input_data.get("materialDemandIsInactive"),
        }

        for series in input_data.get("demandSeries", []):
            demand_category_code = series.get("demandCategory", {}).get("demandCategoryCode")
            customer_location = series.get("customerLocation")
            supplier_location = series.get("expectedSupplierLocation")

            for demand_entry in series.get("demands", []):
                flattened_record = {
                    **base_fields,
                    "demandCategoryCode": demand_category_code,
                    "customerLocation": customer_location,
                    "expectedSupplierLocation": supplier_location,
                    "demand": demand_entry.get("demand"),
                    "pointInTime": demand_entry.get("pointInTime"),
                }
                flattened_records.append(flattened_record)

        logger.debug(f"Flattened {len(flattened_records)} demand records from input")
        return flattened_records

    except Exception as e:
        logger.error(f"[ERROR] Failed to transform demand data: {e}")
        logger.debug(traceback.format_exc())
        return []





