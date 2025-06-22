from dotenv import load_dotenv
from typing import Optional
from enum import Enum
from pydantic import BaseModel
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

import os
import logging

class SensorType(str, Enum):
    GEMS = "gems"
    IAQ = "iaq"
    HEAT = "heat"
    GAS = "gas"
    TEST = "test"

class GemsData(BaseModel):
    building_id: str
    measurement_point_id: str
    wire: Optional[float]
    total_a: Optional[float]
    total_w: Optional[float]
    total_pf: Optional[float]
    r_v: Optional[float]
    r_a: Optional[float]
    r_w: Optional[float]
    r_pf: Optional[float]
    s_v: Optional[float]
    s_a: Optional[float]
    s_w: Optional[float]
    s_pf: Optional[float]
    t_v: Optional[float]
    t_a: Optional[float]
    t_w: Optional[float]
    t_pf: Optional[float]
    kwh_sum: Optional[float]
    kwh_export_sum: Optional[float]
    recorded_at: str

class IaqData(BaseModel):
    building_id: str
    measurement_point_id: str
    value: Optional[float]
    recorded_at: str

class HeatData(BaseModel):
    building_id: str
    measurement_point_id: str
    instant_flow: Optional[float]
    instant_heat: Optional[float]
    supply_temperature: Optional[float]
    return_temperature: Optional[float]
    cumulative_flow: Optional[float]
    cumulative_heat: Optional[float]
    recorded_at: str

class GasData(BaseModel):
    building_id: str
    measurement_point_id: str
    cumulative_flow: Optional[float]
    instant_flow: Optional[float]
    pressure: Optional[float]
    temp: Optional[float]
    recorded_at: str

load_dotenv()
SENSOR_TABLE_MAPPING = {
    "gems": os.getenv("ICEBERG_TABLE_GEMS"),
    "iaq": os.getenv("ICEBERG_TABLE_IAQ"),
    "heat": os.getenv("ICEBERG_TABLE_HEAT"),
    "gas": os.getenv("ICEBERG_TABLE_GAS"),
    "test": os.getenv("ICEBERG_TABLE_TEST"),
}

logger = logging.getLogger(__name__)
def get_table_name(sensor_type: str) -> str:
    table = SENSOR_TABLE_MAPPING.get(sensor_type)

    if not table:
        logger.error(f"Unknown or unset sensor_type: {sensor_type}")
        raise ValueError(f"Unknown or unset sensor_type: {sensor_type}")
    return table

def get_schema(sensor_type):
    if sensor_type == SensorType.GEMS:
        return StructType([
            StructField("building_id", StringType(), False),
            StructField("measurement_point_id", StringType(), False),
            StructField("wire", DoubleType(), True),
            StructField("total_a", DoubleType(), True),
            StructField("total_w", DoubleType(), True),
            StructField("total_pf", DoubleType(), True),
            StructField("r_v", DoubleType(), True),
            StructField("r_a", DoubleType(), True),
            StructField("r_w", DoubleType(), True),
            StructField("r_pf", DoubleType(), True),
            StructField("s_v", DoubleType(), True),
            StructField("s_a", DoubleType(), True),
            StructField("s_w", DoubleType(), True),
            StructField("s_pf", DoubleType(), True),
            StructField("t_v", DoubleType(), True),
            StructField("t_a", DoubleType(), True),
            StructField("t_w", DoubleType(), True),
            StructField("t_pf", DoubleType(), True),
            StructField("kwh_sum", DoubleType(), True),
            StructField("kwh_export_sum", DoubleType(), True),
            StructField("recorded_at", StringType(), False),
        ])

    if sensor_type == SensorType.IAQ:
        return StructType([
            StructField("building_id", StringType(), False),
            StructField("measurement_point_id", StringType(), False),
            StructField("value", DoubleType(), True),
            StructField("recorded_at", StringType(), False),
        ])

    if sensor_type == SensorType.HEAT:
        return StructType([
            StructField("building_id", StringType(), False),
            StructField("measurement_point_id", StringType(), False),
            StructField("instant_flow", DoubleType(), True),
            StructField("instant_heat", DoubleType(), True),
            StructField("supply_temperature", DoubleType(), True),
            StructField("return_temperature", DoubleType(), True),
            StructField("cumulative_flow", DoubleType(), True),
            StructField("cumulative_heat", DoubleType(), True),
            StructField("recorded_at", StringType(), False),
        ])

    if sensor_type == SensorType.GAS:
        return StructType([
            StructField("building_id", StringType(), False),
            StructField("measurement_point_id", StringType(), False),
            StructField("cumulative_flow", DoubleType(), True),
            StructField("instant_flow", DoubleType(), True),
            StructField("pressure", DoubleType(), True),
            StructField("temp", DoubleType(), True),
            StructField("recorded_at", StringType(), False),
        ])

    if sensor_type == SensorType.TEST:
        return StructType([
            StructField("building_id", StringType(), False),
            StructField("measurement_point_id", StringType(), False),
            StructField("value", DoubleType(), True),
            StructField("recorded_at", StringType(), False),
        ])

    logger.error(f"Unknown sensor type: {sensor_type}")
    raise ValueError(f"Unknown sensor type: {sensor_type}")