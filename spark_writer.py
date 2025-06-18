from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException
from pyspark.sql import functions
from typing import Optional, Dict
import json
import os
import logging

from sensor_schema import get_schema, get_table_name


class SparkInsertError(Exception):
    """Custom Exception for spark_insert errors."""
    pass

logger = logging.getLogger(__name__)

def spark_insert(context):
    spark: Optional[SparkSession] = None
    building_id: Optional[str] = None
    sensor_type: Optional[str] = None

    try:
        if isinstance(context, str):
            body: Dict = json.loads(context)
        elif isinstance(context, dict):
            body = context
        else:
            raise SparkInsertError("context must be str or dict")

        sensor_type = body.get("sensor_type")
        building_id = body.get("building_id")
        data = body.get("data")

        if not (sensor_type and building_id and data):
            raise SparkInsertError(f"Missing field: type/building_id/data [building_id:{building_id}] [type:{sensor_type}]")

        schema: StructType = get_schema(sensor_type)

        load_dotenv()
        spark_connect: Optional[str] = os.getenv("SPARK_CONNECT")
        if not spark_connect:
            raise SparkInsertError(f"SPARK_CONNECT ENV not set [building_id:{building_id}] [type:{sensor_type}]")

        spark = SparkSession.builder.remote(spark_connect).getOrCreate()

        df: DataFrame = spark.createDataFrame(data, schema=schema)
        df = df.withColumn("recorded_at", functions.to_timestamp("recorded_at"))

        table_name = get_table_name(sensor_type)
        df.write.format("iceberg").mode("append").save(table_name)
        print(f"##### Spark Insert End [building_id:{building_id}] [type:{sensor_type}] #####")
        return True

    except json.JSONDecodeError as e:
        logger.error(f"[INPUT ERROR][building_id:{building_id}][type:{sensor_type}] wrong JSON: {e}")
        raise

    except AnalysisException as e:
        logger.error(f"[SPARK ANALYSIS ERROR][building_id:{building_id}][type:{sensor_type}] {e}")
        raise

    except SparkInsertError as e:
        logger.error(f"[USAGE ERROR][building_id:{building_id}][type:{sensor_type}] {e}")
        raise

    except Exception as e:
        logger.error(f"[UNEXPECTED ERROR][building_id:{building_id}][type:{sensor_type}] {e.__class__.__name__}: {e}")
        raise

    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception as e:
                logger.error(f"[SPARK STOP ERROR][building_id:{building_id}][type:{sensor_type}] {e}")