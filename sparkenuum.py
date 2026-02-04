from enum import Enum
import re
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType, FloatType,
    BooleanType, DateType, TimestampType, DecimalType
)

class SparkType(Enum):
    STRING = "StringType"
    INTEGER = "IntegerType"
    LONG = "LongType"
    DOUBLE = "DoubleType"
    FLOAT = "FloatType"
    BOOLEAN = "BooleanType"
    DATE = "DateType"
    TIMESTAMP = "TimestampType"
    DECIMAL = "DecimalType"

    def to_spark(self, type_str: str):
        if self == SparkType.DECIMAL:
            m = re.match(r"DecimalType\((\d+),\s*(\d+)\)", type_str)
            if not m:
                raise ValueError(f"Błędny DecimalType: {type_str}")
            return DecimalType(int(m.group(1)), int(m.group(2)))

        return {
            SparkType.STRING: StringType(),
            SparkType.INTEGER: IntegerType(),
            SparkType.LONG: LongType(),
            SparkType.DOUBLE: DoubleType(),
            SparkType.FLOAT: FloatType(),
            SparkType.BOOLEAN: BooleanType(),
            SparkType.DATE: DateType(),
            SparkType.TIMESTAMP: TimestampType(),
        }[self]
