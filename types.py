from pyspark.sql.types import (
    StringType, IntegerType, LongType, ShortType, ByteType,
    FloatType, DoubleType, DecimalType,
    BooleanType, DateType, TimestampType,
    ArrayType, MapType, StructType
)

# Lista niedozwolonych/problemowych konwersji w PySpark/Hive
forbidden_casts = [
    # Complex -> primitive
    (ArrayType, StringType),
    (ArrayType, IntegerType),
    (ArrayType, DoubleType),
    (MapType, StringType),
    (MapType, IntegerType),
    (MapType, DoubleType),
    (StructType, StringType),
    (StructType, IntegerType),
    (StructType, DoubleType),

    # Primitive -> complex
    (StringType, ArrayType),
    (StringType, MapType),
    (StringType, StructType),
    (IntegerType, ArrayType),
    (IntegerType, MapType),
    (IntegerType, StructType),
    (DoubleType, ArrayType),
    (DoubleType, MapType),
    (DoubleType, StructType),

    # Boolean ↔ inne typy numeryczne i daty
    (BooleanType, IntegerType),
    (BooleanType, LongType),
    (BooleanType, DoubleType),
    (BooleanType, DecimalType),
    (BooleanType, DateType),
    (BooleanType, TimestampType),

    # Date / timestamp ↔ liczby
    (DateType, IntegerType),
    (DateType, LongType),
    (DateType, DoubleType),
    (TimestampType, IntegerType),
    (TimestampType, LongType),
    (TimestampType, DoubleType),

    # Decimal -> boolean
    (DecimalType, BooleanType),

    # String/Char/Varchar -> complex types
    (StringType, ArrayType),
    (StringType, MapType),
    (StringType, StructType),
]
