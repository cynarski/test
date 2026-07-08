from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from typing import Union, List, Tuple, Dict, Any


spark = SparkSession.builder.getOrCreate()


class HiveSpark:

    def __init__(
        self,
        table_name: str
    ) -> None:

        self.table_name = table_name

    # ==========================================================
    # SCHEMA
    # ==========================================================

    @property
    def _get_hive_table_schema(self) -> StructType:
        return spark.table(
            self.table_name
        ).schema

    # ==========================================================
    # PARTITION COLUMNS
    # ==========================================================

    @property
    def _get_partition_columns(self) -> List[str]:

        db, table_name = self.table_name.split(".", 1)

        return [
            col.name.lower()
            for col in spark.catalog.listColumns(
                table_name,
                db
            )
            if col.isPartition
        ]

    # ==========================================================
    # PROVIDER
    # ==========================================================

    @property
    def table_provider(self) -> str:

        row = (
            spark.sql(
                f"DESCRIBE FORMATTED {self.table_name}"
            )
            .where(
                "trim(col_name) = 'Provider:'"
            )
            .select("data_type")
            .first()
        )

        return (
            row["data_type"].lower()
            if row and row["data_type"]
            else None
        )

    # ==========================================================
    # TABLE EXISTS
    # ==========================================================

    def _check_table_exist_in_hive(self) -> bool:

        db, table_name = self.table_name.split(".", 1)

        return spark.catalog.tableExists(
            table_name,
            db
        )

    # ==========================================================
    # APPEND
    # ==========================================================

    def append_to_hive(
        self,
        df: DataFrame
    ) -> None:

        hive_schema = self._get_hive_table_schema

        assert df.schema == hive_schema, (
            f"Schemas are not the same: "
            f"expected {hive_schema}, "
            f"got {df.schema}"
        )

        partition_columns = (
            self._get_partition_columns
        )

        if partition_columns:

            missing_partitions = (
                set(partition_columns)
                - set(
                    col.lower()
                    for col in df.columns
                )
            )

            assert not missing_partitions, (
                f"Missing partition columns "
                f"in DataFrame: "
                f"{missing_partitions}"
            )

        table_provider = self.table_provider

        if not table_provider:
            raise ValueError(
                f"Nie można określić providera "
                f"dla tabeli {self.table_name}"
            )

        (
            df.write
            .partitionBy(
                partition_columns
                if partition_columns
                else []
            )
            .format(table_provider)
            .mode("append")
            .saveAsTable(self.table_name)
        )

    # ==========================================================
    # SQL LITERAL
    # ==========================================================

    @staticmethod
    def _sql_literal(value: Any) -> str:

        if value is None:
            raise ValueError(
                "None nie jest obsługiwane "
                "dla wartości partycji."
            )

        if isinstance(value, bool):
            return "true" if value else "false"

        if isinstance(value, (int, float)):
            return str(value)

        escaped = str(value).replace(
            "'",
            "''"
        )

        return f"'{escaped}'"

    # ==========================================================
    # NORMALIZE PARTITION
    # ==========================================================

    def _normalize_partition(
        self,
        partition: Union[
            str,
            List,
            Tuple,
            Dict
        ]
    ) -> Dict[str, Any]:

        partition_columns = (
            self._get_partition_columns
        )

        if not partition_columns:
            raise ValueError(
                f"Tabela {self.table_name} "
                f"nie jest partycjonowana."
            )

        # -----------------------------
        # DICT
        # -----------------------------
        if isinstance(partition, dict):

            normalized = {
                str(key).lower(): value
                for key, value
                in partition.items()
            }

        # -----------------------------
        # STR
        # -----------------------------
        elif isinstance(partition, str):

            if "=" in partition:

                col_name, value = (
                    partition.split("=", 1)
                )

                normalized = {
                    col_name.strip().lower():
                    value.strip()
                }

            else:

                if len(partition_columns) != 1:
                    raise ValueError(
                        f"Tabela ma wiele kolumn "
                        f"partycjonujących: "
                        f"{partition_columns}. "
                        f"Użyj dict."
                    )

                normalized = {
                    partition_columns[0]:
                    partition
                }

        # -----------------------------
        # LIST / TUPLE
        # -----------------------------
        elif isinstance(
            partition,
            (list, tuple)
        ):

            if not partition:
                raise ValueError(
                    "Lista partycji jest pusta."
                )

            # ["dt=2024-01-01", "hr=12"]
            if all(
                isinstance(item, str)
                and "=" in item
                for item in partition
            ):

                normalized = {}

                for item in partition:

                    col_name, value = (
                        item.split("=", 1)
                    )

                    col_name = (
                        col_name
                        .strip()
                        .lower()
                    )

                    normalized[col_name] = (
                        value.strip()
                    )

            # ["2024-01-01", "12"]
            else:

                if (
                    len(partition)
                    != len(partition_columns)
                ):
                    raise ValueError(
                        f"Oczekiwano "
                        f"{len(partition_columns)} "
                        f"wartości partycji, "
                        f"otrzymano "
                        f"{len(partition)}."
                    )

                normalized = dict(
                    zip(
                        partition_columns,
                        partition
                    )
                )

        else:
            raise ValueError(
                "partition musi być typu "
                "str, list, tuple lub dict."
            )

        # -----------------------------
        # VALIDATION
        # -----------------------------
        provided = set(normalized)
        expected = set(partition_columns)

        unknown = provided - expected

        if unknown:
            raise ValueError(
                f"Nieznane kolumny partycji: "
                f"{sorted(unknown)}"
            )

        missing = expected - provided

        if missing:
            raise ValueError(
                f"Brakujące kolumny partycji: "
                f"{sorted(missing)}"
            )

        # Kolejność zgodna z tabelą
        return {
            col: normalized[col]
            for col in partition_columns
        }

    # ==========================================================
    # BUILD PARTITION SQL
    # ==========================================================

    def _build_partition_sql(
        self,
        partition: Union[
            str,
            List,
            Tuple,
            Dict
        ]
    ) -> str:

        normalized = (
            self._normalize_partition(
                partition
            )
        )

        conditions = [
            (
                f"`{col}`="
                f"{self._sql_literal(value)}"
            )
            for col, value
            in normalized.items()
        ]

        return (
            f"PARTITION "
            f"({', '.join(conditions)})"
        )

    # ==========================================================
    # CHECK PARTITION
    # ==========================================================

    def check_partitions_in_hive(
        self,
        partition: Union[
            str,
            List,
            Tuple,
            Dict
        ]
    ) -> bool:

        partition_sql = (
            self._build_partition_sql(
                partition
            )
        )

        try:
            result = spark.sql(
                f"""
                SHOW PARTITIONS
                {self.table_name}
                {partition_sql}
                """
            )

            return bool(
                result.limit(1).take(1)
            )

        except Exception as e:
            raise Exception(
                f"Błąd podczas sprawdzania "
                f"partycji {partition} "
                f"w tabeli "
                f"{self.table_name} -> {e}"
            ) from e

    # ==========================================================
    # DELETE PARTITION
    # ==========================================================

    def delete_partition_in_hive(
        self,
        partition: Union[
            str,
            List,
            Tuple,
            Dict
        ]
    ) -> None:

        partition_sql = (
            self._build_partition_sql(
                partition
            )
        )

        sql_query = (
            f"ALTER TABLE "
            f"{self.table_name} "
            f"DROP IF EXISTS "
            f"{partition_sql}"
        )

        try:
            spark.sql(sql_query)

        except Exception as e:
            raise Exception(
                f"Błąd podczas usuwania "
                f"partycji {partition} "
                f"z tabeli "
                f"{self.table_name} -> {e}"
            ) from e

    # ==========================================================
    # TRUNCATE
    # ==========================================================

    def delete_all_data_in_hive(
        self
    ) -> None:

        try:
            spark.sql(
                f"TRUNCATE TABLE "
                f"{self.table_name}"
            )

        except Exception as e:
            raise Exception(
                f"Error during truncate table "
                f"{self.table_name} -> {e}"
            ) from e

    # ==========================================================
    # CREATE EMPTY TABLE
    # ==========================================================

    def create_empty_table(
        self,
        schema,
        file_format,
        partitions=None
    ) -> None:

        if not self._check_table_exist_in_hive():

            df = spark.createDataFrame(
                [],
                schema
            )

            (
                df.write
                .partitionBy(
                    partitions
                    if partitions
                    else []
                )
                .format(file_format)
                .mode("overwrite")
                .saveAsTable(
                    self.table_name
                )
            )

        else:
            raise Exception(
                f"Table {self.table_name} "
                f"already exist"
            )
