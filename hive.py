from __future__ import annotations

import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Mapping, Sequence
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class HivePartitionManager:
    """
    Zarządzanie partycjami istniejącej tabeli Hive/Spark.

    Obsługuje:
    - odczyt tabeli przez spark.table("db.table")
    - sprawdzanie istnienia partycji
    - dodawanie danych do partycji
    - nadpisywanie danych partycji
    - dodawanie pustej partycji do metastore
    - usuwanie partycji
    - odczyt konkretnej partycji

    Działa dla:
    - jednej kolumny partycjonującej
    - wielu kolumn partycjonujących
    """

    def __init__(
        self,
        spark: SparkSession,
        table_name: str,
        partition_cols: Sequence[str],
    ) -> None:
        if not table_name or not table_name.strip():
            raise ValueError("table_name nie może być pusty")

        if not partition_cols:
            raise ValueError(
                "partition_cols musi zawierać co najmniej jedną kolumnę"
            )

        if len(set(partition_cols)) != len(partition_cols):
            raise ValueError(
                f"partition_cols zawiera duplikaty: {partition_cols}"
            )

        self.spark = spark
        self.table_name = table_name
        self.partition_cols = list(partition_cols)

        # Ważne:
        # dostęp do tabeli odbywa się przez spark.table(...)
        table_df = self.spark.table(self.table_name)

        missing_partition_cols = [
            col_name
            for col_name in self.partition_cols
            if col_name not in table_df.columns
        ]

        if missing_partition_cols:
            raise ValueError(
                f"Kolumny partycjonujące nie istnieją w tabeli "
                f"{self.table_name}: {missing_partition_cols}"
            )

    # ============================================================
    # PUBLIC API
    # ============================================================

    def table(self) -> DataFrame:
        """
        Zwraca całą tabelę jako DataFrame.

        Dostęp dokładnie przez:
            spark.table("db.table")
        """
        return self.spark.table(self.table_name)

    def partition_exists(
        self,
        partition_spec: Mapping[str, Any],
    ) -> bool:
        """
        Sprawdza, czy konkretna partycja istnieje.

        Przykład jednej kolumny:
            {"dt": "2026-07-08"}

        Przykład wielu kolumn:
            {
                "year": 2026,
                "month": 7,
                "day": 8
            }
        """
        spec = self._validate_partition_spec(
            partition_spec,
            require_full=True,
        )

        partition_sql = self._build_partition_sql(spec)

        query = f"""
            SHOW PARTITIONS {self._quoted_table_name()}
            {partition_sql}
        """

        result = self.spark.sql(query)

        # Nie pobieramy wszystkich partycji.
        # Wystarczy informacja, czy istnieje pierwszy rekord.
        return bool(result.take(1))

    def append_to_partition(
        self,
        df: DataFrame,
        partition_spec: Mapping[str, Any],
    ) -> None:
        """
        Dodaje dane do konkretnej partycji.

        Jeżeli partycja nie istnieje, zostanie utworzona
        przez INSERT INTO.

        Jeżeli istnieje, dane zostaną dopisane.

        DataFrame może:
        - zawierać kolumny partycjonujące
        - nie zawierać kolumn partycjonujących

        Jeśli zawiera kolumny partycjonujące, ich wartości
        są walidowane względem partition_spec.
        """
        self._write_to_partition(
            df=df,
            partition_spec=partition_spec,
            overwrite=False,
        )

    def overwrite_partition(
        self,
        df: DataFrame,
        partition_spec: Mapping[str, Any],
    ) -> None:
        """
        Nadpisuje konkretną partycję.

        Używa:
            INSERT OVERWRITE TABLE ... PARTITION (...)
        """
        self._write_to_partition(
            df=df,
            partition_spec=partition_spec,
            overwrite=True,
        )

    def add_partition(
        self,
        partition_spec: Mapping[str, Any],
        location: str | None = None,
        if_not_exists: bool = True,
    ) -> None:
        """
        Dodaje partycję do Hive Metastore bez zapisywania danych.

        Opcjonalnie można podać LOCATION.

        Przykład:
            manager.add_partition(
                {"dt": "2026-07-08"}
            )

        albo:
            manager.add_partition(
                {"year": 2026, "month": 7},
                location="hdfs:///data/table/year=2026/month=7"
            )
        """
        spec = self._validate_partition_spec(
            partition_spec,
            require_full=True,
        )

        partition_sql = self._build_partition_sql(spec)

        if_not_exists_sql = (
            "IF NOT EXISTS"
            if if_not_exists
            else ""
        )

        location_sql = ""

        if location is not None:
            location_sql = (
                f" LOCATION {self._sql_literal(location)}"
            )

        query = f"""
            ALTER TABLE {self._quoted_table_name()}
            ADD {if_not_exists_sql}
            {partition_sql}
            {location_sql}
        """

        self.spark.sql(query)

    def drop_partition(
        self,
        partition_spec: Mapping[str, Any],
        if_exists: bool = True,
        purge: bool = False,
    ) -> bool:
        """
        Usuwa partycję.

        Zwraca:
            True  - partycja istniała przed usunięciem
            False - partycja nie istniała

        purge=True dodaje PURGE do ALTER TABLE.
        Używaj ostrożnie.
        """
        spec = self._validate_partition_spec(
            partition_spec,
            require_full=True,
        )

        existed = self.partition_exists(spec)

        partition_sql = self._build_partition_sql(spec)

        if_exists_sql = (
            "IF EXISTS"
            if if_exists
            else ""
        )

        purge_sql = (
            "PURGE"
            if purge
            else ""
        )

        query = f"""
            ALTER TABLE {self._quoted_table_name()}
            DROP {if_exists_sql}
            {partition_sql}
            {purge_sql}
        """

        self.spark.sql(query)

        return existed

    def read_partition(
        self,
        partition_spec: Mapping[str, Any],
    ) -> DataFrame:
        """
        Odczytuje konkretną partycję jako DataFrame.

        Tabela jest pobierana przez:
            spark.table(self.table_name)

        Następnie nakładany jest filtr po kolumnach partycji.
        """
        spec = self._validate_partition_spec(
            partition_spec,
            require_full=True,
        )

        df = self.spark.table(self.table_name)

        condition = None

        for col_name, value in spec.items():
            current_condition = (
                F.col(self._quoted_identifier(col_name))
                .eqNullSafe(F.lit(value))
            )

            condition = (
                current_condition
                if condition is None
                else condition & current_condition
            )

        return df.filter(condition)

    # ============================================================
    # INTERNAL WRITE
    # ============================================================

    def _write_to_partition(
        self,
        df: DataFrame,
        partition_spec: Mapping[str, Any],
        overwrite: bool,
    ) -> None:
        """
        Wspólna implementacja INSERT INTO / INSERT OVERWRITE.
        """
        spec = self._validate_partition_spec(
            partition_spec,
            require_full=True,
        )

        # Pobranie schematu/kolumn tabeli dokładnie przez spark.table(...)
        target_df = self.spark.table(self.table_name)
        target_columns = target_df.columns

        # Kolumny danych = wszystkie poza partycjonującymi
        data_columns = [
            col_name
            for col_name in target_columns
            if col_name not in self.partition_cols
        ]

        self._validate_input_dataframe(
            df=df,
            target_columns=target_columns,
            data_columns=data_columns,
            partition_spec=spec,
        )

        # Wybieramy wyłącznie kolumny niepartycjonujące
        # i dokładnie w kolejności tabeli docelowej.
        source_df = df.select(
            *[
                F.col(self._quoted_identifier(col_name))
                for col_name in data_columns
            ]
        )

        temp_view_name = (
            f"_hive_partition_manager_{uuid4().hex}"
        )

        try:
            source_df.createOrReplaceTempView(temp_view_name)

            operation = (
                "OVERWRITE"
                if overwrite
                else "INTO"
            )

            partition_sql = self._build_partition_sql(spec)

            select_columns_sql = ", ".join(
                self._quoted_identifier(col_name)
                for col_name in data_columns
            )

            query = f"""
                INSERT {operation}
                TABLE {self._quoted_table_name()}
                {partition_sql}
                SELECT {select_columns_sql}
                FROM {self._quoted_identifier(temp_view_name)}
            """

            self.spark.sql(query)

        finally:
            # Sprzątamy widok również w razie błędu INSERT-a.
            self.spark.catalog.dropTempView(temp_view_name)

    # ============================================================
    # VALIDATION
    # ============================================================

    def _validate_input_dataframe(
        self,
        df: DataFrame,
        target_columns: Sequence[str],
        data_columns: Sequence[str],
        partition_spec: Mapping[str, Any],
    ) -> None:
        """
        Sprawdza zgodność wejściowego DataFrame z tabelą.
        """

        # 1. Sprawdzenie brakujących kolumn danych
        missing_columns = [
            col_name
            for col_name in data_columns
            if col_name not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"DataFrame nie zawiera wymaganych kolumn tabeli "
                f"{self.table_name}: {missing_columns}"
            )

        # 2. Sprawdzenie nieznanych kolumn
        extra_columns = [
            col_name
            for col_name in df.columns
            if col_name not in target_columns
        ]

        if extra_columns:
            raise ValueError(
                f"DataFrame zawiera kolumny, których nie ma w tabeli "
                f"{self.table_name}: {extra_columns}"
            )

        # 3. Jeśli DataFrame posiada kolumny partycjonujące,
        #    sprawdzamy, czy zgadzają się z partition_spec.
        mismatch_condition = None

        for col_name, expected_value in partition_spec.items():
            if col_name not in df.columns:
                continue

            current_mismatch = ~(
                F.col(self._quoted_identifier(col_name))
                .eqNullSafe(F.lit(expected_value))
            )

            mismatch_condition = (
                current_mismatch
                if mismatch_condition is None
                else mismatch_condition | current_mismatch
            )

        if mismatch_condition is not None:
            has_mismatch = bool(
                df.filter(mismatch_condition)
                .limit(1)
                .take(1)
            )

            if has_mismatch:
                raise ValueError(
                    f"DataFrame zawiera wartości kolumn partycjonujących "
                    f"niezgodne z partition_spec={dict(partition_spec)}"
                )

    def _validate_partition_spec(
        self,
        partition_spec: Mapping[str, Any],
        require_full: bool,
    ) -> Dict[str, Any]:
        """
        Waliduje specyfikację partycji i ustawia stabilną kolejność
        zgodną z partition_cols.
        """
        if not partition_spec:
            raise ValueError(
                "partition_spec nie może być pusty"
            )

        unknown_columns = set(partition_spec) - set(self.partition_cols)

        if unknown_columns:
            raise ValueError(
                f"Nieznane kolumny partycjonujące: "
                f"{sorted(unknown_columns)}. "
                f"Oczekiwane: {self.partition_cols}"
            )

        if require_full:
            missing_columns = (
                set(self.partition_cols)
                - set(partition_spec)
            )

            if missing_columns:
                raise ValueError(
                    f"Brakuje kolumn partycjonujących: "
                    f"{sorted(missing_columns)}. "
                    f"Wymagane: {self.partition_cols}"
                )

        # None odrzucamy celowo.
        # Obsługa NULL-owych partycji zależy od konfiguracji
        # i konwencji Hive (__HIVE_DEFAULT_PARTITION__).
        null_columns = [
            col_name
            for col_name, value in partition_spec.items()
            if value is None
        ]

        if null_columns:
            raise ValueError(
                f"Wartość None nie jest obsługiwana dla partycji: "
                f"{null_columns}"
            )

        # Stabilna kolejność zgodna z partition_cols
        return {
            col_name: partition_spec[col_name]
            for col_name in self.partition_cols
            if col_name in partition_spec
        }

    # ============================================================
    # SQL HELPERS
    # ============================================================

    def _build_partition_sql(
        self,
        partition_spec: Mapping[str, Any],
    ) -> str:
        """
        Buduje:
            PARTITION (`dt` = '2026-07-08')

        albo:
            PARTITION (
                `year` = 2026,
                `month` = 7,
                `day` = 8
            )
        """
        parts = [
            (
                f"{self._quoted_identifier(col_name)} "
                f"= {self._sql_literal(value)}"
            )
            for col_name, value in partition_spec.items()
        ]

        return f"PARTITION ({', '.join(parts)})"

    def _quoted_table_name(self) -> str:
        """
        db.table -> `db`.`table`
        catalog.db.table -> `catalog`.`db`.`table`
        """
        parts = self.table_name.split(".")

        if not all(parts):
            raise ValueError(
                f"Niepoprawna nazwa tabeli: {self.table_name}"
            )

        return ".".join(
            self._quoted_identifier(part)
            for part in parts
        )

    @staticmethod
    def _quoted_identifier(identifier: str) -> str:
        """
        Bezpieczne cytowanie identyfikatora SQL.
        """
        escaped = identifier.replace("`", "``")
        return f"`{escaped}`"

    @staticmethod
    def _sql_literal(value: Any) -> str:
        """
        Konwertuje wartość Pythona do literału Spark SQL.
        """
        if value is None:
            return "NULL"

        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"

        if isinstance(value, int):
            return str(value)

        if isinstance(value, Decimal):
            return str(value)

        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError(
                    f"Niedozwolona wartość float: {value}"
                )
            return repr(value)

        if isinstance(value, datetime):
            formatted = value.strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )
            return f"TIMESTAMP '{formatted}'"

        if isinstance(value, date):
            return f"DATE '{value.isoformat()}'"

        # Domyślnie traktujemy jako STRING
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"
