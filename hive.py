from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql import functions as F

from typing import (
    Any,
    Dict,
    List,
    Tuple,
    Union,
    Optional
)

from urllib.parse import unquote
import unicodedata


class HiveSpark:
    """
    Klasa do obsługi tabel Hive/Spark.

    Konstruktor przyjmuje tylko nazwę tabeli:

        hive = HiveSpark("db.table_name")

    Obsługuje:
    - odczyt schematu tabeli,
    - automatyczne wykrywanie kolumn partycjonujących,
    - sprawdzanie istnienia tabeli,
    - append danych,
    - sprawdzanie istnienia partycji,
    - usuwanie jednej partycji,
    - usuwanie wszystkich danych,
    - tworzenie pustej tabeli.

    Obsługuje tabele z:
    - jedną kolumną partycjonującą,
    - wieloma kolumnami partycjonującymi.
    """

    def __init__(
        self,
        table_name: str
    ) -> None:

        if not table_name:
            raise ValueError(
                "table_name nie może być pusty."
            )

        self.table_name = table_name.strip()

        # Konstruktor nadal przyjmuje tylko table_name.
        # SparkSession pobierana jest automatycznie.
        self.spark = (
            SparkSession
            .builder
            .getOrCreate()
        )

    # =========================================================
    # TABLE NAME HELPERS
    # =========================================================

    def _split_table_name(
        self
    ) -> Tuple[Optional[str], str]:
        """
        Obsługuje:

            table_name

        oraz:

            db.table_name

        Zwraca:

            (None, "table_name")

        albo:

            ("db", "table_name")
        """

        parts = self.table_name.split(".")

        if len(parts) == 1:
            return None, parts[0]

        if len(parts) == 2:
            return parts[0], parts[1]

        raise ValueError(
            f"Nieobsługiwany format nazwy tabeli: "
            f"{self.table_name}. "
            f"Oczekiwano 'table' albo 'db.table'."
        )

    @staticmethod
    def _quote_identifier(
        identifier: str
    ) -> str:
        """
        Bezpiecznie cytuje identyfikator SQL.

        date
        ->
        `date`
        """

        escaped = identifier.replace(
            "`",
            "``"
        )

        return f"`{escaped}`"

    def _quoted_table_name(
        self
    ) -> str:
        """
        db.table
        ->
        `db`.`table`
        """

        return ".".join(
            self._quote_identifier(part)
            for part in self.table_name.split(".")
        )

    # =========================================================
    # TABLE
    # =========================================================

    @property
    def table(self) -> DataFrame:
        """
        Zwraca tabelę jako DataFrame.

        Dostęp dokładnie przez:
            spark.table(...)
        """

        return self.spark.table(
            self.table_name
        )

    # =========================================================
    # HIVE TABLE SCHEMA
    # =========================================================

    @property
    def _get_hive_table_schema(
        self
    ) -> StructType:
        """
        Pobiera schemat tabeli Hive/Spark.
        """

        return self.spark.table(
            self.table_name
        ).schema

    # =========================================================
    # PARTITION COLUMNS
    # =========================================================

    @property
    def _get_partition_columns(
        self
    ) -> List[str]:
        """
        Automatycznie wykrywa kolumny partycjonujące.

        Zachowuje rzeczywiste nazwy kolumn z katalogu.
        """

        db_name, table_name = (
            self._split_table_name()
        )

        if db_name is None:
            columns = (
                self.spark.catalog
                .listColumns(table_name)
            )
        else:
            columns = (
                self.spark.catalog
                .listColumns(
                    table_name,
                    db_name
                )
            )

        return [
            col.name
            for col in columns
            if col.isPartition
        ]

    # =========================================================
    # TABLE PROVIDER
    # =========================================================

    @property
    def table_provider(
        self
    ) -> Optional[str]:
        """
        Pobiera providera tabeli,
        np. parquet, hive, orc.
        """

        row = (
            self.spark.sql(
                f"""
                DESCRIBE FORMATTED
                {self._quoted_table_name()}
                """
            )
            .where(
                F.trim(F.col("col_name"))
                == F.lit("Provider:")
            )
            .select("data_type")
            .first()
        )

        if (
            row is None
            or row["data_type"] is None
        ):
            return None

        return (
            row["data_type"]
            .strip()
            .lower()
        )

    # =========================================================
    # CHECK TABLE EXISTS
    # =========================================================

    def _check_table_exist_in_hive(
        self
    ) -> bool:
        """
        Sprawdza, czy tabela istnieje.
        """

        db_name, table_name = (
            self._split_table_name()
        )

        if db_name is None:
            return (
                self.spark.catalog
                .tableExists(table_name)
            )

        return (
            self.spark.catalog
            .tableExists(
                table_name,
                db_name
            )
        )

    # =========================================================
    # APPEND TO HIVE
    # =========================================================

    def append_to_hive(
        self,
        df: DataFrame
    ) -> None:
        """
        Dodaje dane do istniejącej tabeli Hive.

        DataFrame musi mieć schemat zgodny
        ze schematem tabeli.
        """

        hive_schema = (
            self._get_hive_table_schema
        )

        if df.schema != hive_schema:
            raise ValueError(
                "Schemas are not the same.\n"
                f"Expected:\n{hive_schema}\n"
                f"Got:\n{df.schema}"
            )

        partition_columns = (
            self._get_partition_columns
        )

        # Sprawdzenie kolumn partycjonujących
        if partition_columns:

            df_columns_lookup = {
                col.casefold()
                for col in df.columns
            }

            missing_partitions = {
                col
                for col in partition_columns
                if col.casefold()
                not in df_columns_lookup
            }

            if missing_partitions:
                raise ValueError(
                    f"Missing partition columns "
                    f"in DataFrame: "
                    f"{sorted(missing_partitions)}"
                )

        table_provider = (
            self.table_provider
        )

        if not table_provider:
            raise ValueError(
                f"Nie można określić providera "
                f"dla tabeli "
                f"{self.table_name}."
            )

        writer = (
            df.write
            .format(table_provider)
            .mode("append")
        )

        # partitionBy tylko wtedy,
        # kiedy tabela faktycznie jest partycjonowana
        if partition_columns:
            writer = writer.partitionBy(
                *partition_columns
            )

        writer.saveAsTable(
            self.table_name
        )

    # =========================================================
    # NORMALIZE PARTITION VALUE
    # =========================================================

    @staticmethod
    def _normalize_partition_value(
        value: Any
    ) -> str:
        """
        Normalizuje wartość partycji.

        Przykłady:

            "'kraków'"
            ->
            "kraków"

            "krak%C3%B3w"
            ->
            "kraków"

        Dodatkowo normalizuje Unicode do NFC.

        UWAGA:
        Nie wykonujemy lower() na wartościach partycji.
        """

        if value is None:
            raise ValueError(
                "Wartość partycji nie może być None."
            )

        value = str(value).strip()

        # Usunięcie otaczających apostrofów:
        #
        # 'kraków'
        # ->
        # kraków
        if (
            len(value) >= 2
            and value[0] == "'"
            and value[-1] == "'"
        ):
            value = value[1:-1]

        # Usunięcie otaczających cudzysłowów:
        #
        # "kraków"
        # ->
        # kraków
        elif (
            len(value) >= 2
            and value[0] == '"'
            and value[-1] == '"'
        ):
            value = value[1:-1]

        # Dekodowanie:
        #
        # krak%C3%B3w
        # ->
        # kraków
        value = unquote(value)

        # Stabilna reprezentacja Unicode
        value = unicodedata.normalize(
            "NFC",
            value
        )

        return value

    # =========================================================
    # RESOLVE PARTITION COLUMN
    # =========================================================

    def _resolve_partition_column(
        self,
        column_name: str
    ) -> str:
        """
        Dopasowuje nazwę kolumny wejściowej
        do rzeczywistej nazwy kolumny partycji.

        Porównanie nazw kolumn jest
        case-insensitive.

        Przykład:

            DATE
            ->
            date
        """

        partition_columns = (
            self._get_partition_columns
        )

        lookup = {
            col.casefold(): col
            for col in partition_columns
        }

        normalized_name = (
            str(column_name)
            .strip()
            .casefold()
        )

        if normalized_name not in lookup:
            raise ValueError(
                f"Nieznana kolumna partycjonująca "
                f"'{column_name}'. "
                f"Oczekiwane: "
                f"{partition_columns}"
            )

        return lookup[normalized_name]

    # =========================================================
    # NORMALIZE CONDITIONS
    # =========================================================

    def _normalize_partition_conditions(
        self,
        conditions: Union[List, Tuple]
    ) -> Dict[str, str]:
        """
        Zamienia:

            [
                "date=2026-07-01",
                "region=kraków",
                "kategoria=a"
            ]

        na:

            {
                "date": "2026-07-01",
                "region": "kraków",
                "kategoria": "a"
            }
        """

        normalized = {}

        for condition in conditions:

            if not isinstance(condition, str):
                raise ValueError(
                    f"Warunek partycji musi być stringiem. "
                    f"Otrzymano: {condition!r}"
                )

            if "=" not in condition:
                raise ValueError(
                    f"Nieprawidłowy format warunku "
                    f"partycji: {condition!r}. "
                    f"Oczekiwano 'kolumna=wartość'."
                )

            col_name, value = (
                condition.split("=", 1)
            )

            actual_col_name = (
                self._resolve_partition_column(
                    col_name
                )
            )

            if actual_col_name in normalized:
                raise ValueError(
                    f"Kolumna partycjonująca "
                    f"'{actual_col_name}' "
                    f"została podana więcej niż raz."
                )

            normalized[actual_col_name] = (
                self._normalize_partition_value(
                    value
                )
            )

        return normalized

    # =========================================================
    # NORMALIZE PARTITION
    # =========================================================

    def _normalize_partition(
        self,
        partition: Union[
            str,
            List,
            Tuple,
            Dict
        ]
    ) -> Dict[str, str]:
        """
        Normalizuje różne formaty wejścia
        do jednego słownika.

        -------------------------------------------------------
        JEDNA KOLUMNA PARTYCJONUJĄCA
        -------------------------------------------------------

        "2026-07-01"

        "date=2026-07-01"

        ["date=2026-07-01"]

        {"date": "2026-07-01"}

        -------------------------------------------------------
        WIELE KOLUMN PARTYCJONUJĄCYCH
        -------------------------------------------------------

        [
            "date=2026-07-01",
            "region=kraków",
            "kategoria=a"
        ]

        [
            "2026-07-01",
            "kraków",
            "a"
        ]

        {
            "date": "2026-07-01",
            "region": "kraków",
            "kategoria": "a"
        }

        Obsługiwany jest też pełny format SHOW PARTITIONS:

        "date=2026-07-01/region=kraków/kategoria=a"
        """

        partition_columns = (
            self._get_partition_columns
        )

        if not partition_columns:
            raise ValueError(
                f"Tabela {self.table_name} "
                f"nie jest partycjonowana."
            )

        normalized = {}

        # =====================================================
        # DICT
        # =====================================================

        if isinstance(partition, dict):

            for col_name, value in partition.items():

                actual_col_name = (
                    self._resolve_partition_column(
                        str(col_name)
                    )
                )

                if actual_col_name in normalized:
                    raise ValueError(
                        f"Kolumna '{actual_col_name}' "
                        f"została podana więcej niż raz."
                    )

                normalized[actual_col_name] = (
                    self._normalize_partition_value(
                        value
                    )
                )

        # =====================================================
        # STRING
        # =====================================================

        elif isinstance(partition, str):

            partition = partition.strip()

            if not partition:
                raise ValueError(
                    "partition nie może być pustym stringiem."
                )

            # Pełny format:
            #
            # date=.../region=.../kategoria=...
            if (
                "/" in partition
                and "=" in partition
            ):

                conditions = partition.split("/")

                normalized = (
                    self._normalize_partition_conditions(
                        conditions
                    )
                )

            # Jedna kondycja:
            #
            # date=2026-07-01
            elif "=" in partition:

                normalized = (
                    self._normalize_partition_conditions(
                        [partition]
                    )
                )

            # Sama wartość:
            #
            # 2026-07-01
            else:

                if len(partition_columns) != 1:
                    raise ValueError(
                        f"Tabela {self.table_name} ma "
                        f"{len(partition_columns)} kolumny "
                        f"partycjonujące: "
                        f"{partition_columns}. "
                        f"Nie można jednoznacznie przypisać "
                        f"wartości {partition!r}."
                    )

                normalized = {
                    partition_columns[0]:
                    self._normalize_partition_value(
                        partition
                    )
                }

        # =====================================================
        # LIST / TUPLE
        # =====================================================

        elif isinstance(
            partition,
            (list, tuple)
        ):

            if not partition:
                raise ValueError(
                    "Lista/tuple partycji "
                    "nie może być pusta."
                )

            contains_equal = [
                (
                    isinstance(item, str)
                    and "=" in item
                )
                for item in partition
            ]

            # Wszystkie elementy:
            #
            # [
            #   "date=2026-07-01",
            #   "region=kraków",
            #   "kategoria=a"
            # ]
            if all(contains_equal):

                normalized = (
                    self._normalize_partition_conditions(
                        partition
                    )
                )

            # Mieszany format:
            #
            # [
            #   "date=2026-07-01",
            #   "kraków"
            # ]
            #
            # -> błąd
            elif any(contains_equal):

                raise ValueError(
                    "Nie można mieszać formatów partycji. "
                    "Użyj albo:\n"
                    "['date=2026-07-01', "
                    "'region=kraków']\n"
                    "albo:\n"
                    "['2026-07-01', 'kraków']"
                )

            # Same wartości:
            #
            # [
            #   "2026-07-01",
            #   "kraków",
            #   "a"
            # ]
            else:

                if (
                    len(partition)
                    != len(partition_columns)
                ):
                    raise ValueError(
                        f"Nieprawidłowa liczba wartości "
                        f"partycji. "
                        f"Oczekiwano "
                        f"{len(partition_columns)} "
                        f"dla kolumn "
                        f"{partition_columns}, "
                        f"otrzymano "
                        f"{len(partition)}."
                    )

                normalized = {
                    col_name:
                    self._normalize_partition_value(value)

                    for col_name, value
                    in zip(
                        partition_columns,
                        partition
                    )
                }

        else:
            raise ValueError(
                "Parametr partition musi być typu "
                "str, list, tuple lub dict."
            )

        # =====================================================
        # VALIDATION
        # =====================================================

        expected_columns = set(
            partition_columns
        )

        provided_columns = set(
            normalized.keys()
        )

        missing_columns = (
            expected_columns
            - provided_columns
        )

        if missing_columns:
            raise ValueError(
                f"Brakujące kolumny partycjonujące: "
                f"{sorted(missing_columns)}. "
                f"Wymagane: "
                f"{partition_columns}"
            )

        unknown_columns = (
            provided_columns
            - expected_columns
        )

        if unknown_columns:
            raise ValueError(
                f"Nieznane kolumny partycjonujące: "
                f"{sorted(unknown_columns)}."
            )

        # Bardzo ważne:
        # zwracamy w kolejności kolumn partycji
        # zdefiniowanej w tabeli.
        return {
            col_name: normalized[col_name]
            for col_name in partition_columns
        }

    # =========================================================
    # PARSE SHOW PARTITIONS RESULT
    # =========================================================

    def _parse_hive_partition(
        self,
        partition_string: str
    ) -> Dict[str, str]:
        """
        Parsuje wartość zwróconą przez:

            SHOW PARTITIONS db.table

        Przykład wejścia:

            date=2026-07-01/region=kraków/kategoria=a

        Wynik:

            {
                "date": "2026-07-01",
                "region": "kraków",
                "kategoria": "a"
            }

        Dzięki temu nie porównujemy surowych stringów.
        """

        if partition_string is None:
            raise ValueError(
                "Wartość partycji z SHOW PARTITIONS "
                "jest None."
            )

        partition_string = str(
            partition_string
        ).strip()

        if not partition_string:
            raise ValueError(
                "Pusta wartość zwrócona "
                "przez SHOW PARTITIONS."
            )

        result = {}

        # Ważne:
        # split("/") wykonujemy PRZED unquote().
        #
        # Jeśli wartość zawiera zakodowany slash %2F,
        # nie zepsujemy struktury partycji.
        conditions = (
            partition_string.split("/")
        )

        for condition in conditions:

            if "=" not in condition:
                raise ValueError(
                    f"Nieprawidłowy format partycji "
                    f"z SHOW PARTITIONS: "
                    f"{partition_string!r}. "
                    f"Błędny fragment: "
                    f"{condition!r}"
                )

            col_name, value = (
                condition.split("=", 1)
            )

            actual_col_name = (
                self._resolve_partition_column(
                    col_name
                )
            )

            if actual_col_name in result:
                raise ValueError(
                    f"Kolumna '{actual_col_name}' "
                    f"występuje więcej niż raz "
                    f"w partycji: "
                    f"{partition_string!r}"
                )

            result[actual_col_name] = (
                self._normalize_partition_value(
                    value
                )
            )

        # Kolejność zgodna z tabelą.
        partition_columns = (
            self._get_partition_columns
        )

        return {
            col_name: result[col_name]
            for col_name in partition_columns
            if col_name in result
        }

    # =========================================================
    # CHECK PARTITIONS IN HIVE
    # =========================================================

    def check_partitions_in_hive(
        self,
        partition: Union[
            str,
            List,
            Tuple,
            Dict
        ]
    ) -> bool:
        """
        Sprawdza, czy konkretna partycja istnieje.

        WAŻNE:
        Nie używa:

            SHOW PARTITIONS table
            PARTITION (...)

        ponieważ Twoje środowisko tego nie obsługuje.

        Wykonuje wyłącznie:

            SHOW PARTITIONS table

        Następnie:
        1. pobiera wynikową ramkę,
        2. parsuje każdą partycję,
        3. porównuje jako dict.

        -------------------------------------------------------
        PRZYKŁAD
        -------------------------------------------------------

        Hive zwraca:

            date=2026-07-01/region=kraków/kategoria=a

        Wywołanie:

            check_partitions_in_hive(
                [
                    "date=2026-07-01",
                    "region=kraków",
                    "kategoria=a"
                ]
            )

        zwróci:

            True
        """

        partition_columns = (
            self._get_partition_columns
        )

        if not partition_columns:
            raise ValueError(
                f"Tabela {self.table_name} "
                f"nie jest partycjonowana."
            )

        try:
            # Normalizacja oczekiwanej partycji:
            #
            # {
            #   "date": "2026-07-01",
            #   "region": "kraków",
            #   "kategoria": "a"
            # }
            expected_partition = (
                self._normalize_partition(
                    partition
                )
            )

            # Tylko pełne SHOW PARTITIONS.
            partitions_df = self.spark.sql(
                f"""
                SHOW PARTITIONS
                {self._quoted_table_name()}
                """
            )

            if not partitions_df.columns:
                return False

            # Zwykle kolumna nazywa się "partition",
            # ale dla kompatybilności bierzemy pierwszą
            # kolumnę wyniku.
            result_column = (
                partitions_df.columns[0]
            )

            # Iterujemy po ramce wynikowej.
            # Nie robimy collect() całej listy naraz.
            for row in (
                partitions_df
                .select(result_column)
                .toLocalIterator()
            ):

                raw_partition = row[0]

                existing_partition = (
                    self._parse_hive_partition(
                        raw_partition
                    )
                )

                # Porównujemy DICT,
                # a nie surowe stringi.
                if (
                    existing_partition
                    == expected_partition
                ):
                    return True

            return False

        except Exception as e:
            raise Exception(
                f"Błąd podczas sprawdzania "
                f"partycji {partition} "
                f"w tabeli "
                f"{self.table_name} -> {e}"
            ) from e

    # =========================================================
    # SQL LITERAL
    # =========================================================

    @staticmethod
    def _sql_literal(
        value: Any
    ) -> str:
        """
        Buduje bezpieczny literał SQL.

        kraków
        ->
        'kraków'

        O'Brien
        ->
        'O''Brien'
        """

        if value is None:
            raise ValueError(
                "Wartość SQL nie może być None."
            )

        value = str(value)

        # Backslash
        value = value.replace(
            "\\",
            "\\\\"
        )

        # Apostrof
        value = value.replace(
            "'",
            "''"
        )

        return f"'{value}'"

    # =========================================================
    # BUILD PARTITION SQL
    # =========================================================

    def _build_partition_sql(
        self,
        partition: Union[
            str,
            List,
            Tuple,
            Dict
        ]
    ) -> str:
        """
        Buduje SQL do DROP PARTITION.

        Jedna kolumna:

            PARTITION (
                `date`='2026-07-01'
            )

        Wiele kolumn:

            PARTITION (
                `date`='2026-07-01',
                `region`='kraków',
                `kategoria`='a'
            )
        """

        normalized = (
            self._normalize_partition(
                partition
            )
        )

        conditions = [
            (
                f"{self._quote_identifier(col_name)}"
                f"="
                f"{self._sql_literal(value)}"
            )
            for col_name, value
            in normalized.items()
        ]

        return (
            f"PARTITION "
            f"({', '.join(conditions)})"
        )

    # =========================================================
    # DELETE PARTITION IN HIVE
    # =========================================================

    def delete_partition_in_hive(
        self,
        partition: Union[
            str,
            List,
            Tuple,
            Dict
        ]
    ) -> None:
        """
        Usuwa konkretną partycję.

        -------------------------------------------------------
        JEDNA KOLUMNA
        -------------------------------------------------------

        delete_partition_in_hive(
            "2026-07-01"
        )

        delete_partition_in_hive(
            {"date": "2026-07-01"}
        )

        -------------------------------------------------------
        WIELE KOLUMN
        -------------------------------------------------------

        delete_partition_in_hive(
            [
                "date=2026-07-01",
                "region=kraków",
                "kategoria=a"
            ]
        )

        albo:

        delete_partition_in_hive(
            {
                "date": "2026-07-01",
                "region": "kraków",
                "kategoria": "a"
            }
        )
        """

        partition_columns = (
            self._get_partition_columns
        )

        if not partition_columns:
            raise ValueError(
                f"Tabela {self.table_name} "
                f"nie jest partycjonowana."
            )

        try:
            partition_sql = (
                self._build_partition_sql(
                    partition
                )
            )

            query = (
                f"ALTER TABLE "
                f"{self._quoted_table_name()} "
                f"DROP IF EXISTS "
                f"{partition_sql}"
            )

            self.spark.sql(query)

        except Exception as e:
            raise Exception(
                f"Błąd podczas usuwania "
                f"partycji {partition} "
                f"z tabeli "
                f"{self.table_name} -> {e}"
            ) from e

    # =========================================================
    # DELETE ALL DATA
    # =========================================================

    def delete_all_data_in_hive(
        self
    ) -> None:
        """
        Usuwa wszystkie dane z tabeli.
        """

        try:
            self.spark.sql(
                f"""
                TRUNCATE TABLE
                {self._quoted_table_name()}
                """
            )

        except Exception as e:
            raise Exception(
                f"Error during truncate table "
                f"{self.table_name} -> {e}"
            ) from e

    # =========================================================
    # CREATE EMPTY TABLE
    # =========================================================

    def create_empty_table(
        self,
        schema: StructType,
        file_format: str,
        partitions: Optional[
            Union[
                str,
                List[str],
                Tuple[str, ...]
            ]
        ] = None
    ) -> None:
        """
        Tworzy pustą tabelę.

        Przykład bez partycji:

            create_empty_table(
                schema=schema,
                file_format="parquet"
            )

        Jedna partycja:

            create_empty_table(
                schema=schema,
                file_format="parquet",
                partitions="date"
            )

        Wiele partycji:

            create_empty_table(
                schema=schema,
                file_format="parquet",
                partitions=[
                    "date",
                    "region",
                    "kategoria"
                ]
            )
        """

        if self._check_table_exist_in_hive():
            raise Exception(
                f"Table {self.table_name} "
                f"already exists."
            )

        df = self.spark.createDataFrame(
            [],
            schema
        )

        writer = (
            df.write
            .format(file_format)
            .mode("overwrite")
        )

        # Normalizacja partitions
        if isinstance(partitions, str):
            partition_columns = [
                partitions
            ]

        elif isinstance(
            partitions,
            (list, tuple)
        ):
            partition_columns = list(
                partitions
            )

        elif partitions is None:
            partition_columns = []

        else:
            raise ValueError(
                "partitions musi być typu "
                "str, list, tuple albo None."
            )

        if partition_columns:
            writer = writer.partitionBy(
                *partition_columns
            )

        writer.saveAsTable(
            self.table_name
        )
