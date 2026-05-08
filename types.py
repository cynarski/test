from pathlib import Path


_ALREADY_SENT = set()


def send_matching_spark_dir(source_package_file: str) -> None:
    """
    Mapuje katalog z my_lib/source/... na odpowiadający katalog
    my_lib/source_spark/... i wysyła go do Sparka.

    Przykład:
        my_lib/source/dir1/__init__.py
        ->
        my_lib/source_spark/dir1
    """

    source_dir = Path(source_package_file).resolve().parent

    # Dla:
    # my_lib/source/dir1/__init__.py
    #
    # source_dir = my_lib/source/dir1
    # source_dir.parents[0] = my_lib/source
    # source_dir.parents[1] = my_lib
    package_root = source_dir.parents[1]

    source_root = package_root / "source"
    spark_root = package_root / "source_spark"

    relative_path = source_dir.relative_to(source_root)

    spark_dir = spark_root / relative_path

    if not spark_dir.exists():
        return

    if spark_dir in _ALREADY_SENT:
        return

    send_to_spark(spark_dir)

    _ALREADY_SENT.add(spark_dir)


def send_to_spark(path: Path) -> None:
    """
    Tutaj wstaw swoją obecną logikę wysyłania plików do Sparka.

    Jeśli już masz funkcję, która wysyła katalog albo pliki do Sparka,
    możesz ją tutaj wywołać.
    """

    # PRZYKŁAD:
    print(f"Wysyłam do Sparka: {path}")

    # Tutaj podmień na swoją logikę, np.:
    #
    # for file in path.rglob("*.py"):
    #     spark.sparkContext.addPyFile(str(file))
    #
    # albo:
    #
    # upload_directory_to_spark(path)


from my_lib.spark_upload import send_matching_spark_dir

send_matching_spark_dir(__file__)


