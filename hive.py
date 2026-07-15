import json
import os
import socket
import sys
import zipfile
from pathlib import Path

from pyspark import SparkFiles, TaskContext


def inspect_python_environment(location):
    spark_files_root = SparkFiles.getRootDirectory()

    # Pliki znajdujące się bezpośrednio w katalogu SparkFiles.
    spark_files_entries = []
    py_files_in_root = []
    py_files_in_archives = {}

    if os.path.isdir(spark_files_root):
        spark_files_entries = sorted(os.listdir(spark_files_root))

        py_files_in_root = sorted(
            str(path)
            for path in Path(spark_files_root).rglob("*.py")
        )

        # --py-files często przekazuje paczki jako ZIP/EGG.
        for entry in spark_files_entries:
            full_path = os.path.join(spark_files_root, entry)

            if os.path.isfile(full_path) and zipfile.is_zipfile(full_path):
                try:
                    with zipfile.ZipFile(full_path) as archive:
                        py_files_in_archives[entry] = sorted(
                            name
                            for name in archive.namelist()
                            if name.endswith(".py")
                        )
                except Exception as exc:
                    py_files_in_archives[entry] = [
                        f"<nie udało się odczytać: {exc}>"
                    ]

    task_context = TaskContext.get()

    return {
        "location": location,
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "executor_id": os.environ.get("SPARK_EXECUTOR_ID"),
        "cwd": os.getcwd(),
        "main_module": getattr(sys.modules.get("__main__"), "__file__", None),
        "spark_files_root": spark_files_root,
        "spark_files_entries": spark_files_entries,
        "py_files_in_spark_root": py_files_in_root,
        "py_files_in_archives": py_files_in_archives,
        "sys_path": list(sys.path),
        "task": None if task_context is None else {
            "stage_id": task_context.stageId(),
            "partition_id": task_context.partitionId(),
            "attempt_number": task_context.attemptNumber(),
        },
    }


# ---------------------------------------------------------
# DRIVER
# ---------------------------------------------------------

driver_info = inspect_python_environment("driver")

print("=== DRIVER ===")
print(json.dumps(driver_info, indent=2, ensure_ascii=False))


# ---------------------------------------------------------
# EXECUTORY
# ---------------------------------------------------------

def inspect_partition(_):
    # Ta część wykonuje się w Python workerze executora.
    yield inspect_python_environment("executor")


number_of_tasks = max(spark.sparkContext.defaultParallelism * 2, 20)

executor_results = (
    spark.sparkContext
    .parallelize(range(number_of_tasks), number_of_tasks)
    .mapPartitions(inspect_partition)
    .collect()
)

# Jeden executor może wykonać wiele tasków, więc usuwamy duplikaty.
unique_executors = {}

for result in executor_results:
    key = (
        result["executor_id"],
        result["hostname"],
        result["spark_files_root"],
    )
    unique_executors[key] = result

print("=== EXECUTORY / PYTHON WORKERY ===")

for result in unique_executors.values():
    print(json.dumps(result, indent=2, ensure_ascii=False))
