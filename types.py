from __future__ import annotations

import importlib
import sys
import zipfile
import tempfile
from pathlib import Path


_ROOT = Path(__file__).resolve().parent
_SOURCE_DIR = _ROOT / "source"
_SPARK_SOURCE_DIR = _ROOT / "spark_source"

_SENT_TO_SPARK = set()


def _ensure_source_on_path() -> None:
    source = str(_SOURCE_DIR)
    if source not in sys.path:
        sys.path.insert(0, source)


def _zip_spark_package(package_name: str) -> str:
    package_dir = _SPARK_SOURCE_DIR / package_name

    if not package_dir.exists():
        raise ImportError(f"Brak spark_source/{package_name}")

    zip_path = Path(tempfile.gettempdir()) / f"mypack_{package_name}_spark.zip"

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for file in package_dir.rglob("*"):
            if file.is_file():
                arcname = file.relative_to(_SPARK_SOURCE_DIR)
                zf.write(file, arcname)

    return str(zip_path)


def _send_to_spark(package_name: str) -> None:
    if package_name in _SENT_TO_SPARK:
        return

    package_dir = _SPARK_SOURCE_DIR / package_name
    if not package_dir.exists():
        return

    try:
        from pyspark import SparkContext
    except ImportError:
        return

    sc = SparkContext._active_spark_context
    if sc is None:
        return

    zip_path = _zip_spark_package(package_name)
    sc.addPyFile(zip_path)

    _SENT_TO_SPARK.add(package_name)


def __getattr__(name: str):
    """
    Dzięki temu działa:

        from mypack import pack1

    bez pisania:

        import mypack.pack1
        mypack.load(...)
        mypack.register(...)
    """
    local_package = _SOURCE_DIR / name

    if not local_package.exists():
        raise AttributeError(f"module 'mypack' has no attribute {name!r}")

    _ensure_source_on_path()

    module = importlib.import_module(name)

    _send_to_spark(name)

    globals()[name] = module
    return module
