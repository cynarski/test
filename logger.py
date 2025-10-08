import logging
import sys
import threading
import time
from datetime import datetime
from io import StringIO
import subprocess
# jeśli masz pydoop, możesz użyć tego zamiast subprocess:
# from pydoop import hdfs


class HDFSLogger:
    LEVELS = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    def __init__(self, spark_session, hdfs_dir: str, level: str = "INFO", flush_interval: int = 5):
        """
        spark_session : obiekt SparkSession
        hdfs_dir      : katalog HDFS, np. 'hdfs://namenode:8020/user/logs'
        level         : minimalny poziom logowania (np. 'INFO', 'ERROR')
        flush_interval: co ile sekund flushować logi do HDFS
        """
        self.spark = spark_session
        self.app_id = self.spark.sparkContext.applicationId
        self.hdfs_path = hdfs_dir.rstrip("/") + f"/{self.app_id}.log"
        self.level = self.LEVELS.get(level.upper(), logging.INFO)
        self.flush_interval = flush_interval

        # konfiguracja loggera
        self.logger = logging.getLogger(f"HDFSLogger-{self.app_id}")
        self.logger.setLevel(self.level)

        # przechwycenie stdout
        sys.stdout = self

        # bufor i blokada
        self._buffer = StringIO()
        self._lock = threading.Lock()

        # wątek flushujący
        self._stop_event = threading.Event()
        self._flusher = threading.Thread(target=self._flush_periodically, daemon=True)
        self._flusher.start()

        print(f"[INIT] HDFSLogger started for Spark app: {self.app_id}")
        print(f"[INIT] Writing logs to: {self.hdfs_path}")
        print(f"[INIT] Level: {level}, flush interval: {flush_interval}s")

    def write(self, message: str):
        """Przechwytuje komunikaty stdout"""
        if message.strip():
            with self._lock:
                log_line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message.strip()}\n"
                self._buffer.write(log_line)

    def flush(self):
        """Zapisz aktualny bufor do HDFS"""
        with self._lock:
            data = self._buffer.getvalue()
            if data:
                # --- opcja 1: przez systemowe hdfs dfs ---
                subprocess.run(
                    ["hdfs", "dfs", "-appendToFile", "-", self.hdfs_path],
                    input=data.encode("utf-8"),
                    check=False,
                )

                # --- opcja 2 (zalecana): jeśli masz pydoop ---
                # with hdfs.open(self.hdfs_path, "a") as f:
                #     f.write(data)

                self._buffer = StringIO()  # czyścimy bufor

    def _flush_periodically(self):
        """Flush co kilka sekund"""
        while not self._stop_event.is_set():
            time.sleep(self.flush_interval)
            self.flush()

    def close(self):
        """Zakończenie logowania"""
        self._stop_event.set()
        self._flusher.join()
        self.flush()
        sys.stdout = sys.__stdout__
        print(f"[CLOSE] HDFSLogger closed for app: {self.app_id}")

    # --- standardowe metody logowania ---
    def debug(self, msg):    self._log("DEBUG", msg)
    def info(self, msg):     self._log("INFO", msg)
    def warning(self, msg):  self._log("WARNING", msg)
    def error(self, msg):    self._log("ERROR", msg)
    def critical(self, msg): self._log("CRITICAL", msg)

    def _log(self, level_name, msg):
        level = self.LEVELS[level_name]
        if level >= self.level:
            log_line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {level_name}: {msg}"
            with self._lock:
                self._buffer.write(log_line + "\n")



from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MySparkJob").getOrCreate()

# Tworzymy logger
logger = HDFSLogger(
    spark_session=spark,
    hdfs_dir="hdfs://namenode:8020/user/logs",
    level="INFO",
    flush_interval=3
)

logger.info("Start aplikacji Spark")
print("To też trafi do logów!")
logger.error("Wystąpił błąd podczas przetwarzania danych")

# ... reszta kodu PySpark ...

logger.close()
