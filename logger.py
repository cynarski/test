import logging
import sys
import threading
import time
from datetime import datetime
from io import StringIO
import subprocess

class HDFSLogger:
    LEVELS = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    def __init__(self, hdfs_path: str, level: str = "INFO"):
        """
        hdfs_path: ścieżka do pliku w HDFS (np. hdfs://namenode:8020/user/logs/app.log)
        level: minimalny poziom logowania
        """
        self.hdfs_path = hdfs_path
        self.level = self.LEVELS.get(level.upper(), logging.INFO)

        # --- Konfiguracja loggera ---
        self.logger = logging.getLogger("HDFSLogger")
        self.logger.setLevel(self.level)

        # --- Przechwytywanie stdout ---
        sys.stdout = self  # zastępuje sys.stdout na nasz obiekt

        # Bufor i blokada (dla wielowątkowości)
        self._buffer = StringIO()
        self._lock = threading.Lock()

        # --- Start wątku do flushowania logów ---
        self._stop_event = threading.Event()
        self._flusher = threading.Thread(target=self._flush_periodically, daemon=True)
        self._flusher.start()

    def write(self, message):
        """Przechwytuje stdout"""
        if message.strip():  # pomijamy puste linie
            with self._lock:
                log_line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message.strip()}\n"
                self._buffer.write(log_line)
                print(f"Captured: {log_line.strip()}", file=sys.__stdout__)  # opcjonalne echo lokalne

    def flush(self):
        """Ręczne zapisanie bufora do HDFS"""
        with self._lock:
            data = self._buffer.getvalue()
            if data:
                # Append do pliku w HDFS przez `hdfs dfs -appendToFile`
                subprocess.run(
                    ["hdfs", "dfs", "-appendToFile", "-", self.hdfs_path],
                    input=data.encode("utf-8"),
                    check=False,
                )
                self._buffer = StringIO()  # czyścimy bufor

    def _flush_periodically(self, interval=5):
        """Flush co kilka sekund"""
        while not self._stop_event.is_set():
            time.sleep(interval)
            self.flush()

    def close(self):
        """Zatrzymanie flushowania i zamknięcie"""
        self._stop_event.set()
        self._flusher.join()
        self.flush()
        sys.stdout = sys.__stdout__  # przywracamy stdout

    # --- Standardowy interfejs loggera ---
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
