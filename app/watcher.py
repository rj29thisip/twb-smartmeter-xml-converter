from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import os
import threading

from processor import process_with_retry
from utils import setup_logger

logger = setup_logger("watcher")

WATCH_DIR = "data/incoming/"


def is_file_stable(path, checks=5, wait_time=2):
    try:
        previous_size = -1

        for i in range(checks):
            current_size = os.path.getsize(path)
            logger.info(f"[STABILITY] Check {i+1}: size={current_size}")

            if current_size == previous_size:
                return True

            previous_size = current_size
            time.sleep(wait_time)

        return False

    except Exception as e:
        logger.error(f"Stability check failed: {e}")
        return False


def recover_unfinished():
    logger.info("[RECOVERY] Checking unfinished files...")

    for folder in ["data/processing", "data/failed"]:
        for file in os.listdir(folder):
            if file.endswith(".xml"):
                path = os.path.join(folder, file)
                logger.info(f"[RECOVERY] Reprocessing: {path}")
                process_with_retry(path)


def periodic_recovery():
    while True:
        logger.info("[RECOVERY] Periodic recovery running...")
        recover_unfinished()
        time.sleep(60)


class Handler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return

        if event.src_path.endswith(".xml"):
            logger.info(f"New file detected: {event.src_path}")

            logger.info("Waiting for file stability...")

            if not is_file_stable(event.src_path):
                logger.warning(f"File not stable, skipping: {event.src_path}")
                return

            logger.info(f"File is stable, processing: {event.src_path}")

            process_with_retry(event.src_path)


if __name__ == "__main__":
    # Initial recovery
    recover_unfinished()

    # Background recovery thread
    t = threading.Thread(target=periodic_recovery, daemon=True)
    t.start()

    observer = Observer()
    observer.schedule(Handler(), WATCH_DIR, recursive=False)
    observer.start()

    logger.info("Watching for XML files...")

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()