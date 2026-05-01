import logging
import os

def setup_logger(name="app"):
    os.makedirs("logs", exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s [%(name)s] %(message)s"
        )

        file_handler = logging.FileHandler("logs/processor.log")
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger


def normalize(x):
    try:
        return str(int(float(str(x).replace("\xa0", "").strip())))
    except:
        return str(x).replace("\xa0", "").strip().lstrip("0")