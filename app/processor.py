import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime
import shutil
import os
import time
from sqlalchemy import create_engine, text

from utils import normalize, setup_logger
from dotenv import load_dotenv

# ---------------------------
# LOGGER, DB CONFIGS, MAPPING / CACHE
# ---------------------------

load_dotenv()
logger = setup_logger("processor")
MAPPING_FILE = "mapping.xlsx"
_mapping_cache = None

# ---------------------------
# MAPPING CACHE
# ---------------------------
def get_mapping():
    global _mapping_cache

    if _mapping_cache is None:
        logger.info("[MAPPING] Loading mapping file...")
        _mapping_cache = pd.read_excel(MAPPING_FILE, engine="openpyxl")
        _mapping_cache.columns = [c.strip().upper() for c in _mapping_cache.columns]
        _mapping_cache["key"] = _mapping_cache["ENDPOINT ID"].apply(normalize)

    return _mapping_cache

# ---------------------------
# CONFIG
# ---------------------------
def get_config():
    cfg = {
        "db": os.getenv("DB_NAME"),
        "table": os.getenv("DB_TABLE"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "host": os.getenv("DB_HOST", "localhost"),  # use default value if not configured in .env
        "port": os.getenv("DB_PORT", "3306")        # use default value if not configured in .env
    }

    # Validate for missing config

    missing = []

    for key, value in cfg.items():
        if value is None or str(value).strip() == "":
            missing.append(key)

    if missing:
        logger.error(f"[CONFIG] Missing required config values: {missing}")
        raise ValueError(f"Missing config: {missing}")

    # Logging
    safe_cfg = cfg.copy()
    safe_cfg["password"] = "****"

    logger.info(f"[CONFIG] Loaded config: {safe_cfg}")

    return cfg


def get_engine():
    cfg = get_config()
    uri = f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['db']}"

    logger.info(f"[DB] Connecting to {cfg['db']}")

    return create_engine(
        uri,
        pool_pre_ping=True,
        pool_recycle=60
    )


# ---------------------------
# CLEAN DATAFRAME
# ---------------------------
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Convert NaN / NaT → None (MySQL-safe)
    df = df.where(pd.notnull(df), None)

    # Convert pandas Timestamp → Python datetime
    if "reading_time" in df:
        df["reading_time"] = pd.to_datetime(df["reading_time"]).dt.to_pydatetime()

    if "file_datetime" in df:
        df["file_datetime"] = pd.to_datetime(df["file_datetime"]).dt.to_pydatetime()

    # Force string-safe fields (avoid NaN / float injection)
    for col in [
        "endpoint_id",
        "source",
        "meter_id",
        "customer_name",
        "account",
        "block_no",
        "file_name"
    ]:
        if col in df:
            df[col] = df[col].fillna("").astype(str)

    return df


# ---------------------------
# INSERT IGNORE (SAFE DUPLICATES)
# ---------------------------
def insert_ignore(df, table_name, engine):
    if df.empty:
        return 0, 0

    cols = list(df.columns)
    col_str = ", ".join(cols)
    val_str = ", ".join([f":{c}" for c in cols])

    sql = text(f"""
        INSERT IGNORE INTO {table_name} ({col_str})
        VALUES ({val_str})
    """)

    data = df.to_dict(orient="records")

    with engine.begin() as conn:
        result = conn.execute(sql, data)
        inserted = result.rowcount

    total = len(df)
    skipped = total - inserted

    return inserted, skipped


# ---------------------------
# MAIN PROCESSOR
# ---------------------------
def process_file(xml_path):
    filename = os.path.basename(xml_path)

    processing_path = f"data/processing/{filename}"
    processed_path = f"data/processed/{filename}"
    failed_path = f"data/failed/{filename}"

    cfg = get_config()
    DB_TABLE = cfg["table"]

    try:
        if not os.path.exists(processing_path):
            shutil.move(xml_path, processing_path)

        logger.info(f"Processing file: {filename}")

        # ---------------------------
        # LOAD MAPPING (CACHED)
        # ---------------------------
        mapping_df = get_mapping()

        # ---------------------------
        # PARSE XML
        # ---------------------------
        tree = ET.parse(processing_path)
        root = tree.getroot()

        # Validate creation datetime
        creation_el = root.find(".//Creation_Datetime")
        if creation_el is None:
            raise ValueError("Missing Creation_Datetime in XML")

        creation_dt = datetime.fromisoformat(creation_el.attrib["Datetime"])

        rows = []

        for channel in root.findall(".//Channel"):
            channel_id = channel.find("ChannelID").attrib.get("EndPointUOMID")

            if ":" in channel_id:
                endpoint_id, source = channel_id.split(":")
                endpoint_id = endpoint_id.split(".")[-1]
            else:
                endpoint_id = channel_id
                source = ""

            endpoint_id = normalize(endpoint_id)
            source = str(source).strip()

            for reading in channel.findall(".//Reading"):

                # ---------------------------
                # VALIDATE READING VALUE
                # ---------------------------
                raw_value = reading.attrib.get("Value")

                if raw_value is None:
                    logger.warning(f"[XML] Missing Value in {filename}, skipping")
                    continue

                try:
                    reading_value = int(float(raw_value))
                except Exception:
                    logger.warning(f"[XML] Invalid Value '{raw_value}', skipping")
                    continue

                # ---------------------------
                # VALIDATE READING TIME
                # ---------------------------
                raw_time = reading.attrib.get("ReadingTime")

                if raw_time is None:
                    logger.warning(f"[XML] Missing ReadingTime, skipping")
                    continue

                try:
                    reading_time = datetime.fromisoformat(raw_time)
                except Exception:
                    logger.warning(f"[XML] Invalid ReadingTime '{raw_time}', skipping")
                    continue

                rows.append({
                    "endpoint_id": endpoint_id,
                    "source": source,
                    "reading_value": reading_value,
                    "reading_time": reading_time,
                    "file_datetime": creation_dt,
                    "file_name": filename
                })

        df = pd.DataFrame(rows)
        logger.info(f"Extracted rows: {len(df)}")

        if df.empty:
            raise ValueError("No valid data extracted from XML")

        # ---------------------------
        # MERGE MAPPING
        # ---------------------------
        df = df.merge(
            mapping_df,
            left_on="endpoint_id",
            right_on="key",
            how="left"
        )

        df.rename(columns={
            "METER ID": "meter_id",
            "CUSTOMER NAME": "customer_name",
            "ACCOUNTS": "account",
            "BLOCK NO": "block_no"
        }, inplace=True)

        # ---------------------------
        # FINAL STRUCTURE
        # ---------------------------
        df = df[
            [
                "endpoint_id",
                "source",
                "reading_value",
                "reading_time",
                "file_datetime",
                "meter_id",
                "customer_name",
                "account",
                "block_no",
                "file_name"
            ]
        ]

        # ---------------------------
        # INTERNAL DEDUP
        # ---------------------------
        before = len(df)

        df = df.drop_duplicates(
            subset=["endpoint_id", "reading_time", "source"]
        )

        after = len(df)

        if before != after:
            logger.warning(f"[DEDUP] Removed {before - after} duplicates")

        # ---------------------------
        # CLEAN DATA
        # ---------------------------
        df = clean_dataframe(df)

        # ---------------------------
        # DB INSERT
        # ---------------------------
        engine = get_engine()

        inserted, skipped = insert_ignore(df, DB_TABLE, engine)

        logger.info(f"[DB] Inserted: {inserted}")
        logger.warning(f"[DB] Skipped (duplicates): {skipped}")

        # ---------------------------
        # VERIFY
        # ---------------------------
        with engine.connect() as conn:
            total = conn.execute(
                text(f"SELECT COUNT(*) FROM {DB_TABLE}")
            ).scalar()

        logger.info(f"[DB] Total rows: {total}")

        # ---------------------------
        # MOVE FILE
        # ---------------------------
        shutil.move(processing_path, processed_path)
        logger.info(f"Completed: {filename}")

    except Exception as e:
        logger.error(f"FAILED {filename}: {str(e)}")

        if os.path.exists(processing_path):
            shutil.move(processing_path, failed_path)


# ---------------------------
# RETRY WRAPPER
# ---------------------------
def process_with_retry(filepath, retries=3):
    for attempt in range(retries):
        try:
            logger.info(f"[RETRY] Attempt {attempt+1}")
            process_file(filepath)
            return
        except Exception as e:
            logger.warning(f"[RETRY FAILED] {e}")
            time.sleep(3)

    logger.error(f"[FAILED] Exhausted retries: {filepath}")