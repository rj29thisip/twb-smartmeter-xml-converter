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
# INIT
# ---------------------------
load_dotenv()
logger = setup_logger("processor")

# ---------------------------
# MAPPING CACHE (DB)
# ---------------------------
_mapping_cache = None
_mapping_last_load = 0


def get_mapping_from_db(engine):
    query = """
    SELECT 
        m.endpoint_id,
        m.id AS meter_id
    FROM meters m
    WHERE m.status = 'active'
    """
    df = pd.read_sql(query, engine)
    df["endpoint_id"] = df["endpoint_id"].apply(normalize)
    return df


def get_mapping(engine, ttl=300):
    global _mapping_cache, _mapping_last_load

    now = time.time()

    if _mapping_cache is None or (now - _mapping_last_load > ttl):
        logger.info("[MAPPING] Refreshing mapping from DB...")
        _mapping_cache = get_mapping_from_db(engine)
        _mapping_last_load = now

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
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "3306")
    }

    missing = [k for k, v in cfg.items() if not v]

    if missing:
        logger.error(f"[CONFIG] Missing config: {missing}")
        raise ValueError(f"Missing config: {missing}")

    safe_cfg = cfg.copy()
    safe_cfg["password"] = "****"
    logger.info(f"[CONFIG] Loaded config: {safe_cfg}")

    return cfg


def get_engine():
    cfg = get_config()

    uri = f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['db']}"

    logger.info(f"[DB] Connecting to {cfg['db']}")

    return create_engine(uri, pool_pre_ping=True, pool_recycle=60)


# ---------------------------
# CLEAN DATAFRAME
# ---------------------------
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.where(pd.notnull(df), None)

    if "capture_time" in df:
        df["capture_time"] = pd.to_datetime(df["capture_time"]).dt.floor("s")

    if "received_time" in df:
        df["received_time"] = pd.to_datetime(df["received_time"]).dt.floor("s")

    # Source truncation (limit length)
    if "source" in df:
        df["source"] = df["source"].fillna("system").astype(str).str[:20]

    if "register_type" in df:
        df["register_type"] = df["register_type"].fillna("default").astype(str).str[:50]

    return df


# ---------------------------
# INSERT IGNORE
# ---------------------------
def insert_ignore(df, table_name, engine):
    if df.empty:
        return 0, 0

    cols = list(df.columns)

    col_str = ", ".join([f"`{c}`" for c in cols])
    val_str = ", ".join([f":{c}" for c in cols])

    sql = text(f"""
        INSERT IGNORE INTO `{table_name}` ({col_str})
        VALUES ({val_str})
    """)

    data = df.to_dict(orient="records")

    with engine.begin() as conn:
        result = conn.execute(sql, data)

    inserted = result.rowcount
    skipped = len(df) - inserted

    return inserted, skipped


# ---------------------------
# LAYER 1 — BUSINESS LOGIC 
# ---------------------------
def _do_process(processing_path, filename, engine, mapping_df, table_name):

    tree = ET.parse(processing_path)
    root = tree.getroot()

    creation_el = root.find(".//Creation_Datetime")
    if creation_el is None:
        raise ValueError("Missing Creation_Datetime")

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

        for reading in channel.findall(".//Reading"):

            raw_value = reading.attrib.get("Value")
            if raw_value is None:
                continue

            try:
                reading_value = int(float(raw_value))
            except:
                continue

            raw_time = reading.attrib.get("ReadingTime")
            if raw_time is None:
                continue

            try:
                reading_time = datetime.fromisoformat(raw_time)
            except:
                continue

            rows.append({
                "endpoint_id": endpoint_id,
                "register_type": source.lower(),
                "value": reading_value,
                "capture_time": reading_time,
                "received_time": creation_dt
            })

    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError("No valid data extracted")

    # ---------------------------
    # MAP endpoint → meter_id
    # ---------------------------
    df = df.merge(mapping_df, on="endpoint_id", how="left")

    missing = df[df["meter_id"].isna()]
    if not missing.empty:
        logger.warning(f"[MAPPING] Dropping unmapped rows: {len(missing)}")

    df = df[df["meter_id"].notna()]
    df["meter_id"] = df["meter_id"].astype(int)

    # ---------------------------
    # FINAL STRUCTURE
    # ---------------------------
    now = datetime.now()

    df = pd.DataFrame({
        "meter_id": df["meter_id"],
        "capture_time": df["capture_time"],
        "received_time": df["received_time"],
        "register_type": df["register_type"],
        "value": df["value"],
        "usage": 0,
        "source": "system",
        "is_anomaly": 0,
        "anomaly_note": None,
        "created_at": now,
        "updated_at": now
    })

    # dedup
    df = df.drop_duplicates(
        subset=["meter_id", "capture_time", "register_type"]
    )

    df = clean_dataframe(df)

    inserted, skipped = insert_ignore(df, table_name, engine)

    logger.info(f"[DB] Inserted: {inserted}")
    logger.info(f"[DB] Skipped: {skipped}")


# ---------------------------
# LAYER 2 — FILE HANDLING
# ---------------------------
def process_file(xml_path):
    filename = os.path.basename(xml_path)

    processing_path = f"data/processing/{filename}"
    processed_path = f"data/processed/{filename}"

    engine = get_engine()
    mapping_df = get_mapping(engine)
    table_name = get_config()["table"]

    try:
        if not os.path.exists(processing_path):
            shutil.move(xml_path, processing_path)

        logger.info(f"Processing file: {filename}")

        _do_process(processing_path, filename, engine, mapping_df, table_name)

        shutil.move(processing_path, processed_path)
        logger.info(f"Completed: {filename}")

    except Exception as e:
        logger.error(f"FAILED {filename}: {str(e)}")
        raise


# ---------------------------
# LAYER 3 — RETRY CONTROL
# ---------------------------
def process_with_retry(filepath, retries=3, delay=3):
    filename = os.path.basename(filepath)

    processing_path = f"data/processing/{filename}"
    failed_path = f"data/failed/{filename}"

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"[RETRY] Attempt {attempt} for {filename}")
            process_file(filepath)
            return

        except Exception as e:
            logger.warning(f"[RETRY] Attempt {attempt} failed: {e}")

            if attempt < retries:
                time.sleep(delay)

    # Move to \failed after retries
    if os.path.exists(processing_path):
        shutil.move(processing_path, failed_path)
    elif os.path.exists(filepath):
        shutil.move(filepath, failed_path)

    logger.error(f"[FAILED] Exhausted retries: {filename}")