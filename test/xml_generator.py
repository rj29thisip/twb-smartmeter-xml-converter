import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import random
import os
import csv

# ---------------------------
# CONFIG
# ---------------------------
START_DATE = datetime(2026, 6, 1, 0, 0, 0)     # <-- change to target "from" date
END_DATE   = datetime(2026, 7, 6, 23, 0, 0)    # <-- change to target "to" date
INTERVAL_HOURS = 1

OUTPUT_DIR = "./output_xml"

ENDPOINTS = [
    "2.16.840.1.114416.17.0120206579:LiterVolume",    # <-- update target meter endpoint(s) for target customer(s) meter
    "2.16.840.1.114416.17.0120206581:LiterVolume",    # <-- update target meter endpoint(s) for target customer(s) meter
    "2.16.840.1.114416.17.0120206588:LiterVolume"    # <-- update target meter endpoint(s) for target customer(s) meter
]

CSV_FILE = "./base_readings.csv"    # <-- update csv file for target meter endpoint(s) and starting reading(s) value

INCREMENT_RANGE = (5, 60)

# ---------------------------
# LOAD BASE CSV
# ---------------------------

def load_base_readings(filename):
    values = {}

    if not os.path.exists(filename):
        raise FileNotFoundError(
            f"Base file '{filename}' not found!"
        )

    with open(filename, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            endpoint = row["endpoint"].strip()
            values[endpoint] = int(row["start_value"])
    
    missing = []

    for endpoint in ENDPOINTS:
        if endpoint not in values:
            missing.append(endpoint)

    if missing:
        raise ValueError(
            "Missing base readings for:\n"
            "\n".join(missing)
        )

    return values

BASE_VALUES = load_base_readings(CSV_FILE)

# ---------------------------
# GENERATE READINGS FOR A DAY
# ---------------------------
def generate_readings(endpoint, start_dt, end_dt, base_value):
    readings = []
    current_time = start_dt
    value = base_value

    while current_time <= end_dt:
        value += random.randint(*INCREMENT_RANGE)

        readings.append({
            "value": value,
            "time": current_time.strftime("%Y-%m-%dT%H:%M:%S")
        })

        current_time += timedelta(hours=INTERVAL_HOURS)

    return readings, value  # return updated value for continuity


# ---------------------------
# BUILD ONE DAY XML
# ---------------------------
def build_daily_xml(day_start, day_end, base_values):
    root = ET.Element("MeterReadingDocument")

    # HEADER
    header = ET.SubElement(root, "Header")
    ET.SubElement(header, "IEE_System", Id="OpenWay")
    ET.SubElement(header, "Creation_Datetime", Datetime=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
    ET.SubElement(header, "Timezone", Id="UTC")
    ET.SubElement(header, "Path", FilePath="C:\\DummyData\\generated.xml")
    ET.SubElement(header, "Export_Template", Id="DefaultReadingXmlExport")
    ET.SubElement(header, "CorrelationID", Id=str(random.randint(1000000, 9999999)))

    # PARAMETERS
    params = ET.SubElement(root, "ImportExportParameters", CreateResubmitFile="false")
    ET.SubElement(params, "DataFormat",
                  ReadingTimestampType="Utc",
                  DSTTransitionType="ITRON_Compliant")

    # CHANNELS
    channels = ET.SubElement(root, "Channels")

    new_base_values = base_values.copy()

    for endpoint in ENDPOINTS:

        channel = ET.SubElement(channels, "Channel",
                                IsRegister="true",
                                MarketType="Water",
                                IsReadingDecoded="false",
                                ReadingsInPulse="false")

        ET.SubElement(channel, "ChannelID", EndPointUOMID=endpoint)

        readings_el = ET.SubElement(channel, "Readings")

        readings, updated_value = generate_readings(
            endpoint,
            day_start,
            day_end,
            new_base_values[endpoint]
        )

        new_base_values[endpoint] = updated_value

        for r in readings:
            reading = ET.SubElement(readings_el, "Reading",
                                    Value=str(r["value"]),
                                    ReadingTime=r["time"])

            status = ET.SubElement(reading, "ReadingStatus")
            ET.SubElement(status, "UnencodedStatus", SourceValidation="NV")

    return root, new_base_values


# ---------------------------
# MAIN GENERATION LOOP (DAILY FILES)
# ---------------------------
def run():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    base_values = BASE_VALUES.copy()
    current_day = START_DATE

    while current_day <= END_DATE:

        day_start = current_day
        day_end = min(
            current_day.replace(hour=23),
            END_DATE
        )

        root, base_values = build_daily_xml(day_start, day_end, base_values)

        tree = ET.ElementTree(root)

        # ✅ Pretty formatting (NEW LINES + INDENTATION)
        ET.indent(tree, space="  ", level=0)

        filename = f"{OUTPUT_DIR}/readings_{day_start.strftime('%Y-%m-%d')}.xml"
        tree.write(filename, encoding="utf-8", xml_declaration=True)

        print(f"Generated: {filename}")

        current_day += timedelta(days=1)


# ---------------------------
# RUN
# ---------------------------
if __name__ == "__main__":
    run()