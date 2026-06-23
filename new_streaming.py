import os
import json
import logging
import threading
from datetime import datetime

import pyodbc

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ==================================================
# CONFIG
# ==================================================

WATCH_DIR = "./watched_folder"

CONN_STR = (
    r"Driver={ODBC Driver 17 for SQL Server};"
    r"Server=(localdb)\MSSQLLocalDB;"
    r"Trusted_Connection=yes;"
)

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# ==================================================
# LOGGING
# ==================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/inventory.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("InventoryPipeline")

# ==================================================
# SPARK SESSION
# ==================================================

spark = (
    SparkSession.builder
    .appName("InventoryStreamingPipeline")
    .master("local[*]")
    .getOrCreate()
)

# ==================================================
# DATABASE
# ==================================================

class DatabaseManager:

    def __init__(self):
        self.conn_str = CONN_STR

    def get_connection(self):
        return pyodbc.connect(self.conn_str)

    def setup_database(self):

        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
        IF NOT EXISTS
        (
            SELECT *
            FROM sys.tables
            WHERE name='inventory'
        )
        BEGIN

            CREATE TABLE inventory
            (
                id NVARCHAR(50) PRIMARY KEY,
                item_name NVARCHAR(255),
                quantity INT,
                last_updated DATETIME
            )

        END
        """)

        conn.commit()
        conn.close()

        logger.info("Inventory table verified.")

    def batch_upsert(self, records):

        if not records:
            return

        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.fast_executemany = True

        merge_sql = """
        MERGE inventory AS target
        USING
        (
            SELECT
                ? AS id,
                ? AS item_name,
                ? AS quantity
        ) AS source

        ON target.id = source.id

        WHEN MATCHED THEN
            UPDATE SET
                item_name = source.item_name,
                quantity = source.quantity,
                last_updated = GETDATE()

        WHEN NOT MATCHED THEN
            INSERT
            (
                id,
                item_name,
                quantity,
                last_updated
            )
            VALUES
            (
                source.id,
                source.item_name,
                source.quantity,
                GETDATE()
            );
        """

        params = []

        for r in records:
            params.append(
                (
                    r["id"],
                    r["item_name"],
                    int(r["quantity"])
                )
            )

        cursor.executemany(merge_sql, params)

        conn.commit()
        conn.close()

        logger.info(
            f"Batch upsert completed. Rows={len(records)}"
        )


db = DatabaseManager()

# ==================================================
# VALIDATION
# ==================================================

class Validator:

    @staticmethod
    def validate(record):

        try:

            if not record["id"]:
                return False

            if not record["item_name"]:
                return False

            qty = int(record["quantity"])

            if qty < 0:
                return False

            return True

        except Exception:
            return False

# ==================================================
# PROCESSOR
# ==================================================

class InventoryProcessor:

    def process_json(self, path):

        logger.info(f"Processing JSON {path}")

        try:

            df = spark.read.option(
                "multiline",
                "true"
            ).json(path)

            records = [
                row.asDict()
                for row in df.collect()
            ]

            valid_records = []

            for record in records:

                if Validator.validate(record):
                    valid_records.append(record)

            db.batch_upsert(valid_records)

            logger.info(
                f"JSON processed successfully "
                f"({len(valid_records)} rows)"
            )

        except Exception as e:

            logger.error(
                f"JSON processing failed: {e}"
            )

    def process_csv(self, path):

        logger.info(f"Processing CSV {path}")

        try:

            df = (
                spark.read
                .option("header", True)
                .csv(path)
            )

            df = df.filter(
                col("id").isNotNull()
            )

            records = [
                row.asDict()
                for row in df.collect()
            ]

            valid_records = []

            for record in records:

                if Validator.validate(record):
                    valid_records.append(record)

            db.batch_upsert(valid_records)

            logger.info(
                f"CSV processed successfully "
                f"({len(valid_records)} rows)"
            )

        except Exception as e:

            logger.error(
                f"CSV processing failed: {e}"
            )

    def process_sql(self, path):

        logger.info(f"Executing SQL {path}")

        try:

            with open(path, "r") as f:
                sql = f.read()

            if not sql.strip():
                return

            conn = db.get_connection()
            cursor = conn.cursor()

            cursor.execute(sql)

            conn.commit()
            conn.close()

            logger.info(
                "Custom SQL executed successfully."
            )

        except Exception as e:

            logger.error(
                f"SQL execution failed: {e}"
            )

processor = InventoryProcessor()

# ==================================================
# WATCHDOG EVENT HANDLER
# ==================================================

class InventoryEventHandler(
    FileSystemEventHandler
):

    def on_created(self, event):

        if event.is_directory:
            return

        self.process(event.src_path)

    def on_modified(self, event):

        if event.is_directory:
            return

        self.process(event.src_path)

    def process(self, path):

        if path.endswith(".json"):
            processor.process_json(path)

        elif path.endswith(".csv"):
            processor.process_csv(path)

        elif path.endswith(".sql"):
            processor.process_sql(path)

# ==================================================
# MAIN
# ==================================================

def start_pipeline():

    os.makedirs(
        WATCH_DIR,
        exist_ok=True
    )

    db.setup_database()

    event_handler = InventoryEventHandler()

    observer = Observer()

    observer.schedule(
        event_handler,
        WATCH_DIR,
        recursive=False
    )

    observer.start()

    logger.info(
        f"Watching folder: {WATCH_DIR}"
    )

    try:

        while True:
            threading.Event().wait(1)

    except KeyboardInterrupt:

        observer.stop()

        logger.info(
            "Pipeline stopped gracefully."
        )

    observer.join()

    spark.stop()

# ==================================================
# ENTRY POINT
# ==================================================

if __name__ == "__main__":
    start_pipeline()
