"""Shared configuration for the NammaMart pipelines (ETL + ELT)."""
import os
from pathlib import Path

NAME = "hosur_narahari"

# Inside the Astro container AIRFLOW_HOME=/usr/local/airflow. For local testing
# export AIRFLOW_HOME=<project root> and the same code works unchanged.
AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"))
INCLUDE_DIR = AIRFLOW_HOME / "include"
DATA_DIR = INCLUDE_DIR / "data" / "nammamart"
WAREHOUSE_DB = INCLUDE_DIR / f"warehouse_{NAME}.duckdb"
QUARANTINE_DIR = INCLUDE_DIR / "quarantine" / NAME
WORK_DIR = INCLUDE_DIR / "work" / NAME
SQL_DIR = INCLUDE_DIR / "sql"

TZ = "Asia/Kolkata"

SOURCE_FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}
PRIMARY_KEYS = {
    "orders": "order_id",
    "payments": "payment_id",
    "customers": "customer_id",
    "products": "product_id",
    "stores": "store_id",
}

VALID_ORDER_STATUS = ["DELIVERED", "CANCELLED", "RETURNED"]
VALID_PAYMENT_MODE = ["UPI", "Card", "Cash on Delivery", "Wallet"]
VALID_PAYMENT_STATUS = ["SUCCESS", "FAILED", "REFUNDED"]
VALID_LOYALTY_TIER = ["Bronze", "Silver", "Gold", "Platinum"]
VALID_ZONE = ["North", "South", "East", "West"]
VALID_CHANNEL = ["App", "Web"]
VALID_CATEGORY = ["Fruits & Vegetables", "Dairy & Bakery", "Snacks & Beverages",
                  "Staples", "Household", "Personal Care"]

# Canonical payment-mode spellings. Key = lower-cased, whitespace-stripped input.
PAYMENT_MODE_MAP = {
    "upi": "UPI",
    "card": "Card",
    "cashondelivery": "Cash on Delivery",
    "cod": "Cash on Delivery",
    "cash": "Cash on Delivery",
    "cashh": "Cash on Delivery",
    "wallet": "Wallet",
}

DELIVERY_MIN_NORMAL = 8
DELIVERY_MAX_NORMAL = 35
DELIVERY_MAX_HARD = 120          # anything above is impossible → Critical
SLA_MINUTES = 15
STALE_AFTER_DAYS = 1             # timeliness warning
VOLUME_DROP_WARN_PCT = 90        # volume anomaly (Part F exp 7)
EXPECTED_MIN_ORDER_ROWS = 500    # rough daily floor for the volume check


def safe_run_id(run_id: str) -> str:
    """Make an Airflow run_id usable in a file name."""
    return "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in run_id)
