import pandas as pd
from sqlalchemy import create_engine

# ========= 1. CONFIG =========

# 1) Your Neon URL (same style you used in setup_db.py)
NEON_URL = "postgresql+psycopg2://neondb_owner:**************@ep-lingering-darkness-a13w1fg0-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"

# 2) Path to your CSV (the one that worked in check_columns.py)
CSV_PATH = r"C:\Users\POWERBI PROJECT\data\raw\DataCoSupplyChainDataset.csv"

# ========= 2. COLUMN NAME CONSTANTS (MATCH YOUR DATA) =========
# These are EXACT column names from your Index(...)

COL_ORDER_ID            = "Order Id"
COL_ORDER_DATE          = "order date (DateOrders)"
COL_SHIP_DATE           = "shipping date (DateOrders)"
COL_PRODUCT_NAME        = "Product Name"
COL_PRODUCT_CAT         = "Category Name"

COL_CUST_FNAME          = "Customer Fname"
COL_CUST_LNAME          = "Customer Lname"
COL_CUSTOMER_SEG        = "Customer Segment"
COL_CUSTOMER_CITY       = "Customer City"
COL_CUSTOMER_COUNTRY    = "Customer Country"

COL_SUPPLIER_NAME       = "Department Name"   # treating department as 'supplier'
COL_SUPPLIER_COUNTRY    = "Order Country"

COL_QUANTITY            = "Order Item Quantity"
COL_UNIT_PRICE          = "Order Item Product Price"
COL_ITEM_TOTAL          = "Order Item Total"
COL_PROFIT_RATIO        = "Order Item Profit Ratio"

# ========= 3. READ CSV =========

print("📥 Reading CSV...")
df = pd.read_csv(CSV_PATH, encoding="latin1", on_bad_lines="skip")

print("✅ CSV loaded. Number of rows:", len(df))

# ========= 4. BASIC CLEANING & FEATURE ENGINEERING =========

# Dates
df[COL_ORDER_DATE] = pd.to_datetime(df[COL_ORDER_DATE])
df[COL_SHIP_DATE]  = pd.to_datetime(df[COL_SHIP_DATE])

# Lead time in days
df["lead_time_days"] = (df[COL_SHIP_DATE] - df[COL_ORDER_DATE]).dt.days

# On-time flag (<= 5 days)
df["on_time_flag"] = df["lead_time_days"] <= 5

# Customer full name
df["customer_name"] = df[COL_CUST_FNAME].fillna("") + " " + df[COL_CUST_LNAME].fillna("")

# Revenue, cost, profit
df["total_revenue"] = df[COL_ITEM_TOTAL]

# Profit ratio is typically between 0 and 1; handle missing / bad values
df[COL_PROFIT_RATIO] = pd.to_numeric(df[COL_PROFIT_RATIO], errors="coerce").fillna(0.0)
df["product_cost"] = df["total_revenue"] * (1 - df[COL_PROFIT_RATIO])
df["shipping_cost"] = 0.0  # no explicit shipping cost column
df["profit"] = df["total_revenue"] - df["product_cost"] - df["shipping_cost"]

# ========= 5. BUILD DIMENSION TABLES IN PANDAS =========

print("🧱 Building dimension tables...")

# ----- dim_product -----
dim_product = (
    df[[COL_PRODUCT_NAME, COL_PRODUCT_CAT]]
    .drop_duplicates()
    .copy()
)
dim_product = dim_product.rename(
    columns={
        COL_PRODUCT_NAME: "product_name",
        COL_PRODUCT_CAT: "category",
    }
)
dim_product["subcategory"] = ""  # optional
dim_product = dim_product.reset_index(drop=True)
dim_product["product_id"] = dim_product.index + 1

# ----- dim_customer -----
dim_customer = (
    df[["customer_name", COL_CUSTOMER_SEG, COL_CUSTOMER_CITY, COL_CUSTOMER_COUNTRY]]
    .drop_duplicates()
    .copy()
)
dim_customer = dim_customer.rename(
    columns={
        COL_CUSTOMER_SEG: "segment",
        COL_CUSTOMER_CITY: "city",
        COL_CUSTOMER_COUNTRY: "country",
    }
)
dim_customer["region"] = ""  # can fill if you want to map regions
dim_customer = dim_customer.reset_index(drop=True)
dim_customer["customer_id"] = dim_customer.index + 1

# ----- dim_supplier -----
dim_supplier = (
    df[[COL_SUPPLIER_NAME, COL_SUPPLIER_COUNTRY]]
    .drop_duplicates()
    .copy()
)
dim_supplier = dim_supplier.rename(
    columns={
        COL_SUPPLIER_NAME: "supplier_name",
        COL_SUPPLIER_COUNTRY: "supplier_country",
    }
)
dim_supplier = dim_supplier.reset_index(drop=True)
dim_supplier["supplier_id"] = dim_supplier.index + 1

# ----- dim_date -----
date_range = pd.date_range(df[COL_ORDER_DATE].min(), df[COL_SHIP_DATE].max(), freq="D")
dim_date = pd.DataFrame(
    {
        "date_id": date_range.date,
        "year": date_range.year,
        "month": date_range.month,
        "month_name": date_range.strftime("%b"),
        "quarter": date_range.quarter,
    }
)

# ========= 6. MERGE DIM IDS BACK INTO MAIN DF =========

print("🔁 Merging dimension keys into fact data...")

# Merge product_id
df = df.merge(
    dim_product[["product_id", "product_name", "category"]],
    left_on=[COL_PRODUCT_NAME, COL_PRODUCT_CAT],
    right_on=["product_name", "category"],
    how="left",
)

# Merge customer_id
df = df.merge(
    dim_customer[["customer_id", "customer_name", "segment", "city", "country"]],
    left_on=["customer_name", COL_CUSTOMER_SEG, COL_CUSTOMER_CITY, COL_CUSTOMER_COUNTRY],
    right_on=["customer_name", "segment", "city", "country"],
    how="left",
)

# Merge supplier_id
df = df.merge(
    dim_supplier[["supplier_id", "supplier_name", "supplier_country"]],
    left_on=[COL_SUPPLIER_NAME, COL_SUPPLIER_COUNTRY],
    right_on=["supplier_name", "supplier_country"],
    how="left",
)

# ========= 7. BUILD FACT TABLE =========

print("📦 Building fact_orders table...")

fact_orders = pd.DataFrame(
    {
        "order_id": df[COL_ORDER_ID].astype(str),
        "order_date_id": df[COL_ORDER_DATE].dt.date,
        "ship_date_id": df[COL_SHIP_DATE].dt.date,
        "customer_id": df["customer_id"],
        "product_id": df["product_id"],
        "supplier_id": df["supplier_id"],
        "quantity": df[COL_QUANTITY],
        "unit_price": df[COL_UNIT_PRICE],
        "shipping_cost": df["shipping_cost"],
        "total_revenue": df["total_revenue"],
        "profit": df["profit"],
        "lead_time_days": df["lead_time_days"],
        "on_time_flag": df["on_time_flag"],
    }
)

print("Sample of fact_orders:")
print(fact_orders.head())

# ========= 8. WRITE TO NEON (POSTGRESQL) =========

print("🔌 Connecting to Neon...")
engine = create_engine(NEON_URL)

print("⬆ Uploading tables to Neon (this may take a bit)...")

with engine.begin() as conn:
    dim_date.to_sql("dim_date", conn, if_exists="replace", index=False)
    dim_product[
        ["product_id", "product_sku", "product_name", "category", "subcategory"]
        if "product_sku" in dim_product.columns
        else ["product_id", "product_name", "category", "subcategory"]
    ]
    # ensure product_sku exists
    # create product_sku if missing
if "product_sku" not in dim_product.columns:
    dim_product["product_sku"] = dim_product["product_name"].str[:20]

with engine.begin() as conn:
    dim_product[["product_id", "product_sku", "product_name", "category", "subcategory"]].to_sql(
        "dim_product", conn, if_exists="replace", index=False
    )
    dim_customer[
        ["customer_id", "customer_name", "segment", "city", "country", "region"]
    ].to_sql("dim_customer", conn, if_exists="replace", index=False)
    dim_supplier[
        ["supplier_id", "supplier_name", "supplier_country"]
    ].to_sql("dim_supplier", conn, if_exists="replace", index=False)
    fact_orders.to_sql("fact_orders", conn, if_exists="replace", index=False)

print("✅ ETL complete. Data is now in Neon PostgreSQL.")
#etl_to_neon


