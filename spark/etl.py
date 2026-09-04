import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# ============================================================
# 1. JAVA
# ============================================================

java_home = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.32.101-hotspot"

os.environ["JAVA_HOME"] = java_home
os.environ["PATH"] = (
    os.path.join(java_home, "bin")
    + os.pathsep
    + os.environ.get("PATH", "")
)

# ============================================================
# 2. HADOOP
# ============================================================

hadoop_home = r"C:\hadoop-3.3.6"

os.environ["HADOOP_HOME"] = hadoop_home
os.environ["HADOOP_HOME_DIR"] = hadoop_home
os.environ["hadoop.home.dir"] = hadoop_home

os.environ["PATH"] = (
    os.path.join(hadoop_home, "bin")
    + os.pathsep
    + os.environ.get("PATH", "")
)

# ============================================================
# 3. SPARK
# ============================================================

spark_home = r"D:\real-time-delivery-platform\venv\Lib\site-packages\pyspark"

os.environ["SPARK_HOME"] = spark_home
os.environ["PATH"] = (
    os.path.join(spark_home, "bin")
    + os.pathsep
    + os.environ.get("PATH", "")
)

# ============================================================
# 4. TEMP
# ============================================================

os.makedirs(r"D:\spark-temp", exist_ok=True)

# ============================================================
# 5. LOAD ENVIRONMENT
# ============================================================

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

if not AWS_ACCESS_KEY:
    raise ValueError("Missing AWS_ACCESS_KEY_ID")

if not AWS_SECRET_KEY:
    raise ValueError("Missing AWS_SECRET_ACCESS_KEY")

if not S3_BUCKET:
    raise ValueError("Missing S3_BUCKET_NAME")

# ============================================================
# 6. KHỞI TẠO SPARK
# ============================================================

print("Đang khởi động Apache Spark...")

spark = (
    SparkSession.builder
    .appName("Delivery_ETL_Module4")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "io.delta:delta-spark_2.12:3.1.0"
    )
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("Spark đã sẵn sàng!")

# ============================================================
# 3. KHAI BÁO SOURCE PATH
# ============================================================

RAW_BASE_S3 = f"s3a://{S3_BUCKET}/raw"
EVENTS_RAW = f"{RAW_BASE_S3}/delivery_events/*.json"

LOCAL_RAW = "data/raw"

# Đọc file orders đã được làm sạch từ Module 1
LOCAL_PROCESSED = "data/processed_local"

ORDERS_RAW = f"{LOCAL_PROCESSED}/orders_clean.csv" 
CUSTOMERS_RAW = f"{LOCAL_RAW}/olist_customers_dataset.csv"
ORDER_ITEMS_RAW = f"{LOCAL_RAW}/olist_order_items_dataset.csv"
PRODUCTS_RAW = f"{LOCAL_RAW}/olist_products_dataset.csv"
SELLERS_RAW = f"{LOCAL_RAW}/olist_sellers_dataset.csv"
GEOLOCATION_RAW = f"{LOCAL_RAW}/olist_geolocation_dataset.csv"

# ============================================================
# 4. ĐỌC DATASET OLIST
# ============================================================

print("\n Đang đọc dữ liệu Olist từ S3...")

df_orders = spark.read.option("header", "true").option("inferSchema", "true").csv(ORDERS_RAW)
df_customers = spark.read.option("header", "true").option("inferSchema", "true").csv(CUSTOMERS_RAW)
df_order_items = spark.read.option("header", "true").option("inferSchema", "true").csv(ORDER_ITEMS_RAW)
df_products = spark.read.option("header", "true").option("inferSchema", "true").csv(PRODUCTS_RAW)
df_sellers = spark.read.option("header", "true").option("inferSchema", "true").csv(SELLERS_RAW)
df_geo = spark.read.option("header", "true").option("inferSchema", "true").csv(GEOLOCATION_RAW)

print("Đã đọc toàn bộ 6 dataset Olist.")

# ============================================================
# 5. RAW EVENTS → PROCESSED PARQUET (TỐI ƯU HÓA I/O MẠNG)
# ============================================================

print("RAW EVENTS -> PROCESSED")

df_events_raw = spark.read.json(EVENTS_RAW)
raw_event_count = df_events_raw.count()
print(f"Tổng số event raw: {raw_event_count:,}")

# Deduplicate event_id
df_events_clean = df_events_raw.dropDuplicates(["event_id"])
clean_event_count = df_events_clean.count()
print(f"Sau dedup event_id: {clean_event_count:,}")

# Casting timestamp (Không cần tạo cột year, month, day nữa để tránh partition)
df_events_processed = (
    df_events_clean
    .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
    .withColumn("produced_at", F.to_timestamp("produced_at"))
)

processed_path = f"s3a://{S3_BUCKET}/processed/delivery_events/"
print(f"Ghi Processed Parquet (Tối ưu I/O): {processed_path}")

# Ép Spark gom dữ liệu thành 4 file lớn (.coalesce(4)) và bỏ .partitionBy
(
    df_events_processed
    .coalesce(4)
    .write.mode("overwrite")
    .parquet(processed_path)
)
print("Processed Layer hoàn tất.")

# ============================================================
# 7. CURATED — DIM_CUSTOMER
# ============================================================
print("DIM_CUSTOMER")

df_dim_customer = (
    df_customers
    .select("customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state")
    .dropDuplicates(["customer_id"])
    .withColumn("customer_zip_code_prefix", F.col("customer_zip_code_prefix").cast("string"))
)
customer_output = f"s3a://{S3_BUCKET}/curated/dim_customer/"
df_dim_customer.write.mode("overwrite").parquet(customer_output)
print(f"dim_customer: {df_dim_customer.count():,} rows")

# 8. CURATED — DIM_SELLER
print("DIM_SELLER")

df_dim_seller = (
    df_sellers
    .select("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state")
    .dropDuplicates(["seller_id"])
    .withColumn("seller_zip_code_prefix", F.col("seller_zip_code_prefix").cast("string"))
)
seller_output = f"s3a://{S3_BUCKET}/curated/dim_seller/"
df_dim_seller.write.mode("overwrite").parquet(seller_output)
print(f"dim_seller: {df_dim_seller.count():,} rows")

# 9. CURATED — DIM_PRODUCT
print("DIM_PRODUCT")

df_dim_product = (
    df_products
    .select("product_id", "product_category_name", "product_name_lenght", "product_description_lenght", 
            "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm")
    .dropDuplicates(["product_id"])
    .withColumn("product_volume", F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm"))
)
product_output = f"s3a://{S3_BUCKET}/curated/dim_product/"
df_dim_product.write.mode("overwrite").parquet(product_output)
print(f"dim_product: {df_dim_product.count():,} rows")

# 10. CURATED — DIM_LOCATION
print("MODULE 4.6 — DIM_LOCATION")

df_geo_dedup = (
    df_geo
    .groupBy("geolocation_zip_code_prefix")
    .agg(
        F.avg("geolocation_lat").alias("latitude"),
        F.avg("geolocation_lng").alias("longitude"),
        F.first("geolocation_city", ignorenulls=True).alias("city"),
        F.first("geolocation_state", ignorenulls=True).alias("state")
    )
)
df_dim_location = (
    df_geo_dedup
    .withColumn("zip_code_prefix", F.col("geolocation_zip_code_prefix").cast("string"))
    .select("zip_code_prefix", "city", "state", "latitude", "longitude")
)
location_output = f"s3a://{S3_BUCKET}/curated/dim_location/"
df_dim_location.write.mode("overwrite").parquet(location_output)
print(f"dim_location: {df_dim_location.count():,} rows")

# 11. CURATED — FACT DELIVERY
print("MODULE 4.7 — FACT_DELIVERY")

df_items_agg = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.first("product_id").alias("product_id"),
        F.first("seller_id").alias("seller_id"),
        F.sum("price").alias("price"),
        F.sum("freight_value").alias("freight_value")
    )
)

df_fact = (
    df_orders.alias("o")
    .join(df_items_agg.alias("i"), F.col("o.order_id") == F.col("i.order_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.customer_id"), "left")
    .select(
        F.col("o.order_id"), F.col("o.customer_id"), F.col("i.product_id"), F.col("i.seller_id"),
        F.col("o.order_status"), F.col("o.order_purchase_timestamp"), F.col("o.order_approved_at"),
        F.col("o.order_delivered_carrier_date"), F.col("o.order_delivered_customer_date"),
        F.col("o.order_estimated_delivery_date"), F.col("i.price"), F.col("i.freight_value"),
        F.col("c.customer_zip_code_prefix").cast("string").alias("customer_zip_code_prefix")
    )
)

# Cast Timestamp
df_fact = (
    df_fact
    .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
    .withColumn("order_approved_at", F.to_timestamp("order_approved_at"))
    .withColumn("order_delivered_carrier_date", F.to_timestamp("order_delivered_carrier_date"))
    .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date"))
    .withColumn("order_estimated_delivery_date", F.to_timestamp("order_estimated_delivery_date"))
)

# Tính toán các chỉ số
df_fact = (
    df_fact
    .withColumn(
        "delivery_duration_hours",
        (F.col("order_delivered_customer_date").cast("long") - F.col("order_purchase_timestamp").cast("long")) / F.lit(3600.0)
    )
    .withColumn(
        "delay_days",
        F.datediff(F.to_date("order_delivered_customer_date"), F.to_date("order_estimated_delivery_date"))
    )
    .withColumn("is_delayed", F.when(F.col("delay_days") > 0, F.lit(1)).otherwise(F.lit(0)))
    .withColumn("date_sk", F.date_format(F.to_date("order_purchase_timestamp"), "yyyyMMdd"))
)

fact_output = f"s3a://{S3_BUCKET}/curated/fact_delivery/"
print(f"Ghi fact_delivery: {fact_output}")
df_fact.write.mode("overwrite").parquet(fact_output)
print(f"fact_delivery: {df_fact.count():,} rows")

# 12. FINAL 
print("Dữ liệu đã sẵn sàng cho BigQuery Data Warehouse.")

spark.stop()