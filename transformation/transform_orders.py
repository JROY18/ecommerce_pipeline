import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, datediff, to_timestamp, round, avg, count, sum, date_format)

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += ";C:\\hadoop\\bin"

spark = SparkSession.builder\
        .appName("EcEcommerce Pipeline Transformation")\
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR") #only for errors

print("Spark Session Started!")

print("Loading CSV files")

orders = spark.read.csv("data/olist_orders_dataset.csv", header = True, inferSchema = True)
order_items = spark.read.csv("data/olist_order_items_dataset.csv", header=True, inferSchema=True)
customers = spark.read.csv("data/olist_customers_dataset.csv", header=True, inferSchema=True)
payments = spark.read.csv("data/olist_order_payments_dataset.csv", header=True, inferSchema=True)

print("All files loaded!")

print("Join all tables")

df = orders\
    .join(order_items, on = "order_id", how = "left")\
    .join(customers, on = "customer_id", how  = "left")\
    .join(payments, on = "order_id", how = "left")

print(f"Joined table shape: {df.count()} rows, {len(df.columns)} columns")

print("Cleaning Data")

df = df.withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp")))\
       .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date")))\
       .withColumn("order_estimated_delivery_date", to_timestamp(col("order_estimated_delivery_date")))

df = df.withColumn("delivery_time_days", datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp")))

df = df.withColumn("total_order_value", round(col("price") + col("freight_value") ,2))  

print("Cleaning done!")

# Business metrics

# Revenue by state

revenue_by_state = df.groupBy("customer_state")\
                .agg(round(sum("payment_value"),2).alias("total_revenue"), count("order_id").alias("total_orders"))\
                .orderBy("total_revenue", ascending = False)

# 2. Average delivery time

avg_delivery = df.filter(col("delivery_time_days").isNotNull())\
                 .agg(round(avg("delivery_time_days"), 2).alias("avg_delivery_days"))

# 3. Revenue by payment 

revenue_by_payment = df.groupBy("payment_type") \
    .agg(round(sum("payment_value"), 2).alias("total_revenue"),
         count("order_id").alias("total_orders")) \
    .orderBy("total_revenue", ascending=False)


# 4. Monthly Revenue

monthly_revenue = df.groupBy(date_format("order_purchase_timestamp", "yyyy-MM").alias("month")).agg(round(sum("payment_value"), 2).alias("total_revenue")) \
    .orderBy("month")

# Actions

revenue_by_state.show()
avg_delivery.show()
revenue_by_payment.show()
monthly_revenue.show()

print("Saving results...")

output_path = "data/curated/"

revenue_by_state.toPandas().to_csv(f"{output_path}revenue_by_state.csv", index=False)
revenue_by_payment.toPandas().to_csv(f"{output_path}revenue_by_payment.csv", index=False)
monthly_revenue.toPandas().to_csv(f"{output_path}monthly_revenue.csv", index=False)

print("All results saved to data/curated/")
print("Transformation complete!")

spark.stop()













