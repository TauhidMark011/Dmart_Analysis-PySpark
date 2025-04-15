# scripts/clean_transform.py 
from pyspark.sql.functions import col

def clean_data(customer_df, product_df, sales_df):
    # ✅ Rename actual columns (verified from your CSV)
    customer_df = customer_df.withColumnRenamed("Customer ID", "Customer_ID")
    product_df = product_df.withColumnRenamed("Product ID", "Product_ID")
    sales_df = sales_df.withColumnRenamed("Product ID", "Product_ID")
    sales_df = sales_df.withColumnRenamed("Customer ID", "Customer_ID")

    # ✅ Drop missing values from customer data
    customer_df = customer_df.dropna()

    # ✅ Fill missing discounts in sales with 0
    sales_df = sales_df.fillna({"Discount": 0})

    # ✅ Convert data types
    sales_df = sales_df.withColumn("Quantity", col("Quantity").cast("int"))
    sales_df = sales_df.withColumn("Sales", col("Sales").cast("double"))
    sales_df = sales_df.withColumn("Profit", col("Profit").cast("double"))

    # ✅ Join all datasets
    joined_df = sales_df.join(product_df, on="Product_ID", how="inner") \
                        .join(customer_df, on="Customer_ID", how="inner")

    return joined_df
