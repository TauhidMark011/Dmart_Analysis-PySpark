# scripts/clean_transform.py 
from pyspark.sql.functions import col

def clean_data(customer_df, product_df, sales_df):
    #Rename columns for consistency and to remove spaces
    #This helps avoid errors during transformations and joins
    customer_df = customer_df.withColumnRenamed("Customer ID", "Customer_ID")
    product_df = product_df.withColumnRenamed("Product ID", "Product_ID")
    sales_df = sales_df.withColumnRenamed("Product ID", "Product_ID")
    sales_df = sales_df.withColumnRenamed("Customer ID", "Customer_ID")

    #Removes any rows in customer_df that contain null (missing) values on customer dataset 
    # Ensures clean and reliable customer information
    customer_df = customer_df.dropna()

     #Fill missing Discount values with 0 in the sales dataset
    # This prevents issues during calculations and avoids NULL errors
    sales_df = sales_df.fillna({"Discount": 0})

    #Convert data types
    #Ensures numerical operations can be performed without issues
    sales_df = sales_df.withColumn("Quantity", col("Quantity").cast("int"))
    sales_df = sales_df.withColumn("Sales", col("Sales").cast("double"))
    sales_df = sales_df.withColumn("Profit", col("Profit").cast("double"))

    #Join all datasets
    #Combines product, customer, and sales information into a unified DataFrame
    joined_df = sales_df.join(product_df, on="Product_ID", how="inner") \
                        .join(customer_df, on="Customer_ID", how="inner")
    #inner ensures only those sales with matching products are kept.

    return joined_df
