import sys
import os
# Add the root folder to the system path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from scripts.pyspark_connection import create_spark_sessions
from scripts.load_data import load_data
from scripts.clean_transform import clean_data
from scripts.analysis_queries import run_queries

def export_data(final_df):
    """Saves the final transformed data to disk"""
    # 1. Exporting to Parquet (Interview Favorite: Columnar storage)
    print("Exporting data to Parquet...")
    final_df.write.mode("overwrite").parquet("output/processed_dmart_data.parquet")
    
    # 2. Exporting to CSV (For Excel/Reporting)
    print("Exporting data to CSV...")
    final_df.write.mode("overwrite").option("header", "true").csv("output/processed_dmart_data.csv")
    
    print("Files saved successfully in the 'output' folder!")

def main():
    # Step 1: Create a PySpark session
    spark = create_spark_sessions()

    try:
        # Step 2: Load
        print("Loading CSV files...")
        customer_df, product_df, sales_df = load_data(spark)

        # Step 3: Clean & Transform
        print("Cleaning and transforming data...")
        joined_df = clean_data(customer_df, product_df, sales_df)

        # Step 4: Run Analysis
        print("Running Analytical Queries...")
        run_queries(joined_df)

        # Step 5: Export (CRITICAL: Added this call)
        export_data(joined_df)

    finally:
        # Step 6: Stop the Spark session (Always stop to release memory)
        print("Closing Spark Session...")
        spark.stop()

if __name__ == "__main__":
    main()