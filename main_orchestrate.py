import sys
import os
#Add the root folder to the system path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
# main_orchestrate.py
# Import all scripts
from scripts.pyspark_connection import create_spark_sessions
from scripts.load_data import load_data
from scripts.clean_transform import clean_data
from scripts.analysis_queries import run_queries

# Main function to execute the pipeline
def main():
    # Step 1: Create a PySpark session
    spark = create_spark_sessions()

    # Step 2: Load CSV files into DataFrames
    print("Loading CSV files...")
    customer_df, product_df, sales_df = load_data(spark)

    # Step 3: Clean and transform the data
    print("Cleaning and transforming data...")
    joined_df = clean_data(customer_df, product_df, sales_df)
    # Step 4: Join the DataFrames
    print("Joining DataFrames...")
    # final_df = join_data(customer_df, product_df, sales_df)

    # Step 5: Run analysis queries
    print("Running Analytical Queries...")
    run_queries(joined_df)

    # Stop the Spark session
    spark.stop()

# Entry point
if __name__ == "__main__":
    main()