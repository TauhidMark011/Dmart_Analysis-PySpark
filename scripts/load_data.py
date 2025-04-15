#Function to load CSV files into PySpark DataFrames
def load_data(spark):
    #Load Customer data
    customer_df = spark.read.csv("D:/Customer.csv", header = True, inferSchema = True)
    #Load Product data
    product_df = spark.read.csv("D:/Product.csv", header = True, inferSchema = True)
    #Load Sales Data 
    sales_df = spark.read.csv("D:/Sales.csv", header = True, inferSchema = True)
    #Return all Data Frames
    return customer_df, product_df, sales_df