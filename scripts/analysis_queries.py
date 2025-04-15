#Function to run all analytical queries on the final dataset
from pyspark.sql.functions import countDistinct
def run_queries(final_df):
#1.Total sales for each product category
    print("\n Total Sales per Product Category: ")
    final_df.groupBy("Category").sum("Sales").show()
#2.Customer with highest number of purchases
    print("\n Customer with Highest Number of Purchases:")
    final_df.groupBy("Customer_ID").count().orderBy("count", ascending = False).show(1)
#3. Average discount on sales
    print("\n Average Discount on Sales:")
    final_df.selectExpr("avg(Discount) as Average_Discount").show()

# #4. Unique products sold in each region
    final_df.groupBy("Region") \
    .agg(countDistinct("Product_ID").alias("Unique_Products")) \
    .show()

#5. Total profit generated per state
    print("\n Total Profit per State:")
    final_df.groupBy("State").sum("Profit").withColumnRenamed("sum(Profit)", "Total_Profit").show()

#6. Product sub-category with highest sales
    print("\n Product Sub-Category with Highest Sales:")
    final_df.groupBy("Sub-Category").sum("Sales").orderBy("sum(Sales)", ascending=False).show(1)

#7. Average age per customer segment
    print("\n Average Age per Customer Segment:")
    final_df.groupBy("Segment").avg("Age").withColumnRenamed("avg(Age)", "Average_Age").show()

#8. Orders shipped in each shipping mode
    print("\n Orders per Shipping Mode:")
    final_df.groupBy("Ship Mode").count().withColumnRenamed("count", "Order_Count").show()

#9. Total quantity sold per city
    print("\n Total Quantity Sold per City:")
    final_df.groupBy("City").sum("Quantity").withColumnRenamed("sum(Quantity)", "Total_Quantity").show()

#10. Customer segment with highest profit margin
    print("\n Customer Segment with Highest Total Profit:")
    final_df.groupBy("Segment").sum("Profit").withColumnRenamed("sum(Profit)", "Total_Profit").orderBy("Total_Profit", ascending=False).show(1)
