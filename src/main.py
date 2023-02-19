import os


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
from pyspark.sql.window import Window
from pyspark.sql.functions import max, row_number, col

source_path = os.getenv('SOURCE_PATH')
out_path = os.getenv('OUT_PATH')


spark = SparkSession \
	.builder \
	.appName("The most popular product from customers") \
	.master("local[*]") \
	.getOrCreate()

schema_customer = StructType([ \
		StructField("id", IntegerType(), False), \
		StructField("name", StringType(), True), \
		StructField("email", StringType(), True), \
		StructField("joinDate", DateType(), True), \
		StructField("status", StringType(), True), \
	])

schema_product = StructType([ \
		StructField("id", IntegerType(), False), \
		StructField("name", StringType(), True), \
		StructField("price", DoubleType(), True), \
		StructField("numberOfProducts", IntegerType(), True), \
	])

schema_order = StructType([ \
		StructField("customerID", IntegerType(), False), \
		StructField("orderID", IntegerType(), False), \
		StructField("productID", IntegerType(), False), \
		StructField("numberOfProduct", IntegerType(), True), \
		StructField("orderDate", DateType(), True), \
		StructField("status", StringType(), True), \
	])


df_customer = spark.read \
	.options(
		header=False, 
		delimiter='\t') \
	.schema(schema_customer) \
	.csv(os.path.join(source_path, 'customer.csv'))

df_product = spark.read \
	.options(
		header=False, 
		delimiter='\t') \
	.schema(schema_product) \
	.csv(os.path.join(source_path, 'product.csv'))

df_order = spark.read \
	.options(
		header=False, 
		delimiter='\t') \
	.schema(schema_order) \
	.csv(os.path.join(source_path, 'order.csv'))


df_customer_filtered = df_customer
df_product_filtered = df_product
df_order_filtered = df_order.filter((df_order['status'] != 'canceled') & (df_order['status'] != 'error'))


window_customerId = Window.partitionBy("customeId")

df_num_products_per_customers = df_customer_filtered \
	.join(df_order_filtered, df_customer_filtered['id'] == df_order_filtered['customerID']) \
	.join(df_product_filtered, df_order_filtered['productID'] == df_product_filtered['id']) \
	.select(
		df_product_filtered['id'].alias('productID'),
		df_customer_filtered['id'].alias('customeId'), 
		df_product_filtered['name'].alias('productName'),
		df_customer_filtered['name'].alias('customerName'), 
		df_order_filtered['numberOfProduct'],
	) \
	.groupBy('customeId', 'customerName', 'productID', 'productName') \
	.sum('numberOfProduct').withColumnRenamed('sum(numberOfProduct)', 'num_product') \
	.withColumn("max_num_product", max('num_product').over(window_customerId)) \
	.filter('num_product == max_num_product') \
	.select(
		col('customerName'),
		col('productName'),\
		) \
	.orderBy('customerName')


window_customerId_productName = Window.partitionBy("customerName").orderBy('productName')

report = df_num_products_per_customers \
	.withColumn("unique_filter", row_number().over(window_customerId_productName)) \
	.filter("unique_filter == 1") \
	.select(
		col('customerName'),
		col('productName'),
	)

report.show()

pd = report.toPandas()
pd.to_csv(
	os.path.join(out_path, 'report.csv'), 
	index=False, 
	header=False,
	sep='\t',
	)
