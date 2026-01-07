from pyspark.sql import SparkSession

# Tạo SparkSession
spark = SparkSession.builder.master("spark://spark-master:7077").appName("DemoApp").getOrCreate()

# Đọc file CSV
#df_csv = spark.read.option("header", True).csv("file:///app/orders.csv")

# Hiển thị dữ liệu
df_csv.show()

# Ghi dữ liệu dưới dạng Parquet
#output_path = "file:///app/output/orders_parquet"
#df_csv.write.mode("overwrite").parquet(output_path)

#print(f"✅ File Parquet đã được lưu tại: {output_path}")

# Dừng SparkSession
spark.stop()
