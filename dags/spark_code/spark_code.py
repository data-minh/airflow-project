from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .master('local') \
    .appName('MyApp') \
    .config("spark.jars.packages", 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .config('spark.mongodb.input.uri', 'mongodb://root:root@mongodb/dep303x?authSource=admin') \
    .config('spark.mongodb.output.uri', 'mongodb://root:root@mongodb/dep303x?authSource=admin') \
    .getOrCreate()

# Tạo dataframe questions
df_answers = spark.read\
        .format('com.mongodb.spark.sql.DefaultSource')\
        .option("collection", "answers")\
        .option("inferSchema", "true")\
        .load()

# Lấy dữ liệu từ data answers Id, ParentId
data_answers = df_answers.select("ParentId")

# Xử lý dữ liệu
data_answers = data_answers.groupBy("ParentID").count() \
                            .withColumnRenamed("ParentID", "Id") \
                            .withColumnRenamed("count(ParentID)", "Number of answers") \
                            .orderBy("Id")

# Đường dẫn đến tệp CSV đầu ra
output_path = '/opt/airflow/data/data_asm2/out_put'

# Lưu DataFrame thành tệp CSV
data_answers.write \
            .format("csv") \
            .mode("overwrite") \
            .option("path", output_path) \
            .option("header", "true") \
            .save()