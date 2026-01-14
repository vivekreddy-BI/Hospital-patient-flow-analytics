from pyspark.sql.functions import *
from pyspark.sql.types import *

#ADLS configuration 
spark.conf.set(
  "fs.azure.account.key.<<Storageaccount_name>>.dfs.core.windows.net",
  <<Storageaccount_accesskey>>
)


bronze_path = "abfss://<<Container>>@<<Storageaccount_name>>.dfs.core.windows.net/<<path>>"
Silver_path = "abfss://<<Container>>@<<Storageaccount_name>>.dfs.core.windows.net/<<path>>"

#read from bronze
bronze_df = spark.readStream.format("delta").load(bronze_path)

#define Schema
schema = StructType([
    StructField("patient_id", StringType()),
    StructField("gender", StringType()),
    StructField("age", IntegerType()),
    StructField("department", StringType()),
    StructField("admission_time", StringType()),
    StructField("discharge_time", StringType()),
    StructField("bed_id", IntegerType()),
    StructField("hospital_id", IntegerType())]    
                    )

#Parse it to df
parsed_df = bronze_df.withColumn("data",from_json(col("raw_json"),schema)).select("data.*")

#convert type to timestamp
clean_df = parsed_df.withColumn("admission_time",to_timestamp("admission_time"))
clean_df = clean_df.withColumn("discharge_time",to_timestamp("discharge_time"))

#invalid Admission times
clean_df = clean_df.withColumn("admission_time"
                               ,when(
                                   col("admission_time").isNull() | (col("admission_time") > current_timestamp()),
                                     current_timestamp())
                                     .otherwise(col("admission_time")))

#invalid Age
clean_df = clean_df.withColumn("age",
                               when(col("age")>100,
                                    floor(rand()*90+1).cast("int"))
                                    .otherwise(col("age"))
                               )

#schema evolution
expected_col = ["patient_id","gender","age","department","admission_time","discharge_time","bed_id","hospital_id"]

for col_name in expected_col:
  if col_name not in clean_df.columns:
    clean_df = clean_df.withColumn(col_name,lit(None))


#write to silver table
(
    clean_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("meargeSchema","true")
    .option("checkpointLocation", Silver_path + "/_checkpoints")
    .start(Silver_path)
)



