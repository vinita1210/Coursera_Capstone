from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
from pyspark.sql import functions as F


Checkpoint 1 :

1)
schema = StructType([StructField("date", StringType(), True),\
StructField("registrar", StringType(), True),\
StructField("private_agency", StringType(), True),\
StructField("state", StringType(), True),\
StructField("district", StringType(), True),\
StructField("sub_district", StringType(), True),\
StructField("pincode", StringType(), True),\
StructField("gender", StringType(), True),\
StructField("age", StringType(), True),\
StructField("aadhar_generated", StringType(), True),\
StructField("rejected", StringType(), True),\
StructField("mobile_number", StringType(), True),\
StructField("email_id", StringType(), True)])

df = spark.read.format("csv")\
.option("header", "false")\
.schema(schema)\
.load("G:\Study/aadhaar_data.csv")

2)
window = Window.partitionBy('private_agency').orderBy(df['private_agency'].desc())
df.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 25).show()
 
 
Checkpoint 2 :
 
1) df.printSchema()

2) df.groupBy('registrar').agg(F.count('registrar')).show()

window1 = Window.partitionBy('registrar').orderBy(df['registrar'].desc())  
df.select('*', rank().over(window1)).count()

3) **Doubt**
df1 = df.groupBy('state','district','sub_district').agg(F.count('*'))
df2 = df1.withColumn('ccol',F.concat(df1['district'],F.lit('_cnt'))).groupby('state','district').agg(F.count('ccol'))


df.groupBy('state').agg(F.count('district')).show()

4)
window = Window.partitionBy('state').orderBy(df['state'].desc())
df1 = df.select("state","private_agency",rank().over(window)).show()


Checkpoint 3 :

1) df.groupBy("state").agg(F.sum("aadhar_generated").alias('num')).sort(desc('num')).show(3,False)

2) df.groupBy("district").agg(F.sum("aadhar_generated").alias('num')).sort(desc('num')).show(3,False)

3) df1 = df.groupBy("state").agg(F.sum("aadhar_generated").alias('accepted'),F.sum("rejected").alias('reject'))
   df1= df1.withColumn("aadhar", (F.col("accepted") - F.col("reject")))
   df1.select("state","aadhar").show()
   
Checkpoint 4 :

1) df.select("state","pincode").groupBy("state").agg(F.countDistinct("pincode").alias('count')).filter(col('count') > 1).show()

2) df.select("state","rejected").groupBy("state").agg(F.sum('rejected').alias('sumOfReject')).where(col("state").isin({"Uttar Pradesh","Maharashtra"})).show()
