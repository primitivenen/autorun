import pandas as pd

# Import csv
df1 = pd.read_csv('outputs/df-2017-12-29.csv', index_col=0)
df2 = pd.read_csv('outputs/df-2017-12-30.csv', index_col=0)

# Build spark session
import findspark  # find spark home directory
findspark.init('/usr/hdp/current/spark2-client')
#import pyspark
#conf = pyspark.SparkConf().setAll([('spark.app.name', 'daily_stats_run'), # App Name
#('spark.master', 'yarn'),              # spark run mode: locally or remotely
#('spark.submit.deployMode', 'client'), # deploy in yarn-client or yarn-cluster
#('spark.executor.memory', '8g'),       # memory allocated for each executor
#('spark.executor.cores', '3'),         # number of cores for each executor
#('spark.executor.instances', '60'),    # number of executors in total
#('spark.yarn.am.memory','20g')])       # memory for spark driver (application master)
#sc = pyspark.SparkContext(conf=conf)

# Build the SparkSession
from pyspark.sql import SparkSession

# Initialise Hive metastore

spark = SparkSession.builder \
   .master("local") \
   .appName("DF2HIVE") \
   .enableHiveSupport() \
   .config("spark.executor.memory", "1gb") \
   .getOrCreate()
sc = spark.sparkContext


#from pyspark.sql import SQLContext
#sqlContext = SQLContext(sc)
from pyspark.sql import HiveContext

sqlContext = HiveContext(sc)

# Convert to a Spark dataframe
spark_df1 = sqlContext.createDataFrame(df1)
spark_df2 = sqlContext.createDataFrame(df2)

# Write the Spark dataframe as a Hive table
tablename = 'trips_db'
schema = 'default.' 
#spark_df1.write.partitionBy('end_date').option("path","/home/trangel/trips_db").mode('Overwrite').saveAsTable(schema + tablename)
spark_df1.write.partitionBy('end_date').mode('Overwrite').saveAsTable(schema + tablename)


query = sqlContext.sql(""" SELECT * FROM trips_db""")
query.show()

query = sqlContext.sql(""" SELECT DISTINCT(end_date) FROM trips_db""")
query.show()

query = sqlContext.sql(""" SELECT COUNT(*) FROM trips_db""")
query.show()

# Append more rows
spark_df2.write.partitionBy('end_date').mode('Append').saveAsTable(schema + tablename)

query = sqlContext.sql(""" SELECT DISTINCT(end_date) FROM trips_db""")
query.show()

query = sqlContext.sql(""" SELECT COUNT(*) FROM trips_db""")
query.show()
