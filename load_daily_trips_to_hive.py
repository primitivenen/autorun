# date: 2018-03-23
# author: Shuai Tang (stang@gacrndusa.com)
# version: 0.1

from vehicle import *
from veh_stats import *
from datetime import datetime, date
import subprocess
import findspark
import sys

# spark location on namenode server
findspark.init("/usr/hdp/current/spark2-client")
import pyspark
from pyspark.sql import HiveContext

# configs
conf = pyspark.SparkConf().setAll([('spark.app.name', 'export_trip_to_hive'),
                                    ('spark.master', 'local'),
                                    ('spark.executor.memory', '10g'),
                                    ('spark.memory.fraction', '0.7'),
                                    ('spark.executor.cores', '3')])

sc = pyspark.SparkContext(conf=conf)
hc = HiveContext(sc)

files = sorted([f for f in os.listdir('trip_stats') if '.done' not in f])

for f in files:
    f = 'trip_stats/' + f
    print(f)
    if os.path.exists(f + '.done'):
        print('{} exported already, skipping'.format(f))
        continue
        
    df = pd.read_csv(f)
    df['date'] = df.apply(lambda row : row['stime'].split()[0], axis=1)
    df['stime'] = df.apply(lambda row: row['stime'].split()[1].split('+')[0], axis=1)
    df['etime'] = df.apply(lambda row: row['etime'].split()[1].split('+')[0], axis=1)
    df = df[['vin', 'date', 'stime', 'etime', 'slat', 'slon', 'elat', 'elon', 'dist', 'dura_sec', 'duration']]
    partition_col = 'date'
    df.drop(labels=partition_col, axis=1, inplace=True)
    
    spark_df = hc.createDataFrame(df)
    spark_df.registerTempTable('update_dataframe')
    day = f.split('.')[0].split('_')[-1]
    day = pd.to_datetime(str(day)).date().strftime('%Y-%m-%d')
    
    sql_cmd = """SELECT COUNT(*) FROM tsp_tbls.daily_trips
                  WHERE {}='{}' """.format(partition_col, day)
    print(sql_cmd)
    hc.sql(sql_cmd).show()

    sql_cmd = """INSERT OVERWRITE TABLE tsp_tbls.daily_trips
              PARTITION ({}='{}')
              SELECT * FROM update_dataframe""".format(partition_col, day)
    print(sql_cmd)
    hc.sql(sql_cmd)

    sql_cmd = """SELECT COUNT(*) FROM tsp_tbls.daily_trips
              WHERE {}='{}' """.format(partition_col, day)
    print(sql_cmd)
    hc.sql(sql_cmd).show()
        
    ts = pd.to_datetime(datetime.now())
    print('{}: {} has been exported to Hive (overwrite mode)'.format(ts, f))
    # write a marker file
    with open(f + '.done', 'w') as outf:
        pass
sc.stop()
print('done.')
