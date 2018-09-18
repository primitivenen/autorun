def main(sc, overwrite):
    import pandas as pd
    from pyspark.sql import HiveContext

    # Hive context
    hc = HiveContext(sc)

    # Get last day of data from daily stats
    query = 'SELECT MAX(day) AS max_day FROM guobiao_tsp_tbls.daily_stats'
    last_day_stats = hive2string(hc, query, colname='max_day')
    print('Last day of data in daily_stats', last_day_stats)

    # Get last day of data from vin_rank_cell_volt_diff
    try:
        query = 'SELECT MAX(last_daily_stats_day) AS last_daily_stats_day FROM \
        guobiao_tsp_tbls.vin_rank_by_cell_volt_diff'
        last_day_ranking = hive2string(hc, query, colname='last_daily_stats_day')
        print('Last day of data in vin_ranking', last_day_ranking)
    except Exception as e:
        print(e)
        last_day_ranking = '2018-01-01'
        pass

    # If last days differ, then update table
    if (last_day_stats != last_day_ranking) or (overwrite):
        # Drop table
        print('Drop table')
        sql = 'DROP TABLE IF EXISTS guobiao_tsp_tbls.vin_rank_by_cell_volt_diff PURGE' 
        res = hc.sql("""{}""".format(sql))

        # Create table
        with open("/home/trangel/hive/update-vin-rank-by-cell-volt-diff.sql", "r") as f:
            query = f.read()
        query = query.replace('LAST_DAY_STATS', '\"{}\"'.format(last_day_stats))
        print(query)
        res = hc.sql("""{}""".format(query))

        # Counts
        query = "SELECT COUNT(vin) FROM guobiao_tsp_tbls.vin_rank_by_cell_volt_diff"
        res = hc.sql("""{}""".format(query))
        print('Counts', res.first())
    else:
        print('Table is up to date, no need to update')

def hive2string(hc, query, colname):
    spark_df = hc.sql("""{}""".format(query))
    row = spark_df.first()
    res = row[colname]
    return res

def hive2csv(hc, query, outfile):
    spark_df = hc.sql("""{}""".format(query))
    # Convert to pandas dataframe
    df = spark_df.toPandas()
    df.to_csv(outfile)

if __name__ == "__main__":
    # Build spark session
    import findspark 
    findspark.init('/usr/hdp/current/spark2-client')
    # Build the SparkSession
    from pyspark.sql import SparkSession

    # Initialize Hive metastore
    spark = SparkSession.builder \
       .master("local") \
       .appName("DF2HIVE") \
       .enableHiveSupport() \
       .config("spark.executor.memory", "1gb") \
       .getOrCreate()

    overwrite = True
    sc = spark.sparkContext

    main(sc, overwrite)
