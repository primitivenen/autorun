def main(sc):
    import pandas as pd

    from pyspark.sql import HiveContext

    # Hive context
    hiveContext = HiveContext(sc)

    #
    # Trips
    #
    # Import csv
    df = pd.read_csv('df-trips-2016-01-01.csv', index_col=0)

    if not df.empty:
        # Remove the problematic time-zone shift from dates
        df.loc[:,('end_time')] = df.loc[:,('end_time')].apply(lambda x: x.replace('+08:00',''))
        df.loc[:,('start_time')] = df.loc[:,('start_time')].apply(lambda x: x.replace('+08:00',''))

        # We are exporting a single partition corresponding to a single end_date
        # Check that the DF contains a single end date
        if len(df.end_date.unique()) != 1:
            raise Exception('DF should contain a unique end_date')
        del df['end_date']

        # Save DF into a temporary update_dataframe
        update_dataframe = hiveContext.createDataFrame(df)
        update_dataframe.registerTempTable('update_dataframe')

        # Show counts before exporting partition to Hive
        query = hiveContext.sql("""SELECT COUNT(*) FROM tsp_tbls.trips""")
        query.show()

        # Export DF as a partition
        hiveContext.sql("""INSERT OVERWRITE TABLE tsp_tbls.trips
                           PARTITION (end_date='2016-01-01')
                           SELECT * FROM update_dataframe""")

        # Show counts after exporting partition to Hive
        query = hiveContext.sql("""SELECT COUNT(*) FROM tsp_tbls.trips""")
        query.show()

    #
    # Charging
    #
    # Import csv
    df = pd.read_csv('df-charging-2016-01-01.csv', index_col=0)

    if not df.empty:
        # Remove the problematic time-zone shift from dates
        df.loc[:,('end_time')] = df.loc[:,('end_time')].apply(lambda x: x.replace('+08:00',''))
        df.loc[:,('start_time')] = df.loc[:,('start_time')].apply(lambda x: x.replace('+08:00',''))

        # We are exporting a single partition corresponding to a single end_date
        # Check that the DF contains a single end date
        if len(df.end_date.unique()) != 1:
            raise Exception('DF should contain a unique end_date')
        del df['end_date']

        # Save DF into a temporary update_dataframe
        update_dataframe = hiveContext.createDataFrame(df)
        update_dataframe.registerTempTable('update_dataframe')

        # Show counts before exporting partition to Hive
        query = hiveContext.sql("""SELECT COUNT(*) FROM tsp_tbls.charging""")
        query.show()

        # Export DF as a partition
        hiveContext.sql("""INSERT OVERWRITE TABLE tsp_tbls.charging
                           PARTITION (end_date='2016-01-01')
                           SELECT * FROM update_dataframe""")

        # Show counts after exporting partition to Hive
        query = hiveContext.sql("""SELECT COUNT(*) FROM tsp_tbls.charging""")
        query.show()

    #
    # Polarization
    #
    # Import csv
    df = pd.read_csv('df-polarization-2016-01-01.csv', index_col=0)

    if not df.empty:
        # Remove the problematic time-zone shift from dates
        df.loc[:,('end_time')] = df.loc[:,('end_time')].apply(lambda x: x.replace('+08:00',''))
        df.loc[:,('start_time')] = df.loc[:,('start_time')].apply(lambda x: x.replace('+08:00',''))

        # We are exporting a single partition corresponding to a single end_date
        # Check that the DF contains a single end date
        if len(df.end_date.unique()) != 1:
            raise Exception('DF should contain a unique end_date')
        del df['end_date']

        # Save DF into a temporary update_dataframe
        update_dataframe = hiveContext.createDataFrame(df)
        update_dataframe.registerTempTable('update_dataframe')

        # Show counts before exporting partition to Hive
        query = hiveContext.sql("""SELECT COUNT(*) FROM tsp_tbls.polarization""")
        query.show()

        # Export DF as a partition
        hiveContext.sql("""INSERT OVERWRITE TABLE tsp_tbls.polarization
                           PARTITION (end_date='2016-01-01')
                           SELECT * FROM update_dataframe""")

        # Show counts after exporting partition to Hive
        query = hiveContext.sql("""SELECT COUNT(*) FROM tsp_tbls.polarization""")
        query.show()

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
    sc = spark.sparkContext

    # Execute Main functionality
    main(sc)
