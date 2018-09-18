import pandas as pd
import numpy as np

TABLE_NAME = 'guobiao_tsp_tbls.vin_ranking'

def export_to_hive(hc, df, end_date):
    """
    Exports dataframe to hive

    Arguments
    ---------
    hc, hive Context
    df, pandas
    end_date, date, partition
    """
    from pyspark.sql.functions import col, when

    # Save DF into a temporary update_dataframe
    spark_df = hc.createDataFrame(df)

    # Convert 'NaN' values to NULL
    cols = [when(~col(x).isin("NULL", "NA", "NaN"), col(x)).alias(x) for
            x in spark_df.columns]

    print('Update dataframe')
    update_dataframe = spark_df.select(*cols)

    update_dataframe.registerTempTable('update_dataframe')

    # Show counts before exporting partition to Hive
    print("Partition counts before exporting partition\n")
    query = hc.sql("""SELECT COUNT(*) FROM {}""".format(TABLE_NAME))
    query.show()

    # Export DF as a partition
    print('Exporting partition {} to database {}\n'.format(TABLE_NAME, end_date))
    hc.sql("""INSERT OVERWRITE TABLE {}
    	PARTITION (end_date='{}')
    	SELECT
            battery_temp_max_charging, 
            battery_temp_max_driving, 
            battery_temp_mean_charging,
            battery_temp_mean_driving,
            battery_temp_min_charging,
            battery_temp_min_driving,
            cell_volt_diff_mean_charging, 
            cell_volt_diff_mean_driving, 
            normalized_ah_charged_mean,
            normalized_ah_consumption_mean,
            normalized_distance_driven_mean,
            vin
    	FROM update_dataframe""".format(TABLE_NAME, end_date))

    # Show counts after exporting partition to Hive
    print("Partition counts after exporting partition\n")
    query = hc.sql("""SELECT COUNT(*) FROM {}""".format(TABLE_NAME))
    query.show()


def rename_filter_columns(df):
    """
    Selects columns of interest and renames the columns
    """

    # Replace min_min, max_max, and avg_mean by just min, max or mean
    df.columns = [x.replace('min_min', 'min') for x in df.columns]
    df.columns = [x.replace('max_max', 'max') for x in df.columns]
    df.columns = [x.replace('avg_mean', 'mean') for x in df.columns]
    df.columns = [x.replace('wmean', 'mean') for x in df.columns]
    df.columns = [x.replace('mean_mean', 'mean') for x in df.columns]
    # Other naming conventions
    df.columns = [x.replace('trips', 'driving') for x in df.columns]
    df.columns = [x.replace('ah_throughput', 'ah_charged') for x in df.columns]

    # Columns to select
    cols = ['vin', 'battery_temp_min_driving',
            'battery_temp_max_driving', 'battery_temp_mean_driving',
            'cell_volt_diff_mean_driving', 'battery_temp_min_charging',
            'battery_temp_max_charging', 'battery_temp_mean_charging',
            'cell_volt_diff_mean_charging', 'normalized_ah_charged_mean',
            'normalized_ah_consumption_mean', 'normalized_distance_driven_mean']

    return df[cols]

def merge_dfs(dfs):
    """
    Merge dataframes
    
    Arguments:
    dfs = [df_charging_by_vin, df_trips_by_vin, df_charging_full_soc_by_vin,
           df_trips_full_soc_by_vin]

    Output
    df, pandas dataframe
    """
    # Join trips (dfs[0]) and charging (dfs[1])
    df = dfs[0].merge(dfs[1], on=['vin'], how='outer', suffixes=['_trips', '_charging'])

    # Join charging_full_soc
    df = df.merge(dfs[2], on=['vin'], how='left', suffixes=['', '_charging_full_soc'])

    # Join trips_full_soc
    df = df.merge(dfs[3], on=['vin'], how='left', suffixes=['', '_trips_full_soc'])

    return df

def apply_min_count_periods_filter(dfs, min_count_periods):
    """
    Applys filter on min_count_periods
    Arguments:
    dfs, list of pandas DFs
    Output:
    dfs, list of pandas DFs
    """
    dfs2 = []
    for df in dfs:
        # Replace '*_count' for 'count' in column names
        df.columns = ['count' if ('count' in x) else x for x in df.columns]
        # Apply filter
        df2 = df.query('count > {}'.format(min_count_periods)).copy()
        dfs2.append(df2)
    return dfs2


def get_end_date(hc):
    """
    Gets end day from trips_on_battery table
    """
    query = 'SELECT MAX(day) AS max_day FROM guobiao_tsp_tbls.trips_on_battery'
    end_date = hive2string(hc, query, colname='max_day')
    print('Last day of data', end_date)
    return end_date

def wmean(series):
    """
    Gets weighted average of a pandas series based on order
    """
    series.reset_index(drop=True, inplace=True)
    # Weight goes from 0.5 to 1.0, giving higher weight to newer samples
    weights = (series.index.values + 1) / float(len(series)) / 2.0 + 0.5
    avg = np.average(series.values, weights=weights)
    return avg

def flatten_index_cols(df):
    """
    Flattens index and cols of a pandas dataframe
    """
    df.columns = df.columns.map('_'.join)
    df.reset_index(inplace=True)

def aggregate_by_vin_trips_n_charging(df):
    """
    Aggregates by vin 
    For trips_on_battery and charging DFs
    """
    # Group by vin and get aggregations
    df.sort_values(by=['vin', 'end_time'], inplace=True, ascending=True)
    df.reset_index(drop=True, inplace=True)
    df_by_vin = df.groupby('vin').agg({'cell_volt_diff_mean': [wmean, 'count'], 
                                       'battery_temp_min': ['min'],
                                       'battery_temp_max' : ['max'],
                                       'battery_temp_avg' : ['mean']})
    flatten_index_cols(df_by_vin)
    return df_by_vin


def aggregate_by_vin_trips_full_soc(df):
    """
    Aggregates by vin trips_full_soc table
    """
    df.sort_values(by=['vin', 'end_time'], inplace=True, ascending=True)
    df.reset_index(drop=True, inplace=True)
    df_by_vin = df.groupby('vin').agg({'normalized_ah_consumption': [wmean,
        'count'], 'normalized_distance_driven' : [wmean]}) 
    flatten_index_cols(df_by_vin)
    return df_by_vin

def aggregate_by_vin_charging_full_soc(df):
    """
    Aggregates by vin charging_full_soc table
    """
    df.sort_values(by=['vin', 'end_time'], inplace=True, ascending=True)
    df.reset_index(drop=True, inplace=True)
    df_by_vin = df.groupby('vin').agg({'normalized_ah_throughput': [wmean,
        'count']}) 
    flatten_index_cols(df_by_vin)
    return df_by_vin

def main(sc, day_range, min_count_records, min_count_periods):
    from pyspark.sql import HiveContext

    # Hive context
    hc = HiveContext(sc)

    # Query templates:
    vin_condition = 'vin IN (SELECT vin FROM guobiao_tsp_tbls.vintypes WHERE vintype = \'A5HEV\')'
    day_condition = 'day > date_sub(\'END_DAY\', {})'.format(day_range)
    count_condition = 'count_records > {}'.format(min_count_records)
    limit_condition = 'LIMIT 1000' #use only for debugging

    #
    # 1. Get last day from trips on battery
    #
    end_date = get_end_date(hc)
    day_condition = day_condition.replace('END_DAY', str(end_date))

    # Query template for trips and charging tables
    query1 = 'SELECT vin, end_time, cell_volt_diff_mean, battery_temp_avg,\
        battery_temp_min, battery_temp_max FROM TABLE WHERE {} AND {} AND {}'.format(vin_condition, 
        day_condition, count_condition, limit_condition)
    #
    # 2. Query charging table
    #
    print('Charging table')
    query = query1.replace('TABLE', 'guobiao_tsp_tbls.charging')
    #print(query)
    df_charging = hive2pandas(hc, query)
    df_charging_by_vin = aggregate_by_vin_trips_n_charging(df_charging)
    #df_charging_by_vin.to_csv('test_charging.csv')
    
    #
    # 3. Query trips on battery table
    #
    print('Querying trips on battery table')
    query = query1.replace('TABLE', 'guobiao_tsp_tbls.trips_on_battery') 
    #print(query)
    df_trips = hive2pandas(hc, query)
    df_trips_by_vin = aggregate_by_vin_trips_n_charging(df_trips)
    #df_trips_by_vin.to_csv('test_trips_on_battery.csv')

    #
    # 4. Query trips full soc table
    #
    print('Querying trips full soc table')
    query = 'SELECT vin, end_time, normalized_ah_consumption,\
        normalized_distance_driven  FROM guobiao_tsp_tbls.trips_full_soc\
        WHERE {} AND {} {}'.format(vin_condition, day_condition, limit_condition)
    #print(query)
    df_trips_full_soc = hive2pandas(hc, query)
    df_trips_full_soc_by_vin = aggregate_by_vin_trips_full_soc(df_trips_full_soc)
    #df_trips_full_soc_by_vin.to_csv('test_trips_full_soc.csv')

    #
    # 5. Query charging full soc table
    #
    print('Querying charging full soc table')
    query = 'SELECT  vin, end_time, normalized_ah_throughput FROM guobiao_tsp_tbls.charging_full_soc\
        WHERE {} AND {} {}'.format(vin_condition, day_condition, limit_condition)
    #print(query)
    df_charging_full_soc = hive2pandas(hc, query)
    df_charging_full_soc_by_vin = aggregate_by_vin_charging_full_soc(df_charging_full_soc)
    df_charging_full_soc_by_vin.to_csv('test_charging_full_soc.csv')

    # Group all DFs into a list for ease
    dfs = [df_charging_by_vin, df_trips_by_vin, df_charging_full_soc_by_vin,
           df_trips_full_soc_by_vin]

    #
    # 7. Apply filter on min_count_periods
    #
    dfs = apply_min_count_periods_filter(dfs, min_count_periods)

    #
    # 8. Merge DFs
    #
    df = merge_dfs(dfs)
    # Select columnns of interest and rename cols
    df = rename_filter_columns(df)

    #
    # 9. Export to hive
    #
    export_to_hive(hc, df, end_date)
    df.to_csv('test.csv')
    print('End')

def hive2string(hc, query, colname):
    spark_df = hc.sql("""{}""".format(query))
    row = spark_df.first()
    res = row[colname]
    return res


def hive2pandas(hc, query):
    spark_df = hc.sql("""{}""".format(query))
    # Convert to pandas dataframe
    df = spark_df.toPandas()
    return df

def hive2csv(hc, query, outfile):
    df = hive2pandas(hc, query)
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

    min_count_periods = 5 # At least 5 trips/periods 
    min_count_records = 20 #At least 20 records per charging_period/trip
    day_range = 30 #Take last 30 days of data
    sc = spark.sparkContext

    main(sc, day_range, min_count_records, min_count_periods)
