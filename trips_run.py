import sys
sys.path.insert(0, '../stats-core/')
from vehicle import *
#from vehicle_stats import *
from get_distance_driven import *
from datetime import datetime, date
import subprocess
import findspark
import os

# spark location on namenode server
findspark.init("/usr/hdp/current/spark2-client")
import pyspark
from pyspark.sql import HiveContext
from pyspark.sql.functions import col, when

import zipfile
zf = zipfile.ZipFile('veh.zip', 'w')
for f in ['vehicle.py', 'utils.py', 'get_distance_driven.py', 'COL_DEPENDENCY_DICT.py']:
    zf.write('../stats-core/' + f, f)
zf.close()

COL_NUM_DICT = {1: 'TDATE', 2: 'SDATE', 16: 'CCS_CHARGECUR', 58: 'HCU_BATCHRGDSP',
                60: 'ICM_TOTALODOMETER', 74: 'LON84', 75: 'LAT84'}

# configs
conf = pyspark.SparkConf().setAll([('spark.app.name', 'daily_trips'),
                                   ('spark.master', 'yarn'),
                                   ('spark.submit.deployMode', 'client'),
                                   ('spark.executor.memory', '10g'),
                                   ('spark.memory.fraction', '0.7'),
                                   ('spark.executor.cores', '3'),
                                   ('spark.executor.instances', '20'),
                                   ('spark.yarn.am.memory', '10g')])
conf1 = pyspark.SparkConf().setAll([('spark.app.name', 'export_trip_to_hive'),
                                    ('spark.master', 'local'),
                                    ('spark.executor.memory', '10g'),
                                    ('spark.memory.fraction', '0.7'),
                                    ('spark.executor.cores', '3')])

def human_readable_time(t):
    t = int(t)
    if t < 60:
        return '{:02d}s'.format(t)
    elif t < 3600:
        m = t // 60
        s = t % 60
        return '{:02d}m{:02d}s'.format(m, s)
    else:
        h = t // 3600
        m = (t - h*3600) // 60
        s = t % 60
        return '{}h{:02d}m{:02d}s'.format(h, m, s)

def transform_to_tuple(line):
    """
    input a line, get required signals as specified in COL_NUM_DICT
    """
    fields = line.split(",")
    vin = fields[0]
    otherfields = {}
    for col_index, col in COL_NUM_DICT.items():
        this_value = fields[int(col_index)]
        otherfields[col] = this_value
    return vin, otherfields

def filter_driving(df):
    filter1 = df['ICM_TOTALODOMETER'] > 0
    filter2 = (df['CCS_CHARGECUR'] < 0.1) | (df['CCS_CHARGECUR'].isna())
    filter3 = (df['HCU_BATCHRGDSP'] == 0) | (df['HCU_BATCHRGDSP'].isna())
    filters = filter1 & filter2 & filter3
    return df.loc[filters].copy()

def _trip_stats_helper(df, vin, max_disrupt=10):
    # drop rows that has null gps value
    df = df.dropna(subset=['LAT84', 'LON84'])
    df = df.reset_index(drop=True)
    
    res = {}
    res['start_time'] = []
    res['end_time'] = []
    res['start_lat'] = []
    res['start_lon'] = []
    res['end_lat'] = []
    res['end_lon'] = []
    res['distance']  = []
    
    if df.empty:
        return pd.DataFrame(res)
    
    # add time difference column, convert to seconds
    df.loc[:, 'tdiff'] = df['TDATE'].diff().fillna(timedelta(seconds=10))
    print(df.shape)
    df['tdiff'] = df['tdiff'].apply(lambda x: int(x.seconds))
    
    # get indices where time difference longer than threshold
    indices = df.index[df['tdiff'] > int(max_disrupt*60)].tolist()
    indices.insert(0, 0)
    if indices[-1] < df.shape[0] - 1:
        indices.append(df.shape[0])
    
    for i in range(len(indices) - 1):
        lo = indices[i]
        hi = indices[i+1]
        
        if lo + 1 == hi:
            continue
            
        res['start_time'].append(df['TDATE'].iloc[lo])
        res['start_lat'].append(df['LAT84'].iloc[lo])
        res['start_lon'].append(df['LON84'].iloc[lo])
        res['end_time'].append(df['TDATE'].iloc[hi-1])
        res['end_lat'].append(df['LAT84'].iloc[hi-1])
        res['end_lon'].append(df['LON84'].iloc[hi-1])
        
        df_seg = df.iloc[lo:hi, :].copy()
        res['distance'].append(get_distance_driven(df_seg))
        
    res_df = pd.DataFrame(res)
    if res_df.empty:
        return res_df
    res_df['vin'] = vin
    res_df['duration'] = res_df.apply(lambda x: int((x['end_time']-x['start_time']).total_seconds())/3600.0, axis=1)
    return res_df

def compute_trip_stats(x):
    df = pd.DataFrame(list(x[1]))
    df['VIN'] = x[0]
    # initialize in Vehicle meaning convert epoch time to timestamp, sort by tdate, drop duplicate
    veh = Vehicle(df) 
    # filter out driving record only
    df = filter_driving(veh.df)
    
    trip_df = _trip_stats_helper(df.copy(), x[0])
    res = trip_df.to_dict()
    print(x[0])
    return veh.vin, res

def run_cmd(args_list):
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.communicate()
    return proc.returncode

def main():
    args = sys.argv[1:]
    if not args:
        print('usage: specify date: executable -d yyyymmdd or --all, plus optinal --force-run')
        sys.exit(1)
    
    dates = []
    if args[0] == '-d':
        dates.append(args[1])
        del args[0:2]
    elif args[0] == '--all':
        start_date = date(2018, 6, 2)
        end_date = date(2015, 1, 18)
        def daterange(start_date, end_date):
            for n in range(int((start_date - end_date).days + 1)):
                yield start_date - timedelta(n)
        for single_date in daterange(start_date, end_date):
            dates.append(single_date.strftime('%Y%m%d'))
        del args[0]
    else:
        print('invalid option {}'.format(args[0]))
        sys.exit(1)

    force_run = False
    if args and args[0] == '--force-run':
        force_run = True
    
    if not os.path.exists('trip_stats'):
        os.makedirs('trip_stats')

    # calculate trip stats
    sc = pyspark.SparkContext(conf=conf)
    sc.addPyFile('veh.zip')
    for d in dates:
        if str(d)[:4] == '2018':
            file_loc = 'csv/d={}'.format(d)
        else:
            file_loc = 'by-day/ag_{}.csv'.format(d)
        data_file = 'hdfs://namenode:8020/data/ag/' + file_loc

        out_file = 'trip_stats/trip_stats_{}.csv'.format(d)

        if not force_run and os.path.exists(out_file):
            print('{} processed before, skipping'.format(data_file))
            continue

        returncode = run_cmd(['hdfs', 'dfs', '-test', '-e', '/data/ag/' + file_loc])
        if returncode:
            print('{} does not exist, skipping ..'.format(data_file))
            continue

        rdd = sc.textFile(data_file).filter(lambda line: len(line.split(',')) in [85, 86])
        res = rdd.map(transform_to_tuple).groupByKey().map(compute_trip_stats).collect()

        vals = OrderedDict(res).values()
        ff = pd.DataFrame()
        for val in vals:
            temp = pd.DataFrame(val)
            if temp.empty:
                continue
            ff = pd.concat([ff, temp])
        ff.to_csv(out_file, index=False)
        ts = pd.to_datetime(datetime.now())
        print('{}: {} has been processed for trip stats'.format(ts, data_file))
    sc.stop()

    
    # export to hive
    sc = pyspark.SparkContext(conf=conf1)
    hc = HiveContext(sc)
    for d in dates:
        stat_file = 'trip_stats/trip_stats_{}.csv'.format(d)
        if not force_run and os.path.exists(stat_file + '.done'):
            print('{} exported before, skipping..'.format(out_file))
            continue
        
        if not os.path.exists(stat_file):
            print('{} does not exist, skipping'.format(stat_file))
            continue
        
        df = pd.read_csv(stat_file)
        df['day'] = df.apply(lambda row : row['start_time'].split()[0], axis=1)
        df['start_time'] = df.apply(lambda row: row['start_time'].split()[1].split('+')[0], axis=1)
        df['end_time'] = df.apply(lambda row: row['end_time'].split()[1].split('+')[0], axis=1)
        df = df[['vin', 'day', 'start_time', 'end_time', 'start_lat', 'start_lon', 
                 'end_lat', 'end_lon', 'distance', 'duration']].copy()
        partition_col = 'day'
        df.drop(labels=partition_col, axis=1, inplace=True)

        spark_df = hc.createDataFrame(df)
        # convert NaN to null, as they are different in Hive
        cols = [when(~col(x).isin("NULL", "NA", "NaN"), col(x)).alias(x) for x in spark_df.columns]
        spark_df = spark_df.select(*cols)

        spark_df.registerTempTable('update_dataframe')
        day = pd.to_datetime(str(d)).date().strftime('%Y-%m-%d')
        sql_cmd = """SELECT COUNT(*) FROM tsp_tbls.trips
                      WHERE {}='{}' """.format(partition_col, day)
        print(sql_cmd)
        hc.sql(sql_cmd).show()

        sql_cmd = """INSERT OVERWRITE TABLE tsp_tbls.trips
                  PARTITION ({}='{}')
                  SELECT * FROM update_dataframe""".format(partition_col, day)
        print(sql_cmd)
        hc.sql(sql_cmd)

        sql_cmd = """SELECT COUNT(*) FROM tsp_tbls.trips
                  WHERE {}='{}' """.format(partition_col, day)
        print(sql_cmd)
        hc.sql(sql_cmd).show()

        ts = pd.to_datetime(datetime.now())
        print('{}: {} has been exported to Hive (overwrite mode)'.format(ts, stat_file))
        # write a marker file
        with open(stat_file + '.done', 'w') as outf:
            pass
    sc.stop()
    
if __name__ == '__main__':
    main()
