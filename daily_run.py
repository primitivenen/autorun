# date: 2018-03-01
# author: Shuai Tang (stang@gacrndusa.com)
# version: 0.1
import sys
sys.path.insert(0, '../stats-core/')
from vehicle import *
from vehicle_stats import *
from configs import *
from datetime import datetime, date
import subprocess
import findspark

# spark location on namenode server
findspark.init("/usr/hdp/current/spark2-client")
import pyspark
from pyspark.sql import HiveContext
from pyspark.sql.functions import col, when

import zipfile
zf = zipfile.ZipFile('veh.zip', 'w')
for f in ['vehicle.py', 'vehicle_stats.py', 'utils.py',
          'get_dist_on_battery.py', 'get_charged_stats.py',
          'get_total_hours.py', 'get_average_speed.py',
          'get_distance_driven.py', 'treat_missing_data.py', 
          'get_energy_n_capacity.py', 'COL_DEPENDENCY_DICT.py']:
    zf.write('../stats-core/' + f, f)
zf.write('configs.py')
zf.close()

# configs
conf = pyspark.SparkConf().setAll([('spark.app.name', 'daily_stats_run'),
                                   ('spark.master', 'yarn'),
                                   ('spark.submit.deployMode', 'client'),
                                   ('spark.executor.memory', '10g'),
                                   ('spark.memory.fraction', '0.7'),
                                   ('spark.executor.cores', '3'),
                                   ('spark.executor.instances', '20'),
                                   ('spark.yarn.am.memory', '10g')])
conf1 = pyspark.SparkConf().setAll([('spark.app.name', 'export_to_hive'),
                                    ('spark.master', 'local'),
                                    ('spark.executor.memory', '10g'),
                                    ('spark.memory.fraction', '0.7'),
                                    ('spark.executor.cores', '3')])

COL_NUM_DICT = {1: 'TDATE', 2: 'SDATE', 4: 'BMS_BATTCURR', 5: 'BMS_BATTVOLT', 8: 'BMS_CELLVOLTMAX',
                9: 'BMS_CELLVOLTMIN', 11: 'BMS_BATTSOC', 12: 'BMS_BATTTEMPAVG',
                13: 'BMS_BATTTEMPMAX',
                14: 'BMS_BATTTEMPMIN', 15: 'CCS_CHARGEVOLT', 16: 'CCS_CHARGECUR',
                52: 'EMS_ACCPEDALPST',
                53: 'EMS_BRAKEPEDALST', 57: 'HCU_AVGFUELCONSUMP', 58: 'HCU_BATCHRGDSP',
                59: 'BCS_VEHSPD',
                60: 'ICM_TOTALODOMETER'}


def transform_to_tuple(line):
    """
    output is a tuple, key is vin, value is all columns in col_dict
    """
    fields = line.split(",")
    vin = fields[0]
    otherfields = {}
    for col_index, col in COL_NUM_DICT.items():
        this_value = fields[int(col_index)]
        otherfields[col] = this_value
    return vin, otherfields


def compute_stats(x, stats, freq):
    """
    x is a list of tuple, (vin, other_fields)
    return is (vin, stats). stats will be a dict {'vin1': {'daily': stats},
                                                  'vin1': {'hourly': stats}...
    """
    df = pd.DataFrame(list(x[1]))
    df['VIN'] = x[0]
    veh = Vehicle(df)
    veh.add_cellvoltdiff()
    veh.add_speed_diff()
    res = get_freq_based_stats(veh.df, veh.vin, stats, freq)
    return veh.vin, res


def run_cmd(args_list):
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.communicate()
    return proc.returncode


def main(**kwargs):
    dates = kwargs.pop('dates', default_configs['dates'])  # default to 20150118
    if not isinstance(dates, list):
        dates = [dates]
    freq = kwargs.pop('freq', default_configs['freq'])
    stats = kwargs.pop('stats', default_configs['stats'])
    customized_module_file = kwargs.pop('customized_module_file', 
        default_configs['customized_module_file'])
    ag_hdfs_dir = kwargs.pop('ag_hdfs_dir', default_configs['ag_hdfs_dir'])
    dest_dir = kwargs.pop('dest_dir', default_configs['dest_dir'])
    force_run = kwargs.pop('force_run', default_configs['force_run'])

    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
        
    # generate daily or hourly stats
    sc = pyspark.SparkContext(conf=conf)
    sc.addPyFile(customized_module_file)
    print('CALCULATING STATS...')
    for i, d in enumerate(dates):
        # create folder if needed
        directory = os.path.join(dest_dir, str(d))
        if not os.path.exists(directory):
            os.makedirs(directory)

        # check if file has been processed before
        if str(d)[:4] == '2018':
            file_loc = 'csv/d={}'.format(d)
        else:
            file_loc = 'by-day/ag_{}.csv'.format(d)
        data_file = ag_hdfs_dir + file_loc

        out_dest = os.path.join(dest_dir, str(d), 'stats_{}_{}.csv'.format(d, freq))
        if not force_run and os.path.exists(out_dest):
            print('{} has been processed before, skipping..'.format(data_file))
            continue

        # check if file exist in HDFS
        returncode = run_cmd(['hdfs', 'dfs', '-test', '-e', '/data/ag/' + file_loc])
        if returncode:
            print('{} does not exist, skipping ..'.format(data_file))
            continue

        # take a break every 10 files, so that others can also run spark jobs
        #if i > 0 and i % 10 == 1:
        #    sc.stop()
        #    sc = pyspark.SparkContext(conf=conf)
        #    sc.addPyFile(customized_module_file)

        # start running in spark job
        ts = pd.to_datetime(datetime.now())
        print('{}: processing {}..'.format(ts, data_file))
        rdd = sc.textFile(data_file).filter(lambda line: len(line.split(',')) in [85, 86])
        res = rdd.map(transform_to_tuple).groupByKey().map(
            lambda x: compute_stats(x, stats, freq)).collect()
        vals = OrderedDict(res).values()

        # expand the result into a dataframe
        df = pd.DataFrame()
        for i in range(len(vals)):
            df = pd.concat([df, pd.DataFrame(vals[i][freq])])
        df.to_csv(out_dest, index=False)

        ts = pd.to_datetime(datetime.now())
        print('{}: {} has been processed on {} basis'.format(ts, data_file, freq))
    sc.stop()

    # export to hive
    sc = pyspark.SparkContext(conf=conf1)
    sc.setLogLevel('WARN')
    hc = HiveContext(sc)
    print('EXPORTING TO HIVE...')
    for i, d in enumerate(dates):
        # check if stats has been calculated
        stats_file = os.path.join(dest_dir, str(d), 'stats_{}_{}.csv'.format(d, freq))
        if not os.path.exists(stats_file):
            print('{} does not exist, skipping'.format(stats_file))
            continue

        # check if file has been exported to Hive already
        out_flag_file = stats_file + '.done'
        if not force_run and os.path.exists(out_flag_file):
            print('{} has been exported to Hive before, skipping..'.format(stats_file))
            continue

        # take a break every 100 files, so that others can also run spark jobs
        #if i > 0 and i % 100 == 1:
        #    sc.stop()
        #    sc = pyspark.SparkContext(conf=conf1)
        #    hc = HiveContext(sc)

        # start exporting stats (csv file) to Hive
        ts = pd.to_datetime(datetime.now())
        print('{}: exporting {} to Hive table tsp_tbls.{}_stats ..'.format(ts, stats_file, freq))
        df = pd.read_csv(stats_file)
        if freq == 'hourly': # add day column
            df['day'] = df.apply(lambda row: row['time_label'].split()[0], axis=1)
        else: # for daily, just rename time_label to day
            df = df.rename(columns = {'time_label': 'day'})
        partition_col = 'day'

        df.drop(labels=partition_col, axis=1, inplace=True)
        spark_df = hc.createDataFrame(df)
        # convert NaN to null, as they are different in Hive
        cols = [when(~col(x).isin("NULL", "NA", "NaN"), col(x)).alias(x) for x in spark_df.columns]
        spark_df = spark_df.select(*cols)

        spark_df.registerTempTable('update_dataframe')
        day = pd.to_datetime(str(d)).date().strftime('%Y-%m-%d')  # convert date format
        sql_cmd = """SELECT COUNT(*) FROM tsp_tbls.{}_stats
                  WHERE {}='{}' """.format(freq, partition_col, day)
        print(sql_cmd)
        hc.sql(sql_cmd).show()

        sql_cmd = """INSERT OVERWRITE TABLE tsp_tbls.{}_stats
                  PARTITION ({}='{}')
                  SELECT * FROM update_dataframe""".format(freq, partition_col, day)
        print(sql_cmd)
        hc.sql(sql_cmd)

        sql_cmd = """SELECT COUNT(*) FROM tsp_tbls.{}_stats
                  WHERE {}='{}' """.format(freq, partition_col, day)
        print(sql_cmd)
        hc.sql(sql_cmd).show()

        ts = pd.to_datetime(datetime.now())
        print('{}: {} has been exported to Hive (overwrite mode)'.format(ts, stats_file))
        # write a marker file
        with open(out_flag_file, 'w') as f:
            pass
    sc.stop()
    print('done.')


if __name__ == '__main__':
    args = sys.argv[1:]
    
    usage_msg = """usage:
\tfor a single date: -d yyyymmdd [--freq freq] [--force-run]')
\tfor a range of dates: --dates yyyymmdd yyyymmdd [--freq freq] [--force-run]
\tto run entire history: --all [--freq freq] [--force-run]
\tto get help: --help"""
    help_msg = """Required to specify a single date or range or range of dates or run entire history,
then optionally specify frequency, then optionally whether to force run.
Options:
\t-d, --date: specify a date, e.g. -d 20150118
\t--dates: specify starting and ending date, e.g. --dates 20150118 20150119
\t--all: run for entire history, e.g. --all
\t--freq: specify frequency, e.g. --freq hourly
\t--force-run: if present, means force re-calculate the stats"""
    
    if not args:
        print(usage_msg)
        sys.exit(1)

    # get dates
    def daterange(start_date, end_date):
        for n in range(int((end_date - start_date).days + 1)):
            yield start_date + timedelta(n)
    dates = []
    if args[0] in ['-d', '--date']:  # run one day
        if len(args) < 2:
            print('no date provided')
            sys.exit(1)
        if len(args[1]) != 8:
            print('invalid input date: {}'.format(args[1]))
            sys.exit(1)
        dates.append(args[1])
        del args[0:2]
    elif args[0] == '--dates':  # run a range of dates
        if len(args) < 3:
            print('need to provide starting and ending date')
            sys.exit(1)
        if len(args[1]) != 8 or len(args[2]) != 8:
            print('invalid starting or ending date: {} {}'.format(args[1], args[2]))
            sys.exit(1)
        try:
            start_date = pd.to_datetime(args[1], format='%Y%m%d').date()
            end_date = pd.to_datetime(args[2], format='%Y%m%d').date()
        except ValueError:
            print('invalid starting or ending date after --dates: {} {}'.\
                format(args[1], args[2]))
            sys.exit(1)
        for single_date in daterange(start_date, end_date):
            dates.append(single_date.strftime('%Y%m%d'))
        del args[0:3]
    elif 'date' in args[0]:
        print('invalid option {}, did you mean --date or --dates'.format(args[0]))
        sys.exit(1)
    elif args[0] == '--all':  # run entire history
        start_date = date(2015, 1, 18)
        end_date = date(2018, 5, 16)
        for single_date in daterange(start_date, end_date):
            dates.append(single_date.strftime('%Y%m%d'))
        del args[0]
    elif args[0] == '--help':
        print(help_msg)
        print(usage_msg)
        sys.exit(0)
    else:
        print('invalid option {}'.format(args[0]))
        sys.exit(1)

    # get freq and stats
    freq = 'daily'
    if args and args[0] == '--freq':
        if len(args) < 2:
            print('no parameter provided for freq')
            sys.exit(1)
        if args[1] not in ['daily', 'hourly']:
            print('{} not supported'.format(args[1]))
            sys.exit(1)
        freq = str(args[1])
        del args[0:2]
    elif args and 'freq' in args[0]:
        print('invalid option {}, did you mean --freq'.format(args[0]))
        sys.exit(1)
    elif args and args[0] != '--force-run':
        print('{} not supported'.format(args[0]))
        sys.exit(1)
    
    stats = sorted(list(COL_DEPENDENCY_DICT.keys()))
    if freq == 'hourly':
        stats = sorted(['distance_driven', 'speed_average', 'charging_hours', 
            'driving_hours', 'charged_energy'])

    # whether force run
    force_run = False
    if args and args[0] == '--force-run':
        force_run = True
        del args[0]
    elif args and 'forc' in args[0]:
        print('invalid option {}, did you mean --force-run'.format(args[0]))
        sys.exit(1)
    elif args and args[0] != '--force-run':
        print('{} not supported'.format(args[0]))
        sys.exit(1)

    configs = {
        'dates': dates,
        'freq': freq,
        'stats': stats,
        'force_run': force_run
    }
    main(**configs)
