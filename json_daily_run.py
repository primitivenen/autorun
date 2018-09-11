from vehicle import *
import sys
import subprocess
import findspark
# findspark.init('/home/stang/myenv/lib/python2.7/site-packages/pyspark')
findspark.init("/usr/hdp/current/spark2-client")
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

col_dict = {1: 'TDATE', 15: 'CCS_CHARGEVOLT', 59: 'BCS_VEHSPD', 60: 'ICM_TOTALODOMETER'}
veh_pyfiles = '/home/stang/user-profile/stats-spark/veh.zip'

def transform_to_tuple(line):
    fields = line.split(",")
    vin = fields[0]
    otherfields = {}
    for col_index, col in col_dict.items():
        this_value = fields[int(col_index)]
        otherfields[col] = this_value
    return vin, otherfields

def compute_stats(x):
    df = pd.DataFrame(list(x[1]))
    df['VIN'] = x[0]
    
    stats = ['average_speed', 'miles_driven', 'charging_hours']
    freqs = ['hourly', 'daily']

    veh = Vehicle(df, list(df.columns))
    attributes = veh.get_attributes()
    res = veh.get_stats(stats, freqs, save_to_csv=False, save_to_json=False)
    return attributes['vin'], res

def run_cmd(args_list):
#     print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return =  proc.returncode
    return s_return, s_output, s_err 

# (ret, out, err)= run_cmd(['hdfs', 'dfs', '-ls', '/data/ag/by-day/'])
# lines = out.split('\n')

def get_daily_stats(data_file, save_to_json=True):
    rdd = sc.textFile(data_file) # .filter(lambda line: len(line.split(',')) == 86)
    res = rdd.map(transform_to_tuple).groupByKey().map(compute_stats).collect()  # new
    res = OrderedDict(res)
    if save_to_json:
        curr_date = data_file.split('/')[-1].split('_')[-1].split('.')[0]
        json_file = 'tmp/ag_stats_{}.json'.format(curr_date)
        with open(json_file, 'w') as jsonf:
            json.dump(res, jsonf)
    print(data_file + ' done.')
    return json_file

def generate_stat_json_for_first_day(date, in_file, out_file=None):
    if not out_file:
        out_file = 'res_ag_stats_{}_{}.json'.format(date, date)
    time_label = convert_time_label_to_str(pd.to_datetime(date).tz_localize('Asia/Hong_Kong'), 'monthly')
    with open(in_file, 'r') as inf:
        x = OrderedDict(json.load(inf))
    for vin in x.keys():
        x_stats = x[vin]
        for stat in ['average_speed_monthly', 'miles_driven_monthly', 'charging_hours_monthly']:
            daily_stat = stat.replace('monthly', 'daily')
            x_stats[stat] = []
            val_to_append = x_stats[daily_stat][-1][1]
            x_stats[stat].append((time_label, val_to_append))
    with open(out_file, 'w') as outf:
        json.dump(x, outf)
    return out_file

def merge(old_file, new_file, date, out_file):
#     print('old file: {}, new file: {}, output to {}'.format(old_file, new_file, out_file))
    def weighted_average(a, b):
        if np.isnan(a[0]) and np.isnan(b[0]):
            return (np.nan, 0)
        if a[0] == 0:
            a[1] = 1
        if b[0] == 0:
            b[1] = 1
        
        count = a[1] + b[1]
        avg = np.nansum([a[0]*a[1], b[0]*b[1]]) / count
        return (avg, count)

    def sum_two_num(a, b):
        if np.isnan(a) and np.isnan(b):
            return np.nan
        return np.nansum([a, b]) # treat nan as zero

    with open(old_file, 'r') as oldf, open(new_file, 'r') as newf:
        x = OrderedDict(json.load(oldf))
        y = OrderedDict(json.load(newf))
    allvins = list(set().union(x.keys(), y.keys()))
#     date = new_file.split('.')[0].split('_')[-1]
    time_label = convert_time_label_to_str(pd.to_datetime(date).tz_localize('Asia/Hong_Kong'), 'monthly')
    for vin in allvins:
        if vin in x.keys() and vin not in y.keys():
            # append null result
            x_stats = x[vin]
            for stat, val in x_stats.iteritems():
                tl = []  # time labels to append
                freq = stat.split('_')[-1]
                last_timestamp = pd.to_datetime(val[-1][0]).tz_localize('Asia/Hong_Kong')
                if (freq == 'daily') or (freq == 'monthly' and date[-2:] == '01'):
                    tmp = next_time_label(last_timestamp, freq)  # output format: timestamp
                    tl.append(convert_time_label_to_str(tmp, freq))
                elif freq == 'hourly':
                    for i in range(24):
                        tmp = next_time_label(last_timestamp, freq)
                        tl.append(convert_time_label_to_str(tmp, freq))
                        last_timestamp = tmp
                for time_label in tl:
                    val_to_append = (np.nan, 0) if 'average_speed' in stat else np.nan
                    val.append((time_label, val_to_append))
        elif vin not in x.keys() and vin in y.keys():
            # copy daily result
            x[vin] = y[vin]
            # add monthly stat, monthly stats will be the same with daily stats
            x_stats = x[vin]
            for stat in ['average_speed_monthly', 'miles_driven_monthly', 'charging_hours_monthly']:
                daily_stat = stat.replace('monthly', 'daily')
                x_stats[stat] = []
                val_to_append = x_stats[daily_stat][-1][1]
                x_stats[stat].append((time_label, val_to_append))
        else:  # in both x and y
            x_stats = x[vin]
            y_stats = y[vin]
            for stat, val in x_stats.iteritems():
                # extend daily stats and hourly stats
                if 'monthly' not in stat:
                    val.extend(y_stats[stat])
                    continue
                # for monthly, if this is first day of month, monthly stats will be the same with daily stats
                daily_stat = stat.replace('monthly', 'daily')
                if date[-2:] == '01':
                    val.append((time_label, y_stats[daily_stat][-1][1]))
                    continue
                # if not first day of month, then recalculate monthly stats
                if 'average_speed' not in stat:
                    val[-1][1] = sum_two_num(val[-1][1], y_stats[daily_stat][-1][1])
                else:
                    val[-1][1] = weighted_average(val[-1][1], y_stats[daily_stat][-1][1])
    
    with open(out_file, 'w') as jsonf:
        json.dump(x, jsonf)
    return out_file

if __name__ == '__main__':
    args = sys.argv[1:]
    if not args:
        print('usage: --date | -d yyyymmdd [--file | -f path/to/csv/file]')
        sys.exit(1)
    
    if args[0] in ['--date', '-d']:
        date = args[1]
        del args[0:2]
       
    daily_csv_file = 'hdfs://namenode:8020/data/ag/by-day/ag_{}.csv'.format(date)
    if args and args[0] in ['--file', '-f']:
        daily_csv_file = args[1]
        del args[0:2]
    
    conf = pyspark.SparkConf().setAll([('spark.app.name', 'daily_stats_run'),
                                       ('spark.master', 'yarn'),
                                       ('spark.submit.deployMode', 'client'),
                                       ('spark.executor.memory', '4g'),
                                       ('spark.executor.cores', '3'),
                                       ('spark.executor.instances', '80'),
                                       ('spark.yarn.am.memory','20g')])
    
    log = 'logfile.daily_run'
    static_json_file = 'res_ag_stats.json'
    if (not os.path.exists(static_json_file)) or (not os.path.exists(log)):
        first_date = '20150118'
        print("No static json file found or no processing record found, "
                      "initializing {} as beginning".format(first_date))
        
        sc = pyspark.SparkContext(conf=conf)
        sc.addPyFile(veh_pyfiles)
        
        first_csv_file = 'hdfs://namenode:8020/data/ag/by-day/ag_{}.csv'.format(first_date)
        first_json_file = get_daily_stats(first_csv_file)
    static_json_file = generate_stat_json_for_first_day(first_date, first_json_file,
                                                            out_file=static_json_file)
        with open(log, 'w') as logf:
            logf.write('{} has been processed and initialized into {}\n'.format(first_date, static_json_file))
        sys.exit(1)

    records = []
    with open(log, 'r') as logf:
        for line in logf:
            records.append(line.split()[0])
    if date in records:
        print('{} has been processed and merged'.format(date))
        sys.exit(1)
    
    last_scanned_ts = pd.to_datetime(records[-1]).tz_localize('Asia/Hong_Kong')
    next_ts = next_time_label(last_scanned_ts, 'daily')
    new_ts = pd.to_datetime(date).tz_localize('Asia/Hong_Kong')
    if new_ts != next_ts:
        print('last scanned date is {}, has to continue from there'.format(records[-1]))
        sys.exit(1)

    sc = pyspark.SparkContext(conf=conf)
    sc.addPyFile(veh_pyfiles)
 
    daily_json_file = get_daily_stats(daily_csv_file)
    merge(static_json_file, daily_json_file, date, static_json_file)
    with open('logfile.daily_run', 'a') as logf:
        logf.write('{} has been processed and merged into {}\n'.format(date, static_json_file))

