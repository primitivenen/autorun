from vehicle import *
import subprocess
import findspark
findspark.init('/usr/hdp/current/spark2-client')
import pyspark
conf = pyspark.SparkConf().setAll([('spark.app.name', 'get_daily_stats'), # App Name
                                ('spark.master', 'local[*]'),              # spark run mode: locally or remotely
                                ('spark.submit.deployMode', 'client'), # deploy in yarn-client or yarn-cluster
                                ('spark.executor.memory', '8g'),       # memory allocated for each executor
                                ('spark.executor.cores', '5'),         # number of cores for each executor
                                ('spark.executor.instances', '48'),    # number of executors in total
                                ('spark.yarn.am.memory','20g')])       # memory for spark driver (application master)

sc = pyspark.SparkContext(conf=conf)
#sc.addPyFile('/home/stang/user-profile/stats-spark/veh.zip') 

col_dict = {1: 'TDATE', 15: 'CCS_CHARGEVOLT', 59: 'BCS_VEHSPD', 60: 'ICM_TOTALODOMETER'}
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

(ret, out, err)= run_cmd(['hdfs', 'dfs', '-ls', '/data/ag/by-day/'])
lines = out.split('\n')

files = ['hdfs://namenode:8020' + line.split()[-1] for line in lines[1:-1]]

def get_daily_stats(data_file, save_to_json=True):
    rdd = sc.textFile(data_file).filter(lambda line: len(line.split(',')) in [85, 86])
    res = rdd.map(transform_to_tuple).groupByKey().map(compute_stats).collect()  # new
    res = OrderedDict(res)
#     print('Number of vehicles in {}: {}'.format(data_file, len(res)))
    if save_to_json:
        curr_date = data_file.split('/')[-1].split('_')[-1].split('.')[0]
        json_file = 'ag_stats_corrected/ag_stats_{}.json'.format(curr_date)
        with open(json_file, 'w') as jsonf:
            json.dump(res, jsonf)
    print(data_file + ' done.')

for f in files[971:]:
    get_daily_stats(f)

sc.stop()
