import sys
sys.path.insert(0, '../stats-core/')
from COL_DEPENDENCY_DICT import *


default_configs = {
	'dates': 20150118,
	'freq': 'daily',
	'stats': sorted(list(COL_DEPENDENCY_DICT.keys())),
	'customized_module_file': 'veh.zip',
	'ag_hdfs_dir': 'hdfs://namenode:8020/data/ag/',
	'dest_dir': 'ag_stats/',
	'force_run': False
}
