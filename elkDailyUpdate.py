import findspark
findspark.init("/usr/hdp/current/spark2-client")
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import subprocess
import csv
import json
import os
import sys
import shutil
import glob


def queryNewData(indexName, hc):
    _ret = 0
    
    set_1 = ["ag2_vehicle_stats", "daily_stats", "hourly_stats", "polarization", "trips_full_soc"]
    set_2 = ["charging_full_soc_guobiao", "daily_stats_guobiao", "hourly_stats_guobiao", "trips_full_soc_guobiao"]
        
    VM_CREDENTIALS = "bitnami@172.15.5.29"
    ES_CREDENTIALS = "elastic:b6WCGumEp7gmlX3uuNDI"
    LOCAL_DIRECTORY = "/home/elk-daily-updates/"
    TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"
    ES_IP_PORT = "localhost:9200/"
    PATH_TO_MAPPINGS = "/home/bitnami/elk-daily-updates/mappings/"
    UPLOAD_PATH = "/home/bitnami/elk-daily-updates/data/"
    NOT_UPLOADED_PATH = "/home/bitnami/elk-daily-updates/data_not_uploaded/"
    TOTAL_LINES_IN_EACH_FILE = "10000"
    PREFIX = "XX"
    
    if (indexName in set_1):
        tableName = "tsp_tbls." + indexName
    elif (indexName in set_2):
        tableName = "guobiao_tsp_tbls." + concatenateListElements(indexName.split("_")[0:-1])
    
    fileName = indexName + "_lastSuccessfulDate.txt"
    
    # import pdb; pdb.set_trace()
    
    with open(LOCAL_DIRECTORY + fileName, "r") as f:
        lastSuccessfulDate = f.readline().strip()
        
    query_1 = """SELECT * FROM {} WHERE day > '{}'""".format(tableName, lastSuccessfulDate)
    queryResult_1 = hc.sql(query_1)
        
    totalNewRecords = queryResult_1.count()
    print("INFO: Fetched " + str(totalNewRecords) + " new records for index " + indexName + ".")
        
    if (totalNewRecords > 0):
        query_2 = """SELECT MAX(day) FROM {}""".format(tableName)
        queryResult_2 = hc.sql(query_2)
        df_2 = queryResult_2.toPandas()
            
        dateString = df_2.iloc[0].iloc[0].strftime("%Y-%m-%d")
        hdfsPathString = indexName + "-" + dateString
        localPathString = LOCAL_DIRECTORY + hdfsPathString + ".csv"
            
        try:
            queryResult_1.write.csv(hdfsPathString, timestampFormat = TIMESTAMP_FORMAT)
        except Exception as ex:
            print(ex)
            print("WARN: Unable to write results of the Hive query into hdfs, skipping updates.")
            return
                    
        (ret, out, err) = run_cmd(["hdfs", "dfs", "-getmerge", hdfsPathString, localPathString])
        if (ret != 0):
            print("WARN: Unable to merge results from hdfs into local directory, skipping updates.")
            return
            
        # No need for error checking here
        (ret, out, err) = run_cmd(["hdfs", "dfs", "-rmr", "-skipTrash", hdfsPathString])
        (ret, out, err) = run_cmd(["rm", LOCAL_DIRECTORY + "." + hdfsPathString + ".csv" + ".crc"])
            
        # Create the index in elasticsearch
        print("INFO: Creating the index " + hdfsPathString + " in elasticsearch.")
        (ret, out, err) = run_cmd(["ssh", VM_CREDENTIALS, "curl", "-H", "Content-Type:application/json", 
                                   "--user", ES_CREDENTIALS, "-XPUT", ES_IP_PORT + hdfsPathString, 
                                   "-d", "@" + PATH_TO_MAPPINGS + indexName + ".json"])
            
        if (ret != 0):
            print("ERROR: Failed to create new index " + hdfsPathString + " in elasticsearch, skipping the update.")
            return
            
        (ret, out, err) = run_cmd(["sudo", "split", "-l", TOTAL_LINES_IN_EACH_FILE, localPathString, 
                                   LOCAL_DIRECTORY + PREFIX])
            
        filesToUpload = glob.glob(LOCAL_DIRECTORY + PREFIX + "*")
                        
        for f in filesToUpload:
            (ret, out, err) = run_cmd(["sudo", "mv", f, f + ".csv"])
                
            # Add header
            with open(LOCAL_DIRECTORY + "tempFile.csv", "w") as outfile:
                for infile in (LOCAL_DIRECTORY + indexName + ".header", f + ".csv"):
                    shutil.copyfileobj(open(infile), outfile)
                
            (ret, out, err) = run_cmd(["sudo", "mv", LOCAL_DIRECTORY + "tempFile.csv", f + ".csv"])
                
            # Convert to required format for upload, creates a json file
            csv2es(hdfsPathString, f + ".csv")
                
            # Upload the new data to elasticsearch
            (ret, out, err) = run_cmd(["scp", f + ".json", VM_CREDENTIALS + ":" + UPLOAD_PATH])
            _ret = _ret + ret
                
            (ret, out, err) = run_cmd(["ssh", VM_CREDENTIALS, "curl", "-H", "Content-Type:application/x-ndjson", 
                                       "--user", ES_CREDENTIALS, "-XPOST", ES_IP_PORT + "_bulk?pretty", 
                                       "--data-binary", "@" + UPLOAD_PATH + f.split("/")[-1] + ".json"])
            _ret = _ret + ret
                
            if (ret == 0):
                (ret, out, err) = run_cmd(["ssh", VM_CREDENTIALS, "rm", UPLOAD_PATH + f.split("/")[-1] + ".json"])
                _ret = _ret + ret
                print("INFO: Bulk " + f.split("/")[-1] + "-" + hdfsPathString + " uploaded successfully to elasticsearch.")
            else:
                (ret, out, err) = run_cmd(["ssh", VM_CREDENTIALS, "mv", UPLOAD_PATH + f.split("/")[-1] + ".json", 
                                           NOT_UPLOADED_PATH])
                _ret = _ret + ret
                print("WARN: Bulk " + f.split("/")[-1] + "-" + hdfsPathString + " was not uploaded.")
                    
            # Clean up
            (ret, out, err) = run_cmd(["sudo", "rm", f + ".csv"])
            (ret, out, err) = run_cmd(["sudo", "rm", f + ".json"])
            
        (ret, out, err) = run_cmd(["sudo", "rm", LOCAL_DIRECTORY + hdfsPathString + ".csv"])
            
        if (_ret == 0):
            df_2.to_csv(LOCAL_DIRECTORY + fileName, index = False, header = False)
            print("INFO: Updated the lastSuccessfulDate for index " + indexName + ".")
        else:
            print("_ret = " + str(_ret))
            print("WARN: Rollback might be needed for index " + indexName + ".")
            # TODO: delete the index which was already created
            # TODO: send an alert email


def run_cmd(args_list):
    # Run linux commands
    # print("Running system command: {0}".format(" ".join(args_list)))
    proc = subprocess.Popen(args_list, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err


def isFloat(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def concatenateListElements(list):
    result = ""
    for element in list:
        result = result + str(element) + "_"
    return result[0:-1]


def csv2es(indexName, fileName):
    LOCAL_DIRECTORY = "/home/elk-daily-updates/"
    
    jsonFileName = fileName.split("/")[-1].split(".")[0] + ".json"
    
    with open(fileName) as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    metaData = {"index": {"_index": indexName, "_type": "doc"}}

    geoVariables = ["start", "mean", "end"]
  
    with open(LOCAL_DIRECTORY + jsonFileName, "w") as f:
        for r in rows:
            for var in geoVariables:
                lat = var + "_latitude"
                lon = var + "_longitude" 
                if(lat in r and lon in r):
                    if (r[lat] != "" and r[lon] != ""):
                        r[var + "_location"] =  {"lat": float(r[lat]), "lon": float(r[lon])}

                    r.pop(lat, None)
                    r.pop(lon, None)
                
            json.dump(metaData, f, separators = (",", ":"), ensure_ascii = False)
            f.write("\n")
            for k in r.iterkeys():
                if isinstance(r[k], dict) == False:
                    if(r[k].isdigit()):
                        r[k] = int(r[k])
                    elif (isFloat(r[k])):
                        r[k] = float(r[k])
			for i in list(r):
				if not r[i] or r[i]=='':
					r.pop(i, None)
            json.dump(r, f, separators = (",", ":"))
            f.write("\n")



# Check the status of the elasticsearch server
(ret, out, err) = run_cmd(["ssh", "bitnami@172.15.5.29", "curl", "--user", 
                           "elastic:b6WCGumEp7gmlX3uuNDI", "-XGET", "localhost:9200/_cat/indices?v"])

if (ret == 0):
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("ELK Daily Update") \
        .enableHiveSupport() \
        .config("spark.executor.memory", "4G") \
        .config("spark.submit.deployMode", "client") \
        .config("spark.memory.fraction", "0.7") \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.instances", "10") \
        .config("spark.yarn.am.memory", "4G") \
        .getOrCreate()
        
    sc = spark.sparkContext
    hiveContext = HiveContext(sc)
    
    updates = ["daily_stats", "hourly_stats", "polarization", "trips_full_soc", "daily_stats_guobiao", "hourly_stats_guobiao", 
               "trips_full_soc_guobiao"]
    
    # updates = ["charging_full_soc_guobiao"]
    
    updates = ["ag2_vehicle_stats"]
    
    i = 0
    
    while i < len(updates):
        print("INFO: Updating the index " + updates[i] + ".")
        queryNewData(updates[i], hiveContext)
        i = i + 1

    spark.stop()
    
else:
    print("ERROR: elasticsearch server is not running, skipping updates.")

