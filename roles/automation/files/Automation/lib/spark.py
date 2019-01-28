from conf.config import *
from crontab import CronTab
import os
import json
import subprocess
class SparkAutomate:
    def __init__(self, spark_dir_list, util, hdfs) :
        self.dir_list = spark_dir_list
        self.util = util
        self.hdfs = hdfs

    def create_hdfs_directories(self):
        print("Creating hdfs directories for spark")
        for directory in self.dir_list:
            self.hdfs.make_dir(directory, self.util)

        self.hdfs.own_dir("/user/root", self.util)
        self.hdfs.own_dir(self.dir_list[0], self.util)

    def push_data_to_hdfs(self):
        print("Pushing dataset to hdfs")
        dataset_year = 2013
        for year in range(dataset_year, 2018):
            if year < 2016 :
                dataset = LOCAL_DATASET_PATH+"/OP_DTL_RSRCH_PGYR"+str(year)+DATASET_SUFFIX+".csv "
                schema_path = SCHEMA_PATH_ONE
            else  :
                dataset = LOCAL_DATASET_PATH+"/"+"OP_DTL_RSRCH_PGYR"+str(year)+DATASET_SUFFIX+".csv "
                schema_path = SCHEMA_PATH_TWO
            self.hdfs.put_file(dataset, schema_path, self.util)

    def create_parquet(self, util):
        print("converting dataset to parquet format")
        cmd = SPARK2_HOME + "/spark-submit --master yarn --deploy-mode client --driver-memory 4g --num-executors 2 --executor-cores 3 --executor-memory 7g "+os.path.abspath(CREATE_PARQUET_SCRIPT)
        util.run_call(cmd, shell=True)

    def run_query(self):
        print("Running spark query in crontab")
        cmd = SPARK2_HOME + "/spark-submit --master yarn --deploy-mode client --driver-memory 4g --num-executors 2 --executor-cores 3 --executor-memory 2g "+ os.path.abspath(SPARK_QUERY_SCRIPT)
        cron = CronTab(user='root')
        job = cron.new(command=cmd, comment='Run spark job every '+ str(SPARK_INTERVAL) +' minutes')
        job.minute.every(SPARK_INTERVAL)
        cron.write()


    def configure_schema(self):
        print("configuring schema.json file")
        filename = 'src/schema.json'
        with open(filename, 'r') as f:
            data = json.load(f)
            data['schema1']['input_path'] = SCHEMA_PATH_ONE
            data['schema2']['input_path'] = SCHEMA_PATH_TWO
            data['schema1']['output_path'] = SPARK_OUTPUT_PATH
            data['schema2']['output_path'] = SPARK_OUTPUT_PATH
        os.remove(filename)
        with open(filename, 'w') as f:
            json.dump(data, f)

    def automate(self):
        self.configure_schema()
        self.create_hdfs_directories()
        self.push_data_to_hdfs()
        self.create_parquet(self.util)
        self.run_query()
