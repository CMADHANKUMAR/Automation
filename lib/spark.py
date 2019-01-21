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

        self.hdfs.own_dir(self.dir_list[0], self.util)
        self.hdfs.own_dir("user", self.util)
        self.hdfs.own_dir("user/root", self.util)

    def push_data_to_hdfs(self):
        print("Pushing dataset to hdfs")
        dataset_year = 2013
        for year in range(dataset_year, 2018):
            if year < 2016 :
                dataset = local_dataset_path+"/OP_DTL_RSRCH_PGYR"+str(year)+"_P06292018.csv "
                schema_path = schema_path_one
            else  :
                dataset = local_dataset_path+"/"+"OP_DTL_RSRCH_PGYR"+str(year)+"_P06292018.csv "
                schema_path = schema_path_two
            self.hdfs.put_file(dataset, schema_path, self.util)

    def create_parquet(self, util):
        print("converting dataset to parquet format")
        cmd = spark2_home + "/spark-submit --master yarn --deploy-mode client --driver-memory 4g --num-executors 2 --executor-cores 3 --executor-memory 7g "+os.path.abspath(create_parquet_script)
        util.run_call(cmd, shell=True)

    def run_query(self):
       print("Running spark query in crontab")
       cmd = spark2_home + "/spark-submit --master yarn --deploy-mode client --driver-memory 4g --num-executors 2 --executor-cores 3 --executor-memory 2g "+ os.path.abspath(spark_query_script)
       cron = CronTab(user='root')
       job = cron.new(command=cmd, comment='Run spark job every '+ str(spark_interval) +' minutes')
       job.minute.every(spark_interval)
       cron.write()
       

    def configure_schema(self):
        print("configuring schema.json file")
        filename = 'src/schema.json'
        with open(filename, 'r') as f:
            data = json.load(f)
            data['schema1']['input_path'] = schema_path_one
            data['schema2']['input_path'] = schema_path_two
            data['schema1']['output_path'] = spark_output_path
            data['schema2']['output_path'] = spark_output_path
        os.remove(filename)
        with open(filename, 'w') as f:
            json.dump(data, f)

    def automate(self):
        self.configure_schema()
        self.create_hdfs_directories()
        self.push_data_to_hdfs()
        self.create_parquet(self.util)
        self.run_query()
