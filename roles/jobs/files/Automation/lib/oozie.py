from conf.config import *
from crontab import CronTab
import ConfigParser
import os
import json
class OozieAutomate:

    def __init__(self , oozie_dir_list, hdfs, util):
        self.dir_list = oozie_dir_list
        self.hdfs = hdfs
        self.util = util


    def create_hdfs_directories(self):
        print("creating hdfs directories for oozie")
        for directory in self.dir_list:
            self.hdfs.make_dir(directory, self.util)

        self.hdfs.own_dir(self.dir_list[0], self.util)

    def push_workflow_to_hdfs(self):
        print("pushing workflow to hdfs")
        self.hdfs.put_file(QUERY_PATH, OOZIE_APP_DIRECTORY,self.util)
        self.hdfs.put_file(WORKFLOW_PATH, OOZIE_APP_DIRECTORY, self.util)


    def run_workflow(self):
        with open('src/cluster_details.json', 'r+') as f:
            data = json.load(f)

        print("running oozie workflow in crontab")
        cmd = "oozie job --oozie "+data['oozie_base_url']+" -config " + os.path.abspath(JOB_FILE_PATH) + " -run"
        cron = CronTab(user='root')
        job = cron.new(command=cmd, comment='Run hive job every '+str(OOZIE_INTERVAL) +' minutes')
        job.minute.every(OOZIE_INTERVAL)
        cron.write()

    def configure_job(self):
        
        with open('src/cluster_details.json', 'r+') as f:
            data = json.load(f)
        name_node = "hdfs://"+data['name_node_address']
        jobtracker = data['jobtracker']
        
        print("configuring job.properties")
        config = ConfigParser.ConfigParser()
        config.optionxform = str
        config.read(JOB_FILE_PATH)
        config.set('DEFAULT', 'nameNode', name_node)
        config.set('DEFAULT', 'jobTracker', jobtracker)
        config.set('DEFAULT', 'appPath', OOZIE_APP_DIRECTORY)
        with open(JOB_FILE_PATH, 'wb') as configfile:
            config.write(configfile)

    def automate(self):
        self.configure_job()
        self.create_hdfs_directories()
        self.push_workflow_to_hdfs()
        self.run_workflow()
