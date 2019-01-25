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
        self.hdfs.put_file(query_path, oozie_app_directory,self.util)
        self.hdfs.put_file(workflow_path, oozie_app_directory, self.util)


    def run_workflow(self):
        with open('src/cluster_details.json', 'r+') as f:
            data = json.load(f)

        print("running oozie workflow in crontab")
        cmd = "oozie job --oozie http://"+ data['oozie_server_host'] + ":"+ data['oozie_port'] + "/oozie -config " + os.path.abspath(job_file_path) + " -run"
        cron = CronTab(user='root')
        job = cron.new(command=cmd, comment='Run hive job every '+str(oozie_interval) +' minutes')
        job.minute.every(oozie_interval)
        cron.write()

    def configure_job(self):
        
        with open('src/cluster_details.json', 'r+') as f:
            data = json.load(f)
        name_node = "hdfs://"+data['name_node_host']+":"+data['name_node_port']+""
        jobtracker = data['job_tracker_host'] + ":"+data['job_tracker_port']
        
        print("configuring job.properties")
        config = ConfigParser.ConfigParser()
        config.optionxform = str
        config.read(job_file_path)
        config.set('DEFAULT', 'nameNode', name_node)
        config.set('DEFAULT', 'jobTracker', jobtracker)
        config.set('DEFAULT', 'appPath', oozie_app_directory)
        with open(job_file_path, 'wb') as configfile:
            config.write(configfile)

    def automate(self):
        self.configure_job()
        self.create_hdfs_directories()
        self.push_workflow_to_hdfs()
        self.run_workflow()
