from conf.config import *
import requests
import json
import subprocess
import xmltodict

class Util:
    def download_file(self, url, local_path, proxy=None):
       if proxy:
           cmd = "wget -e use_proxy=on -e http_proxy={0} -O {1} {2}".format(proxy, local_path, url)
       else:
           cmd = "wget -O {0} {1}".format(local_path, url)
           print cmd
           self.run_call(cmd, shell=True)

    def unzip_file(self, zip_file, target_dir):
        cmd = "unzip "+zip_file+" -d "+ target_dir
        self.run_call(cmd, shell=True)

    def run_call(self, cmd, shell):
        try:
            subprocess.call(cmd, shell=shell)
        except subprocess.CalledProcessError as error:
            print >> sys.stderr, "Error: {0}".format(error)
            print "error ignored"
            return
    def get_from_ambari(self, url) :
            res_json = requests.get(url, auth=(AMBARI_USERNAME, AMBARI_PASSWORD), verify=False)
            if res_json.status_code != 200:
                print("Failed to send request to"+ url)
                return None
            return res_json.json()
      
    def get_property(self, config_file_path, property_name):
        with open(config_file_path, 'r+') as f:
            conf_dict = xmltodict.parse(f.read())
        for key in conf_dict['configuration']['property'] :
            if key['name'] == property_name :
                return key['value']        
        

    def get_cluster_details(self):
        jobtracker = self.get_property(YARN_CONF_FILE,'yarn.resourcemanager.address')
        name_node_address = self.get_property(HDFS_CONF_FILE,'dfs.namenode.rpc-address')
        oozie_base_url =  self.get_property(OOZIE_CONF_FILE,'oozie.base.url') 
        cluster_details = {}
        cluster_details ['oozie_base_url'] = oozie_base_url
        cluster_details ['name_node_address'] = name_node_address
        cluster_details ['jobtracker'] = jobtracker

        with open('src/cluster_details.json', 'w') as outfile:
            json.dump(cluster_details, outfile)
