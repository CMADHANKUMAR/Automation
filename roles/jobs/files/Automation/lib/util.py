from conf.config import *
import requests
import json
import subprocess
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
            res_json = requests.get(url, auth=(ambari_username, ambari_password), verify=False)
            if res_json.status_code != 200:
                print("Failed to send request to"+ url)
                return None
            return res_json.json()

    def get_cluster_details(self):
        res_json = self.get_from_ambari(ambari_url)
        cluster_name = res_json["items"][0]["Clusters"]["cluster_name"]  
        name_node_url = ambari_url + "/" + cluster_name + "/services/HDFS/components/NAMENODE"
        res_json = self.get_from_ambari(name_node_url)
        name_node_host = res_json["host_components"][0]["HostRoles"]["host_name"]

        job_tracker_url = ambari_url + "/" + cluster_name + "/services/YARN/components/RESOURCEMANAGER"
        res_json = self.get_from_ambari(job_tracker_url)
        job_tracker_host = res_json["host_components"][0]["HostRoles"]["host_name"]

        oozie_host_url = ambari_url + "/" + cluster_name + "/services/OOZIE/components/OOZIE_SERVER"
        res_json = self.get_from_ambari(oozie_host_url)
        oozie_server_host = res_json["host_components"][0]["HostRoles"]["host_name"]
 
        cluster_details = {}
        cluster_details ['ambari_user_name'] = ambari_username
        cluster_details ['ambari_password'] = ambari_password
        cluster_details ['cluster_name'] = cluster_name
        cluster_details [ 'name_node_host' ] = name_node_host
        cluster_details [ 'name_node_port' ] = name_node_port
        cluster_details [ 'job_tracker_host' ] = job_tracker_host
        cluster_details [ 'job_tracker_port' ] = job_tracker_port
        cluster_details [ 'oozie_server_host' ] = oozie_server_host
        cluster_details [ 'oozie_port' ] = oozie_port
               
        with open('src/cluster_details.json', 'w') as outfile:  
            json.dump(cluster_details, outfile)   
