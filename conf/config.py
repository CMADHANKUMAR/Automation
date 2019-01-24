ambari_url = "http://10.81.1.161:8080/api/v1/clusters"
ambari_username = "admin"
ambari_password = "admin" 


name_node_port = "8020"
job_tracker_port = "8088"
local_dataset_path = "datasets"
hive_target_dataset = "target"

oozie_port = "11000"
oozie_dir_list = ["/oozie_data","/oozie_data/app_directory"]
oozie_app_directory = "/oozie_data/app_directory"
workflow_path = "src/workflow.xml"
query_path = "src/hive.sql"
job_file_path = "src/job.properties"
oozie_interval = 20

url_list = ['http://download.cms.gov/openpayments/PGYR13_P011819.ZIP', 'http://download.cms.gov/openpayments/PGYR14_P011819.ZIP', 'http://download.cms.gov/openpayments/PGYR15_P011819.ZIP', 'http://download.cms.gov/openpayments/PGYR16_P011819.ZIP', 'http://download.cms.gov/openpayments/PGYR17_P011819.ZIP' ]

spark2_home ="/usr/hdp/2.6.4.0-91/spark2/bin"
schema_path_one = "/spark_job_data/src/rpay/schema1"
schema_path_two = "/spark_job_data/src/rpay/schema2"
spark_output_path = "/spark_job_data/out/rpay"
spark_dir_list = ['/spark_job_data', '/spark_job_data/src', '/spark_job_data/src/rpay', '/spark_job_data/src/rpay/schema1', '/spark_job_data/src/rpay/schema2', '/spark_job_data/out', '/spark_job_data/out/rpay', '/user', '/user/root']
spark_interval = 10
create_parquet_script = "src/create_parq_common.py"
spark_query_script = "src/query.py"

hive_dir_list = [ '/hive_job_data','/hive_job_data/rsrch_data','/hive_job_data/rsrch_data/2017','/hive_job_data/rsrch_data/2016', '/hive_job_data/rsrch_data/2015', '/hive_job_data/rsrch_data/2014', '/hive_job_data/rsrch_data/2013']
hive_target = "target"
hdfs_target = "/hive_job_data/rsrch_data"
