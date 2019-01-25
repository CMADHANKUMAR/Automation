import os
from conf.config import *

class Dataset:
    def __init__(self, url_list, util):
        self.url_list = url_list
        self.util = util

    def setup_datasets(self):
        print("Setting up the datasets")
        if not os.path.exists(LOCAL_DATASET_PATH):
            os.makedirs(LOCAL_DATASET_PATH)
        year = 2013
        for url in self.url_list :
            local_path = LOCAL_DATASET_PATH+"/"+str(year)+".ZIP"
            self.util.download_file(url, local_path)
            self.util.unzip_file(local_path, LOCAL_DATASET_PATH)
            file_list =[ LOCAL_DATASET_PATH+"/OP_DTL_GNRL_PGYR"+str(year)+"_P01182019.csv", LOCAL_DATASET_PATH+"/OP_REMOVED_DELETED_PGYR"+str(year)+"_P01182019.csv", LOCAL_DATASET_PATH+"/OP_DTL_OWNRSHP_PGYR"+str(year)+"_P01182019.csv", LOCAL_DATASET_PATH+"/OP_PGYR"+str(year)+"_README_P01182019.txt", local_path]
            print(os.getcwd())
            for file_name in file_list :
                try:
                    os.remove(file_name)
                except:
                    print("Error while deleting file ", file_name)

            year = year+1

    def automate(self):
        self.setup_datasets()
