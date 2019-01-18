import os
from conf.config import *

class Dataset:
    def __init__(self, url_list, util):
        self.url_list = url_list
        self.util = util

    def setup_datasets(self):
        print("Setting up the datasets")
        if not os.path.exists(local_dataset_path):
            os.makedirs(local_dataset_path)

        year = 2013

        for url in self.url_list :
            local_path = local_dataset_path+"/"+str(year)+".ZIP"
            #self.util.download_file(url, local_path)
            self.util.unzip_file(local_path, local_dataset_path)
            file_list =[ local_dataset_path+"/OP_DTL_GNRL_PGYR"+str(year)+"_P06292018.csv", local_dataset_path+"/OP_REMOVED_DELETED_PGYR"+str(year)+"_P06292018.csv", local_dataset_path+"/OP_DTL_OWNRSHP_PGYR"+str(year)+"_P06292018.csv", local_dataset_path+"/OP_PGYR"+str(year)+"_README_P06292018.txt", local_path]
            print(os.getcwd())
            for file_name in file_list :
                try:
                    os.remove(file_name)
                except:
                    print("Error while deleting file ", file_name)


            year = year+1



    def automate(self):
        self.setup_datasets()
