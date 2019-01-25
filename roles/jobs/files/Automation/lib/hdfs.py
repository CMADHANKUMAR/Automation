class Hdfs:
    def make_dir(self, dir_name, util):
        cmd = "sudo -u hdfs hdfs dfs -mkdir "+dir_name
        util.run_call(cmd, shell=True)

    def own_dir(self, dir_name, util):
        cmd = "sudo -u hdfs hdfs dfs -chown -R root:hdfs "+dir_name
        util.run_call(cmd, shell=True)

    def put_file(self, source, target,util) :
        cmd = "hdfs dfs -put "+source+" "+target
        util.run_call(cmd, shell=True)
