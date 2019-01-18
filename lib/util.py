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
