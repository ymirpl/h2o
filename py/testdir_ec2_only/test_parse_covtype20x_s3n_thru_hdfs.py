import unittest, time, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            # all hdfs info is done thru the hdfs_config michal's ec2 config sets up?
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_covtype20x_s3n_thru_hdfs(self):
        bucket = 'home-0xdiag-datasets'
        csvFilename = "covtype20x.data"
        importFolderPath = 'standard'
        csvPathname = importFolderPath + "/" + csvFilename
        trialMax = 3
        timeoutSecs = 500
        for trial in range(trialMax):
            hex_key = csvFilename + "_" + str(trial) + ".hex"
            start = time.time()
            parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='s3n', hex_key=hex_key,
                timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=60)
            elapsed = time.time() - start

            print 'h2o reported parse time:', parseResult['response']['time']
            print "parse result:", parseResult['destination_key']
            print "Trial #", trial, "completed in", elapsed, "seconds.", \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

if __name__ == '__main__':
    h2o.unit_main()
