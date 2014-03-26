import unittest
import random, sys, time, re
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1, java_heap_GB=14)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_mnist_A_training(self):
        importFolderPath = "mnist"
        csvFilelist = [
            ("mnist_training.csv.gz", 600),
            ("mnist_training.csv.gz", 600),
        ]

        trial = 0
        allDelta = []
        for (csvFilename,  timeoutSecs) in csvFilelist:
            testKey2 = csvFilename + "_" + str(trial) + ".hex"
            start = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=importFolderPath+"/"+csvFilename,
                hex_key=testKey2, timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

    def test_parse_mnist_B_testing(self):
        importFolderPath = "mnist"
        csvFilelist = [
            ("mnist_testing.csv.gz", 600),
            ("mnist_testing.csv.gz", 600),
        ]

        trial = 0
        allDelta = []
        for (csvFilename, timeoutSecs) in csvFilelist:
            testKey2 = csvFilename + "_" + str(trial) + ".hex"
            start = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=importFolderPath+"/"+csvFilename,
                hex_key=testKey2, timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

if __name__ == '__main__':
    h2o.unit_main()
