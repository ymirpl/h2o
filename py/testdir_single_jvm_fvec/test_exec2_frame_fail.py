import unittest, sys, random, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_exec as h2e, h2o_hosts, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2, java_heap_GB=5)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_exec2_frame_fail(self):
        h2o.beta_features = True
        csvPathname = 'standard/covtype.data'
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put', hex_key='c.hex', timeoutSecs=15)
        print "\nParse key is:", parseResult['destination_key']

        start = time.time()

        execExpr = 'Result2=c.hex[,9]'
        resultExec = h2o_cmd.runExec(str=execExpr, timeoutSecs=60)
        h2o.check_sandbox_for_errors()

        execExpr = 'Result2[,48]=(c.hex[,2]==0) ? 54321 : 54321'
        resultExec = h2o_cmd.runExec(str=execExpr, timeoutSecs=60)
        h2o.check_sandbox_for_errors()
        print "exec end on ", "covtype.data" , 'took', time.time() - start, 'seconds'


if __name__ == '__main__':
    h2o.unit_main()
