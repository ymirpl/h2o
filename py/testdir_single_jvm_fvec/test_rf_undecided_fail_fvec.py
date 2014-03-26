import unittest, random, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_rf, h2o_hosts, h2o_import as h2i

params = {
    'nbins': 1000, 
    'ntrees': 10, 
    # apparently fails with undecided node assertion if all inputs the same
    'cols': '0,1,2,3,4,', 
    'seed': '19823134', 
}

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_params_rand1_fvec(self):
        h2o.beta_features = True
        csvPathname = 'poker/poker1000'
        for trial in range(10):
            # params is mutable. This is default.
            kwargs = params.copy()
            timeoutSecs = 180
            parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put', timeoutSecs=timeoutSecs)
            h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
