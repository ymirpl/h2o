import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i

class glm_same_parse(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()
    
    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud(h2o.nodes)
    
    def test_prostate_then_prostate_long_parse(self):
        print "\nput and parse of same file, but both src_key and hex_key are the h2o defaults..always different"
        for trial in range (10):
            start = time.time()
            parseResult = h2i.import_parse(bucket='smalldata', path='logreg/prostate_long.csv.gz', schema='put')
            print "trial #", trial, "parse end on ", "prostate_long.csv.gz" , 'took', time.time() - start, 'seconds'
            h2o.check_sandbox_for_errors()

if __name__ == '__main__':
    h2o.unit_main()
