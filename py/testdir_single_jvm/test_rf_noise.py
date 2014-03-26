import unittest, time, sys
# not needed, but in case you move it down to subdir
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i
import h2o_browse as h2b

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=3)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_A_Basic(self):
        h2o.verify_cloud_size()

    def test_B_RF_iris2(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='iris/iris2.csv', schema='put', noise=('StoreView',None))
        h2o_cmd.runRF(parseResult=parseResult, trees=6, timeoutSecs=10)

    def test_C_RF_poker100(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='poker/poker100', schema='put', noise=('StoreView',None))
        h2o_cmd.runRF(parseResult=parseResult, trees=6, timeoutSecs=10)

    def test_D_GenParity1(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='parity_128_4_100_quad.data', schema='put', noise=('StoreView',None))
        h2o_cmd.runRF(parseResult=parseResult, trees=50, timeoutSecs=10)

    def test_E_ParseManyCols(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='fail1_100x11000.csv.gz', schema='put', noise=('StoreView',None))
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])

    def test_F_StoreView(self):
        storeView = h2o.nodes[0].store_view()

if __name__ == '__main__':
    h2o.unit_main()
