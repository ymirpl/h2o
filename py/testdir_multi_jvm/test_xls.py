import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i

class TestExcel(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3,java_heap_MB=1300)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_iris_xls(self):
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path='xls/iris.xls', schema='put')
        h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=5)

    def test_iris_xlsx(self):
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path='xls/iris.xlsx', schema='put')
        h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=5)

    def test_poker_xls(self):
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path='xls/poker-hand-testing.xls', schema='put')
        h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=10)

    def test_poker_xlsx(self):
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path='xls/poker-hand-testing.xlsx', schema='put',
            timeoutSecs=120, pollTimeoutSecs=60)
        h2o_cmd.runRF(None, parseResult=parseResult, timeoutSecs=120)

if __name__ == '__main__':
    h2o.unit_main()
