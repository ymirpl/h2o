import unittest, sys, time
sys.path.extend(['.','..','py'])
import h2o_cmd, h2o, h2o_hosts, h2o_browse as h2b, h2o_import as h2i

# Uses your username specific json: pytest_config-<username>.json
# copy pytest_config-simple.json and modify to your needs.
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_311M_rows_hosts(self):
        # since we'll be waiting, pop a browser
        # h2b.browseTheCloud()
        importFolderPath = 'standard'
        csvFilename = 'new-poker-hand.full.311M.txt.gz'
        csvPathname = importFolderPath + "/" + csvFilename

        for trials in range(2):
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local', 
                timeoutSecs=500)
            print csvFilename, 'parse time:', parseResult['response']['time']
            print "Parse result['destination_key']:", parseResult['destination_key']
            inspect = h2o_cmd.runInspect(None,parseResult['destination_key'])

            print "\n" + csvFilename
            start = time.time()
            RFview = h2o_cmd.runRF(parseResult=parseResult, trees=5, depth=5,
                timeoutSecs=600, retryDelaySecs=10.0)
            print "RF end on ", csvFilename, 'took', time.time() - start, 'seconds'

if __name__ == '__main__':
    h2o.unit_main()


