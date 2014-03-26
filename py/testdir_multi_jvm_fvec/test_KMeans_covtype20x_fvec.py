import unittest, time, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_hosts, h2o_kmeans
import h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2,java_heap_GB=5)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_KMeans_covtype20x_fvec(self):
        h2o.beta_features = True
        if localhost:
            csvFilenameList = [
                # 68 secs on my laptop?
                ('covtype20x.data', 1200, 'cA'),
                ]
        else:
            # None is okay for hex_key
            csvFilenameList = [
                ('covtype20x.data', 1200,'cA'),
                # ('covtype200x.data', 1000,'cE'),
                ]

        importFolderPath = "standard"
        for csvFilename, timeoutSecs, hex_key in csvFilenameList:
            csvPathname = importFolderPath + "/" + csvFilename
            # creates csvFilename.hex from file in importFolder dir 
            start = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, 
                timeoutSecs=2000, hex_key=hex_key) # noise=('JStack', None)
            print "parse end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o.check_sandbox_for_errors()

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvPathname, \
                "    numRows:", "{:,}".format(inspect['numRows']), \
                "    numCols:", "{:,}".format(inspect['numCols'])

            kwargs = {
                'max_iter': 25,
                'initialization': 'Furthest',
                'k': 2, 
                # reuse the same seed, to get deterministic results (otherwise sometimes fails
                'seed': 265211114317615310,
            }

            start = time.time()
            kmeans = h2o_cmd.runKMeans(parseResult=parseResult, \
                timeoutSecs=timeoutSecs, retryDelaySecs=2, pollTimeoutSecs=60, **kwargs)
            elapsed = time.time() - start
            print "kmeans end on ", csvPathname, 'took', elapsed, 'seconds.', \
                "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            h2o_kmeans.simpleCheckKMeans(self, kmeans, **kwargs)
            (centers, tupleResultList) = h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseResult, 'd', **kwargs)

if __name__ == '__main__':
    h2o.unit_main()
