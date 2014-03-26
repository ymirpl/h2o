import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm

def define_params():
    paramDict = {
        'family': ['binomial'],
        'lambda': [1.0E-4],
        'alpha': [0.5],
        'max_iter': [2000],
        'weight': [1.0],
        'thresholds': [0.5],
        'n_folds': [1],
        'beta_epsilon': [1.0E-4],
        }
    return paramDict

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=10)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

    def test_GLM_trisum(self):
        ### h2b.browseTheCloud()

        csvFilename = "logreg_trisum_int_cat_10000x10.csv"
        csvPathname = "logreg/" + csvFilename
        hex_key = csvFilename + ".hex"

        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=hex_key, timeoutSecs=10, schema='put')
        print csvFilename, 'parse time:', parseResult['response']['time']
        print "Parse result['destination_key']:", parseResult['destination_key']

        # We should be able to see the parse result?
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        print "\n" + csvFilename

        paramDict = define_params()
        paramDict2 = {}
        for k in paramDict:
            # sometimes we have a list to pick from in the value. now it's just list of 1.
            paramDict2[k] = paramDict[k][0]

        y = 10
        # FIX! what should we have for case? 1 should be okay because we have 1's in output col
        kwargs = {'y': y, 'max_iter': 50}
        kwargs.update(paramDict2)

        start = time.time()
        glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=20, **kwargs)
        print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
        h2o_glm.simpleCheckGLM(self, glm, "C9", **kwargs)

        if not h2o.browse_disable:
            h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
            time.sleep(5)

if __name__ == '__main__':
    h2o.unit_main()
