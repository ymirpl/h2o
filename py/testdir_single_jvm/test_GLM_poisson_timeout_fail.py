import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_hosts, h2o_import as h2i

print "Repeated test of case that timeouts in EC2"

def define_params():
    paramDict = {
        'y': 54,
        'weight': None, 
        'family': 'poisson', 
        'beta_epsilon': 0.001, 
        'thresholds': 0.1, 
        'max_iter': 15, 
        'link': 'familyDefault', 
        'alpha': 0.5, 
        'n_folds': 9, 
        'lambda': 1e-4
    }
    return paramDict

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_poisson_timeout_fail(self):
        start = time.time()
        csvPathname = 'standard/covtype.data'
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put')
        print "upload/parse end on ", csvPathname, 'took', time.time() - start, 'seconds'

        kwargs = define_params()
        for trial in range(3):
            # make timeout bigger with xvals
            timeoutSecs = 60 + (kwargs['n_folds']*20)
            # or double the 4 seconds per iteration (max_iter+1 worst case?)
            timeoutSecs = max(timeoutSecs, (8 * (kwargs['max_iter']+1)))
            
            start = time.time()
            glm = h2o_cmd.runGLM(timeoutSecs=timeoutSecs, parseResult=parseResult, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'

            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            print "Trial #", trial, "completed\n"

if __name__ == '__main__':
    h2o.unit_main()
