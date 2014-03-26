import unittest, time, sys, copy
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_util, h2o_hosts, h2o_import as h2i
## Dataset created from this:
#
# from sklearn.datasets import make_hastie_10_2
# import numpy as np
# i = 1000000
# f = 10
# (X,y) = make_hastie_10_2(n_samples=i,random_state=None)
# y.shape = (i,1)
# Y = np.hstack((X,y))
# np.savetxt('./1mx' + str(f) + '_hastie_10_2.data', Y, delimiter=',', fmt='%.2f');

def glm_doit(self, csvFilename, bucket, csvPathname, timeoutSecs=30):
    print "\nStarting parse of", csvFilename
    parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=csvFilename + ".hex", timeoutSecs=10)
    y = "10"
    x = ""
    # NOTE: hastie has two values, -1 and 1. To make H2O work if two valued and not 0,1 have
    kwargs = {
        'x': x, 'y':  y, 'case': '1',
        # better classifier it flipped? (better AUC?)
        'max_iter': 10,
        'case': -1, 'case_mode': '=',
        'n_folds': 2,
        'lambda': '1e-8,1e-4,1e-3',
        'alpha': '0,0.25,0.8',
        # hardwire threshold to 0.5 because the dataset is so senstive right around threshold
        # otherwise, GLMGrid will pick a model with zero coefficients, if it has the best AUC
        # to avoid my checker complaining about all zero coefficients, force the threshold to 0.5
        'thresholds': '0.5',
        # 'thresholds': '0.2:0.8:0.1'
        }

    start = time.time() 
    print "\nStarting GLMGrid of", csvFilename
    glmGridResult = h2o_cmd.runGLMGrid(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
    print "GLMGrid in",  (time.time() - start), "secs (python)"

    # still get zero coeffs..best model is AUC = 0.5 with intercept only.
    h2o_glm.simpleCheckGLMGrid(self,glmGridResult, allowZeroCoeff=True,**kwargs)

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2,java_heap_GB=5)
        else:
            h2o_hosts.build_cloud_with_hosts()
        global SYNDATASETS_DIR
        SYNDATASETS_DIR = h2o.make_syn_dir()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_A_1mx10_hastie_10_2(self):
        # gunzip it and cat it to create 2x and 4x replications in SYNDATASETS_DIR
        # FIX! eventually we'll compare the 1x, 2x and 4x results like we do
        # in other tests. (catdata?)
        bucket = 'home-0xdiag-datasets'
        csvFilename = "1mx10_hastie_10_2.data.gz"
        csvPathname = 'standard' + '/' + csvFilename
        glm_doit(self, csvFilename, bucket, csvPathname, timeoutSecs=300)

        fullPathname = h2i.find_folder_and_filename('home-0xdiag-datasets', csvPathname, returnFullPath=True)
        filename1x = "hastie_1x.data"
        pathname1x = SYNDATASETS_DIR + '/' + filename1x
        h2o_util.file_gunzip(fullPathname, pathname1x)

        filename2x = "hastie_2x.data"
        pathname2x = SYNDATASETS_DIR + '/' + filename2x
        h2o_util.file_cat(pathname1x,pathname1x,pathname2x)
        glm_doit(self, filename2x, None, pathname2x, timeoutSecs=300)

if __name__ == '__main__':
    h2o.unit_main()
