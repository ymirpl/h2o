
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
import unittest, time, sys, copy
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_util, h2o_hosts, h2o_import as h2i

def glm_doit(self, csvFilename, bucket, csvPathname, timeoutSecs=30):
    print "\nStarting GLM of", csvFilename
    parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, hex_key=csvFilename + ".hex", schema='put', timeoutSecs=30)
    y = "10"
    x = ""
    # Took n_folds out, because GLM doesn't include n_folds time and it's slow
    # wanted to compare GLM time to my measured time
    # hastie has two values 1,-1. need to specify case
    kwargs = {'x': x, 'y':  y, 'case': -1, 'thresholds': 0.5}

    start = time.time()
    glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
    print "GLM in",  (time.time() - start), "secs (python)"
    h2o_glm.simpleCheckGLM(self, glm, "C8", **kwargs)

    # compare this glm to the first one. since the files are replications, the results
    # should be similar?
    GLMModel = glm['GLMModel']
    validationsList = glm['GLMModel']['validations']
    validations = validationsList[0]
    # validations['err']

    if self.validations1:
        h2o_glm.compareToFirstGlm(self, 'err', validations, self.validations1)
    else:
        self.validations1 = copy.deepcopy(validations)


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts(1)
        global SYNDATASETS_DIR
        SYNDATASETS_DIR = h2o.make_syn_dir()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    validations1 = {}

    def test_GLM_hastie(self):
        # gunzip it and cat it to create 2x and 4x replications in SYNDATASETS_DIR
        # FIX! eventually we'll compare the 1x, 2x and 4x results like we do
        # in other tests. (catdata?)
        bucket = 'home-0xdiag-datasets'
        csvFilename = "1mx10_hastie_10_2.data.gz"
        csvPathname = 'standard' + '/' + csvFilename
        glm_doit(self, csvFilename, bucket, csvPathname, timeoutSecs=75)
        fullPathname = h2i.find_folder_and_filename(bucket, csvPathname, returnFullPath=True)

        filename1x = "hastie_1x.data"
        pathname1x = SYNDATASETS_DIR + '/' + filename1x
        h2o_util.file_gunzip(fullPathname, pathname1x)

        filename2x = "hastie_2x.data"
        pathname2x = SYNDATASETS_DIR + '/' + filename2x
        h2o_util.file_cat(pathname1x,pathname1x,pathname2x)
        glm_doit(self,filename2x, None, pathname2x, timeoutSecs=75)

        filename4x = "hastie_4x.data"
        pathname4x = SYNDATASETS_DIR + '/' + filename4x
        h2o_util.file_cat(pathname2x,pathname2x,pathname4x)
        
        print "Iterating 3 times on this last one for perf compare"
        for i in range(3):
            print "\nTrial #", i, "of", filename4x
            glm_doit(self, filename4x, None, pathname4x, timeoutSecs=150)

if __name__ == '__main__':
    h2o.unit_main()
