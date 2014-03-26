
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

def glm_doit(self, csvFilename, bucket, csvPathname, timeoutSecs, pollTimeoutSecs, **kwargs):
    print "\nStarting GLM of", csvFilename
    hex_key = csvFilename + ".hex"
    parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hex_key,
        timeoutSecs=60, pollTimeoutSecs=pollTimeoutSecs)

    start = time.time()
    glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
    print "GLM in",  (time.time() - start), "secs (python)"
    h2o_glm.simpleCheckGLM(self, glm, "C8", **kwargs)

    # compare this glm to the first one. since the files are replications, the results
    # should be similar?
    GLMModel = glm['GLMModel']
    validationsList = glm['GLMModel']['validations']
    validations = validationsList[0]
    modelKey = GLMModel['model_key']
    return modelKey, validations, parseResult

def glm_score(self, csvFilename, bucket, csvPathname, modelKey, thresholds="0.5",
    timeoutSecs=30, pollTimeoutSecs=30):
    print "\nStarting GLM score of", csvFilename
    hex_key = csvFilename + ".hex"
    parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hex_key, 
        timeoutSecs=timeoutSecs, pollTimeoutSecs=pollTimeoutSecs)
    y = "10"
    x = ""
    kwargs = {'x': x, 'y':  y, 'case': -1, 'thresholds': 0.5}

    start = time.time()
    glmScore = h2o_cmd.runGLMScore(key=hex_key, model_key=modelKey, thresholds="0.5",
        timeoutSecs=timeoutSecs)
    print "GLMScore in",  (time.time() - start), "secs (python)"
    h2o.verboseprint(h2o.dump_json(glmScore))
    ### h2o_glm.simpleCheckGLM(self, glm, 7, **kwargs)

    # compare this glm to the first one. since the files are replications, 
    # the results
    # should be similar?
    # UPDATE: format for returning results is slightly different than normal GLM
    validation = glmScore['validation']
    if self.validations1:
        h2o_glm.compareToFirstGlm(self, 'err', validation, self.validations1)
    else:
        self.validations1 = copy.deepcopy(validation)



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

    def test_GLM_score_same(self):
        # gunzip it and cat it to create 2x and 4x replications in SYNDATASETS_DIR
        bucket = 'home-0xdiag-datasets'
        csvFilename = "1mx10_hastie_10_2.data.gz"
        csvPathname = 'standard' + '/' + csvFilename

        y = "10"
        x = ""
        kwargs = {'x': x, 'y':  y, 'case': -1, 'thresholds': 0.5}
        (modelKey, validations1, parseResult) = glm_doit(self, csvFilename, bucket, csvPathname, 
            timeoutSecs=60, pollTimeoutSecs=60, **kwargs)

        print "Use", modelKey, "model on 2x and 4x replications and compare results to 1x"

        filename1x = "hastie_1x.data"
        pathname1x = SYNDATASETS_DIR + '/' + filename1x

        fullPathname = h2i.find_folder_and_filename(bucket, csvPathname, returnFullPath=True)
        h2o_util.file_gunzip(fullPathname, pathname1x)

        filename2x = "hastie_2x.data"
        pathname2x = SYNDATASETS_DIR + '/' + filename2x
        bucket = None
        h2o_util.file_cat(pathname1x,pathname1x,pathname2x)
        glm_score(self,filename2x, bucket, pathname2x, modelKey, thresholds="0.5",
            timeoutSecs=60, pollTimeoutSecs=60)

        filename4x = "hastie_4x.data"
        pathname4x = SYNDATASETS_DIR + '/' + filename4x
        h2o_util.file_cat(pathname2x, pathname2x, pathname4x)
        
        print "Iterating 3 times on this last one"
        for i in range(3):
            print "\nTrial #", i, "of", filename4x
            glm_score(self,filename4x, bucket, pathname4x, modelKey, thresholds="0.5",
                timeoutSecs=60, pollTimeoutSecs=60)

if __name__ == '__main__':
    h2o.unit_main()
