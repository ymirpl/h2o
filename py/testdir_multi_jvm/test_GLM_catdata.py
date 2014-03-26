import unittest, time, sys, copy
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_hosts, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3,java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_catdata_hosts(self):
        # these are still in /home/kevin/scikit/datasets/logreg
        # FIX! just two for now..
        csvFilenameList = [
            "1_100kx7_logreg.data.gz",
            "2_100kx7_logreg.data.gz"
        ]

        # pop open a browser on the cloud
        ### h2b.browseTheCloud()

        # save the first, for all comparisions, to avoid slow drift with each iteration
        validations1 = {}
        for csvFilename in csvFilenameList:
            csvPathname = csvFilename
            parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put')
            print "\n" + csvPathname

            start = time.time()
            # FIX! why can't I include 0 here? it keeps getting 'unable to solve" if 0 is included
            # 0 by itself is okay?
            kwargs = {'y': 7, 'x': '1,2,3,4,5,6', 'family': "binomial", 'n_folds': 3, 'lambda': 1e-4}
            timeoutSecs = 200
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, 'C7', **kwargs)

            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            ### h2b.browseJsonHistoryAsUrlLastMatch("GLM")

            GLMModel = glm['GLMModel']
            validationsList = glm['GLMModel']['validations']
            print validationsList
            validations = validationsList[0]

            # validations['err']

            if validations1:
                h2o_glm.compareToFirstGlm(self, 'err', validations, validations1)
            else:
                validations1 = copy.deepcopy(validations)

if __name__ == '__main__':
    h2o.unit_main()
