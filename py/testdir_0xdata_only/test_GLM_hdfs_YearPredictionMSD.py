import unittest, time, sys, time, random, copy
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=4,
                # use_hdfs=True, hdfs_version='cdh3', hdfs_name_node='192.168.1.176')
                use_hdfs=True)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_hdfs_YearPredictionMSD(self):
        if localhost:
            csvFilenameList = [
                'YearPredictionMSD.txt',
                'YearPredictionMSD.txt'
                ]
        else:
            csvFilenameList = [
                'YearPredictionMSD.txt',
                'YearPredictionMSD.txt'
                ]

        # a browser window too, just because we can
        ## h2b.browseTheCloud()

        validations1= {}
        coefficients1= {}
        for csvFilename in csvFilenameList:
            csvPathname = "datasets/" + csvFilename
            parseResult = h2i.import_parse(path=csvPathname, schema='hdfs', timeoutSecs=60)
            print csvFilename, 'parse time:', parseResult['response']['time']
            print "Parse result['destination_key']:", parseResult['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])
            print "\n" + csvFilename

            start = time.time()
            # can't pass lamba as kwarg because it's a python reserved word
            # FIX! just look at X=0:1 for speed, for now
            kwargs = {'y': 54, 'n_folds': 2, 'family': "binomial", 'case': 1}
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=500, **kwargs)

            # different when n_foldsidation is used? No trainingErrorDetails?
            h2o.verboseprint("\nglm:", glm)
            ### h2b.browseJsonHistoryAsUrlLastMatch("GLM")

            GLMModel = glm['GLMModel']
            print "GLM time", GLMModel['time']

            coefficients = GLMModel['coefficients']
            validationsList = GLMModel['validations']
            validations = validationsList.pop()
            # validations['err']

            if validations1:
                h2o_glm.compareToFirstGlm(self, 'err', validations, validations1)
            else:
                validations1 = copy.deepcopy(validations)

            if coefficients1:
                h2o_glm.compareToFirstGlm(self, '0', coefficients, coefficients1)
            else:
                coefficients1 = copy.deepcopy(coefficients)

            sys.stdout.write('.')
            sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
