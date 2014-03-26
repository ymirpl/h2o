import unittest, time, sys, random, copy
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd
import h2o_hosts, h2o_glm
import h2o_browse as h2b
import h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3,java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_from_import_hosts(self):
        if localhost:
            csvFilenameList = [
                'covtype.data',
                ]
        else:
            csvFilenameList = [
                'covtype200x.data',
                'covtype200x.data',
                'covtype.data',
                'covtype.data',
                'covtype20x.data',
                'covtype20x.data',
                ]

        # a browser window too, just because we can
        ## h2b.browseTheCloud()
        importFolderPath = "standard"
        validations1= {}
        coefficients1= {}
        for csvFilename in csvFilenameList:
            # have to re-import each iteration now, since the source key
            # is removed and if we re-parse it, it's not there
            csvPathname = importFolderPath + "/" + csvFilename
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, timeoutSecs=2000)
            print csvFilename, 'parse time:', parseResult['response']['time']
            print "Parse result['destination_key']:", parseResult['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])
            print "\n" + csvFilename

            start = time.time()
            # can't pass lamba as kwarg because it's a python reserved word
            # FIX! just look at X=0:1 for speed, for now
            kwargs = {'y': 54, 'n_folds': 2, 'family': "binomial", 'case': 1}
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=2000, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

            h2o.verboseprint("\nglm:", glm)

            GLMModel = glm['GLMModel']
            coefficients = GLMModel['coefficients']
            validationsList = GLMModel['validations']
            validations = validationsList.pop()
            # validations['err']

            if validations1:
                h2o_glm.compareToFirstGlm(self, 'err', validations, validations1)
            else:
                validations1 = copy.deepcopy(validations)

            if coefficients1:
                h2o_glm.compareToFirstGlm(self, 'C1', coefficients, coefficients1)
            else:
                coefficients1 = copy.deepcopy(coefficients)

            sys.stdout.write('.')
            sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
