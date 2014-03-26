import unittest, random, sys, time, os
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_glm, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

def write_syn_dataset(csvPathname, rowCount, colCount, SEED, translateList):
    # do we need more than one random generator?
    r1 = random.Random(SEED)
    r2 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            ri1 = int(r1.triangular(0,4,2.5))
            rowData.append(ri1)

        rowTotal = sum(rowData)
        if translateList is not None:
            for i, iNum in enumerate(rowData):
                rowData[i] = translateList[iNum]

        result = r2.randint(0,1)
        ### print colCount, rowTotal, result
        rowDataStr = map(str,rowData)
        rowDataStr.append(str(result))
        rowDataCsv = ",".join(rowDataStr)
        dsf.write(rowDataCsv + "\n")

    dsf.close()


paramDict = {
    'family': ['binomial'],
    'lambda': [1.0E-5],
    'alpha': [1.0],
    'max_iter': [50],
    'weight': [1.0],
    'thresholds': [0.5],
    'n_folds': [2],
    'beta_epsilon': [1.0E-4],
    }

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2,java_heap_GB=5,use_flatfile=True)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_many_cols_4(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        translateList = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u']
        tryList = [
            (100000,  100, 'cA', 600),
            ]

        ### h2b.browseTheCloud()

        for (rowCount, colCount, hex_key, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "\nCreating random", csvPathname
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE, translateList)

            print "\nUpload and parse", csvPathname
            parseResult = h2i.import_parse(path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=240, retryDelaySecs=0.5)
            print csvFilename, 'parse time:', parseResult['response']['time']
            print "Parse result['destination_key']:", parseResult['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvFilename

            paramDict2 = {}
            for k in paramDict:
                paramDict2[k] = paramDict[k][0]

            # since we add the output twice, it's no longer colCount-1
            y = colCount
            kwargs = {'y': y, 'max_iter': 50}
            kwargs.update(paramDict2)

            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            # only col Y-1 (next to last)doesn't get renamed in coefficients due to enum/categorical expansion
            print "y:", y 
            # FIX! bug was dropped coefficients if constant column is dropped
            ### h2o_glm.simpleCheckGLM(self, glm, Y-2, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

if __name__ == '__main__':
    h2o.unit_main()
